# pylint: disable=C0103,C0111,C0302,W0212
import math

import pytest
import numpy as np
import numpy.ma as ma
import pandas as pd
import quasardb
import quasardb.numpy as qdbnp
import quasardb.pandas as qdbpd

import test_query as qlib
import test_table as tslib
from utils import assert_ma_equal

all_value_types = ["double", "int64", "blob", "string", "timestamp", "symbol"]


def _select_column_query(table, column_name):
    return 'SELECT "{}" FROM "{}"'.format(column_name, table.get_name())


def _stream_batches(conn, query, batch_size=0):
    # Materialize inside the with block: conversion happens during iteration,
    # while the C result is alive. Yielded dicts stay valid after close.
    with conn.stream_query(query, batch_size=batch_size) as stream:
        return list(stream)


def _concat_column(batches, column_name):
    return ma.concatenate([batch[column_name] for batch in batches])


##
# Equivalence vs query_numpy


@pytest.mark.parametrize("value_type", all_value_types)
def test_stream_query_equals_query_numpy(
    cluster, value_type, native_cluster, table, intervals
):
    qlib._insert_points(
        value_type, native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, value_type)
    query = _select_column_query(table, column_name)

    expected = dict(native_cluster.query_numpy(query))

    batches = _stream_batches(cluster, query, batch_size=32)
    streamed = _concat_column(batches, column_name)

    assert streamed.dtype.kind == expected[column_name].dtype.kind
    assert_ma_equal(streamed, expected[column_name])


##
# Batching contract


def test_stream_query_batching_contract(
    cluster,
    native_cluster,
    table,
    intervals,
    row_count,
    reader_batch_size,
):
    qlib._insert_points(
        "double",
        native_cluster,
        table,
        intervals=intervals,
        points=row_count,
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    with cluster.stream_query(query, batch_size=reader_batch_size) as stream:
        if isinstance(cluster, quasardb.NativeCluster):
            assert stream.get_batch_size() == reader_batch_size
        else:
            # REST keeps the server's Arrow batch boundaries. Zero reports that
            # the client cannot set a batch size, even when one was requested.
            assert stream.get_batch_size() == 0
        batches = list(stream)

    assert all(len(batch[column_name]) > 0 for batch in batches)
    assert sum(len(batch[column_name]) for batch in batches) == row_count
    assert_ma_equal(
        _concat_column(batches, column_name),
        dict(native_cluster.query_numpy(query))[column_name],
    )
    if isinstance(cluster, quasardb.NativeCluster):
        expected_batch_count = math.ceil(row_count / reader_batch_size)
        assert len(batches) == expected_batch_count
        for batch in batches[:-1]:
            assert len(batch[column_name]) == reader_batch_size
        remainder = row_count - (expected_batch_count - 1) * reader_batch_size
        assert len(batches[-1][column_name]) == remainder


def test_stream_query_zero_batch_size_yields_single_batch(
    cluster, native_cluster, table, intervals
):
    row_count = 100
    qlib._insert_points(
        "double",
        native_cluster,
        table,
        intervals=intervals,
        points=row_count,
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    with cluster.stream_query(query) as stream:
        assert stream.get_batch_size() == 0
        batches = list(stream)

    assert sum(len(batch[column_name]) for batch in batches) == row_count
    # Native zero requests the complete result in one batch. The REST
    # server may split these rows across any number of Arrow batches.
    if isinstance(cluster, quasardb.NativeCluster):
        assert len(batches) == 1
        assert len(batches[0][column_name]) == row_count


##
# Argument handling


def test_stream_query_batch_size_is_keyword_only(cluster, native_cluster, table):
    query = 'SELECT * FROM "{}"'.format(table.get_name())

    with pytest.raises(TypeError):
        cluster.stream_query(query, 100)


##
# Lifecycle errors


def test_stream_query_cannot_iterate_without_enter(cluster, native_cluster, table):
    query = 'SELECT * FROM "{}"'.format(table.get_name())
    stream = cluster.stream_query(query)

    with pytest.raises(quasardb.UninitializedError):
        _ = list(stream)


def test_query_reader_cannot_be_instantiated_directly():
    with pytest.raises(quasardb.DirectInstantiationError):
        quasardb.QueryReader()


##
# Empty result


def test_stream_query_empty_result_yields_zero_batches(cluster, native_cluster, table):
    query = 'SELECT * FROM "{}" IN RANGE(2016-01-01, 2016-12-12)'.format(
        table.get_name()
    )

    with cluster.stream_query(query) as stream:
        assert list(stream) == []


def test_stream_query_dml_yields_zero_batches(cluster, native_cluster, table):
    # DML statements return no result set: the C result stays NULL.
    query = 'INSERT INTO "{}" ($timestamp, "{}") VALUES (NOW(), 1.0)'.format(
        table.get_name(), tslib._double_col_name(table)
    )

    with cluster.stream_query(query) as stream:
        assert list(stream) == []


##
# Schema-kind stability across batches


def test_stream_query_schema_stable_when_first_batch_all_null(
    cluster, native_cluster, table, intervals
):
    start_time = tslib._start_time(intervals)
    row_count = 100
    batch_size = 50

    (timestamps, values) = tslib._generate_double_ts(start_time, row_count)

    # Native batching puts all nulls in the first batch and all values in the
    # second. Its global schema probe must still type the first batch as float64.
    mask = np.zeros(row_count, dtype=bool)
    mask[:batch_size] = True
    xs = ma.array(data=values, mask=mask)

    column_name = tslib._double_col_name(table)
    qlib._write_points(native_cluster, table, column_name, (timestamps, xs))

    query = _select_column_query(table, column_name)
    batches = _stream_batches(cluster, query, batch_size=batch_size)

    assert sum(len(batch[column_name]) for batch in batches) == row_count
    offset = 0
    for batch in batches:
        array = batch[column_name]
        assert len(array) > 0
        assert array.dtype == np.dtype("float64")
        np.testing.assert_array_equal(array.mask, mask[offset : offset + len(array)])
        offset += len(array)
    if isinstance(cluster, quasardb.NativeCluster):
        assert len(batches) == 2
        assert ma.count_masked(batches[0][column_name]) == batch_size
        assert ma.count_masked(batches[1][column_name]) == 0


def test_stream_query_entirely_null_column_preserves_mask(
    cluster, native_cluster, table, intervals
):
    # Writing doubles creates rows; selecting a column that was never written
    # yields all-null cells for those rows.
    qlib._insert_points(
        "double", native_cluster, table, intervals=intervals, points=100
    )
    column_name = tslib._int64_col_name(table)
    query = _select_column_query(table, column_name)

    batches = _stream_batches(cluster, query, batch_size=50)

    assert sum(len(batch[column_name]) for batch in batches) == 100
    # The native schema probe falls back to float64 when it sees only nulls.
    # The REST Arrow schema carries the column's declared int64 type.
    if isinstance(cluster, quasardb.NativeCluster):
        assert len(batches) == 2
        expected_dtype = np.dtype("float64")
    else:
        expected_dtype = np.dtype("int64")

    for batch in batches:
        xs = batch[column_name]
        assert len(xs) > 0
        assert xs.dtype == expected_dtype
        assert ma.count_masked(xs) == len(xs)


##
# Mixed-type columns


def test_stream_query_mixed_type_column_raises(
    cluster, native_cluster, entry_name, intervals
):
    start_time = tslib._start_time(intervals)

    t1_name = entry_name + "_double"
    t2_name = entry_name + "_blob"

    t1 = native_cluster.table(t1_name)
    t1.create([quasardb.ColumnInfo(quasardb.ColumnType.Double, "the_col")])
    t2 = native_cluster.table(t2_name)
    t2.create([quasardb.ColumnInfo(quasardb.ColumnType.Blob, "the_col")])

    qlib._write_points(
        native_cluster, t1, "the_col", tslib._generate_double_ts(start_time, 1)
    )
    qlib._write_points(
        native_cluster, t2, "the_col", tslib._generate_blob_ts(start_time, 1)
    )

    query = 'SELECT the_col FROM "{}", "{}"'.format(t1_name, t2_name)

    if isinstance(cluster, quasardb.NativeCluster):
        # Native results carry a tag per cell. The mismatch surfaces during iteration.
        with cluster.stream_query(query) as stream:
            with pytest.raises(quasardb.IncompatibleTypeError):
                list(stream)
    else:
        # qdb-rest rejects the mixed column with HTTP 400 before sending an Arrow schema.
        with pytest.raises(quasardb.InvalidQueryError):
            with cluster.stream_query(query):
                pass


##
# numpy layer


@pytest.mark.parametrize("value_type", all_value_types)
def test_qdbnp_stream_query_equals_query(
    cluster, value_type, native_cluster, table, intervals
):
    qlib._insert_points(
        value_type, native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, value_type)
    query = _select_column_query(table, column_name)

    (expected_idx, expected_cols) = qdbnp.query(cluster, query, dict=True)

    batches = list(qdbnp.stream_query(cluster, query, dict=True, batch_size=32))

    streamed_idx = np.concatenate([idx for (idx, _) in batches])
    streamed_col = ma.concatenate([cols[column_name] for (_, cols) in batches])

    np.testing.assert_array_equal(streamed_idx, expected_idx)
    assert_ma_equal(streamed_col, expected_cols[column_name])


def test_qdbnp_stream_query_default_index_is_offset_aware(
    cluster, native_cluster, table, intervals
):
    row_count = 100
    batch_size = 32

    qlib._insert_points(
        "double",
        native_cluster,
        table,
        intervals=intervals,
        points=row_count,
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    idx_batches = []
    offset = 0
    for idx, _ in qdbnp.stream_query(cluster, query, batch_size=batch_size):
        np.testing.assert_array_equal(idx, np.arange(offset, offset + len(idx)))
        idx_batches.append(idx)
        offset += len(idx)

    np.testing.assert_array_equal(np.concatenate(idx_batches), np.arange(row_count))


def test_qdbnp_stream_query_named_index(cluster, native_cluster, table, intervals):
    qlib._insert_points(
        "double", native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, "double")
    query = 'SELECT $timestamp, "{}" FROM "{}"'.format(column_name, table.get_name())

    (expected_idx, _) = qdbnp.query(cluster, query, index="$timestamp")

    idx_batches = []
    for idx, cols in qdbnp.stream_query(
        cluster, query, index="$timestamp", dict=True, batch_size=32
    ):
        assert idx.dtype.kind == "M"
        assert "$timestamp" not in cols
        idx_batches.append(idx)

    np.testing.assert_array_equal(np.concatenate(idx_batches), expected_idx)


def test_qdbnp_stream_query_dict_and_list_shapes(
    cluster, native_cluster, table, intervals
):
    qlib._insert_points(
        "double", native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, "double")
    query = 'SELECT $timestamp, "{}" FROM "{}"'.format(column_name, table.get_name())

    for _, cols in qdbnp.stream_query(
        cluster, query, index="$timestamp", dict=True, batch_size=32
    ):
        assert isinstance(cols, dict)
        assert list(cols.keys()) == [column_name]

    for _, cols in qdbnp.stream_query(
        cluster, query, index="$timestamp", dict=False, batch_size=32
    ):
        assert isinstance(cols, list)
        assert len(cols) == 1


def test_qdbnp_stream_query_empty_result_yields_zero_batches(
    cluster, native_cluster, table
):
    query = 'SELECT * FROM "{}" IN RANGE(2016-01-01, 2016-12-12)'.format(
        table.get_name()
    )

    assert list(qdbnp.stream_query(cluster, query)) == []


def test_qdbnp_stream_query_dml_yields_zero_batches(cluster, native_cluster, table):
    query = 'INSERT INTO "{}" ($timestamp, "{}") VALUES (NOW(), 1.0)'.format(
        table.get_name(), tslib._double_col_name(table)
    )

    assert list(qdbnp.stream_query(cluster, query)) == []


def test_qdbnp_stream_query_masked_named_index_raises(
    cluster, native_cluster, table, intervals
):
    # Writing doubles creates rows; the int64 column was never written, so
    # selecting it yields an all-null (fully masked) column, which is invalid
    # as an index.
    qlib._insert_points(
        "double", native_cluster, table, intervals=intervals, points=100
    )
    column_name = tslib._int64_col_name(table)
    query = _select_column_query(table, column_name)

    with pytest.raises(ValueError):
        list(qdbnp.stream_query(cluster, query, index=column_name))


def test_qdbnp_query_equals_stream_query_concat(
    cluster,
    native_cluster,
    table,
    intervals,
    row_count,
    reader_batch_size,
):
    qlib._insert_points(
        "double",
        native_cluster,
        table,
        intervals=intervals,
        points=row_count,
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    (expected_idx, expected_cols) = qdbnp.query(cluster, query, dict=True)

    batches = list(
        qdbnp.stream_query(cluster, query, dict=True, batch_size=reader_batch_size)
    )
    assert all(len(idx) > 0 for idx, _ in batches)
    if isinstance(cluster, quasardb.NativeCluster):
        assert all(len(idx) <= reader_batch_size for idx, _ in batches)
        assert len(batches) == math.ceil(row_count / reader_batch_size)

    streamed_idx = np.concatenate([idx for (idx, _) in batches])
    streamed_col = ma.concatenate([cols[column_name] for (_, cols) in batches])

    np.testing.assert_array_equal(streamed_idx, expected_idx)
    assert_ma_equal(streamed_col, expected_cols[column_name])


##
# pandas layer


# Contract: qdbpd.query() is exactly the concatenation of qdbpd.stream_query()
# batches. Native batches respect the requested size.
def test_qdbpd_query_equals_stream_query_concat(
    cluster,
    native_cluster,
    table,
    intervals,
    row_count,
    reader_batch_size,
):
    qlib._insert_points(
        "double",
        native_cluster,
        table,
        intervals=intervals,
        points=row_count,
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    batches = list(qdbpd.stream_query(cluster, query, batch_size=reader_batch_size))
    if isinstance(cluster, quasardb.NativeCluster):
        assert len(batches) == math.ceil(row_count / reader_batch_size)
        assert all(len(df.index) <= reader_batch_size for df in batches)
    assert all(len(df.index) > 0 for df in batches)

    expected = qdbpd.query(cluster, query)
    pd.testing.assert_frame_equal(pd.concat(batches), expected)


# Contract: stream/concat equivalence holds for every column value type.
@pytest.mark.parametrize("value_type", all_value_types)
def test_qdbpd_query_all_value_types(
    cluster, value_type, native_cluster, table, intervals
):
    qlib._insert_points(
        value_type, native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, value_type)
    query = _select_column_query(table, column_name)

    expected = qdbpd.query(cluster, query)
    streamed = pd.concat(qdbpd.stream_query(cluster, query, batch_size=32))

    # String-like columns may differ in exact `U<n>` width across code paths;
    # only the dtype kind is contractual.
    assert streamed[column_name].dtype.kind == expected[column_name].dtype.kind
    np.testing.assert_array_equal(
        streamed[column_name].to_numpy(), expected[column_name].to_numpy()
    )


# Contract: the default index is a running 0..n-1 named "$index" that
# continues across batches.
def test_qdbpd_stream_query_default_index(cluster, native_cluster, table, intervals):
    row_count = 100
    batch_size = 32

    qlib._insert_points(
        "double",
        native_cluster,
        table,
        intervals=intervals,
        points=row_count,
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    batches = []
    offset = 0
    for df in qdbpd.stream_query(cluster, query, batch_size=batch_size):
        assert df.index.name == "$index"
        np.testing.assert_array_equal(
            df.index.to_numpy(), np.arange(offset, offset + len(df))
        )
        batches.append(df)
        offset += len(df)

    np.testing.assert_array_equal(
        pd.concat(batches).index.to_numpy(), np.arange(row_count)
    )


# Contract: a named index column becomes the DataFrame index and is removed
# from the columns.
def test_qdbpd_stream_query_named_index(cluster, native_cluster, table, intervals):
    qlib._insert_points(
        "double", native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, "double")
    query = 'SELECT $timestamp, "{}" FROM "{}"'.format(column_name, table.get_name())

    for df in qdbpd.stream_query(cluster, query, index="$timestamp", batch_size=32):
        assert df.index.name == "$timestamp"
        assert df.index.dtype.kind == "M"
        assert "$timestamp" not in df.columns


# Contract: an empty result streams zero DataFrames, while query() preserves
# the legacy empty shape (empty frame with named index).
def test_qdbpd_query_empty_result(cluster, native_cluster, table):
    query = 'SELECT * FROM "{}" IN RANGE(2016-01-01, 2016-12-12)'.format(
        table.get_name()
    )

    assert list(qdbpd.stream_query(cluster, query)) == []

    df = qdbpd.query(cluster, query)
    assert len(df) == 0
    assert df.index.name == "$index"


# Contract: DML statements yield zero DataFrames.
def test_qdbpd_stream_query_dml_yields_zero_batches(cluster, native_cluster, table):
    query = 'INSERT INTO "{}" ($timestamp, "{}") VALUES (NOW(), 1.0)'.format(
        table.get_name(), tslib._double_col_name(table)
    )

    assert list(qdbpd.stream_query(cluster, query)) == []


##
# Early abandonment


# Contract: __exit__ runs close() regardless of how the block is left, so a
# break releases the C result at the with-block boundary. Batches already
# yielded retain their buffers and stay valid after release.
def test_stream_query_break_releases_reader(cluster, native_cluster, table, intervals):
    qlib._insert_points(
        "double", native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    kept = None
    with cluster.stream_query(query, batch_size=32) as stream:
        for batch in stream:
            kept = batch[column_name]
            break

    with pytest.raises(quasardb.UninitializedError):
        _ = list(stream)

    assert isinstance(kept, ma.MaskedArray)
    assert len(kept) > 0
    if isinstance(cluster, quasardb.NativeCluster):
        assert len(kept) == 32
    assert np.isfinite(kept.sum())


# pybind instances have no __dict__, so the reader cannot be recorded on the
# C++ connection itself; delegate stream_query and capture the returned
# reader instead.
class _RecordingConnection:
    def __init__(self, conn):
        self._conn = conn
        self.reader = None

    def stream_query(self, *args, **kwargs):
        self.reader = self._conn.stream_query(*args, **kwargs)
        return self.reader


# Contract: a break alone does not close a generator; gen.close() raises
# GeneratorExit at the suspended yield inside the generator's with-block,
# which runs the reader's __exit__ and releases the C result.
def test_qdbnp_stream_query_generator_close_releases_reader(
    cluster, native_cluster, table, intervals
):
    qlib._insert_points(
        "double", native_cluster, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    shim = _RecordingConnection(cluster)
    gen = qdbnp.stream_query(shim, query, batch_size=32)
    next(gen)
    gen.close()

    with pytest.raises(quasardb.UninitializedError):
        _ = list(shim.reader)
