# pylint: disable=C0103,C0111,C0302,W0212
import math

import pytest
import numpy as np
import numpy.ma as ma
import quasardb

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
def test_stream_query_equals_query_numpy(value_type, qdbd_connection, table, intervals):
    qlib._insert_points(
        value_type, qdbd_connection, table, intervals=intervals, points=100
    )
    column_name = qlib._column_name(table, value_type)
    query = _select_column_query(table, column_name)

    expected = dict(qdbd_connection.query_numpy(query))

    batches = _stream_batches(qdbd_connection, query, batch_size=32)
    streamed = _concat_column(batches, column_name)

    assert streamed.dtype.kind == expected[column_name].dtype.kind
    assert_ma_equal(streamed, expected[column_name])


##
# Batching contract


def test_stream_query_batching_contract(
    qdbd_connection, table, intervals, row_count, reader_batch_size
):
    qlib._insert_points(
        "double", qdbd_connection, table, intervals=intervals, points=row_count
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    with qdbd_connection.stream_query(query, batch_size=reader_batch_size) as stream:
        assert stream.get_batch_size() == reader_batch_size
        batches = list(stream)

    expected_batch_count = math.ceil(row_count / reader_batch_size)
    assert len(batches) == expected_batch_count

    for batch in batches[:-1]:
        assert len(batch[column_name]) == reader_batch_size

    remainder = row_count - (expected_batch_count - 1) * reader_batch_size
    assert len(batches[-1][column_name]) == remainder


def test_stream_query_zero_batch_size_yields_single_batch(
    qdbd_connection, table, intervals
):
    row_count = 100
    qlib._insert_points(
        "double", qdbd_connection, table, intervals=intervals, points=row_count
    )
    column_name = qlib._column_name(table, "double")
    query = _select_column_query(table, column_name)

    with qdbd_connection.stream_query(query) as stream:
        assert stream.get_batch_size() == 0
        batches = list(stream)

    assert len(batches) == 1
    assert len(batches[0][column_name]) == row_count


##
# Argument handling


def test_stream_query_batch_size_is_keyword_only(qdbd_connection, table):
    query = 'SELECT * FROM "{}"'.format(table.get_name())

    with pytest.raises(TypeError):
        qdbd_connection.stream_query(query, 100)


##
# Lifecycle errors


def test_stream_query_cannot_iterate_without_enter(qdbd_connection, table):
    query = 'SELECT * FROM "{}"'.format(table.get_name())
    stream = qdbd_connection.stream_query(query)

    with pytest.raises(quasardb.UninitializedError):
        _ = list(stream)


def test_query_reader_cannot_be_instantiated_directly():
    with pytest.raises(quasardb.DirectInstantiationError):
        quasardb.QueryReader()


##
# Empty result


def test_stream_query_empty_result_yields_zero_batches(qdbd_connection, table):
    query = 'SELECT * FROM "{}" IN RANGE(2016-01-01, 2016-12-12)'.format(
        table.get_name()
    )

    with qdbd_connection.stream_query(query) as stream:
        assert list(stream) == []


def test_stream_query_dml_yields_zero_batches(qdbd_connection, table):
    # DML statements return no result set: the C result stays NULL.
    query = 'INSERT INTO "{}" ($timestamp, "{}") VALUES (NOW(), 1.0)'.format(
        table.get_name(), tslib._double_col_name(table)
    )

    with qdbd_connection.stream_query(query) as stream:
        assert list(stream) == []


##
# Schema-kind stability across batches


def test_stream_query_schema_stable_when_first_batch_all_null(
    qdbd_connection, table, intervals
):
    start_time = tslib._start_time(intervals)
    row_count = 100
    batch_size = 50

    (timestamps, values) = tslib._generate_double_ts(start_time, row_count)

    # First batch entirely null, second batch fully populated: the stream's
    # global probe must still type the first batch as float64.
    mask = np.zeros(row_count, dtype=bool)
    mask[:batch_size] = True
    xs = ma.array(data=values, mask=mask)

    column_name = tslib._double_col_name(table)
    qlib._write_points(qdbd_connection, table, column_name, (timestamps, xs))

    query = _select_column_query(table, column_name)
    batches = _stream_batches(qdbd_connection, query, batch_size=batch_size)

    assert len(batches) == 2

    first = batches[0][column_name]
    second = batches[1][column_name]

    assert first.dtype.kind == second.dtype.kind
    assert first.dtype == np.dtype("float64")

    assert ma.count_masked(first) == batch_size
    assert ma.count_masked(second) == 0


def test_stream_query_entirely_null_column_is_masked_float64(
    qdbd_connection, table, intervals
):
    # Writing doubles creates rows; selecting a column that was never written
    # yields all-null cells for those rows.
    qlib._insert_points(
        "double", qdbd_connection, table, intervals=intervals, points=100
    )
    column_name = tslib._int64_col_name(table)
    query = _select_column_query(table, column_name)

    batches = _stream_batches(qdbd_connection, query, batch_size=50)

    assert len(batches) == 2

    for batch in batches:
        xs = batch[column_name]
        assert xs.dtype == np.dtype("float64")
        assert ma.count_masked(xs) == len(xs)


##
# Mixed-type columns


def test_stream_query_mixed_type_column_raises(qdbd_connection, entry_name, intervals):
    start_time = tslib._start_time(intervals)

    t1_name = entry_name + "_double"
    t2_name = entry_name + "_blob"

    t1 = qdbd_connection.table(t1_name)
    t1.create([quasardb.ColumnInfo(quasardb.ColumnType.Double, "the_col")])
    t2 = qdbd_connection.table(t2_name)
    t2.create([quasardb.ColumnInfo(quasardb.ColumnType.Blob, "the_col")])

    qlib._write_points(
        qdbd_connection, t1, "the_col", tslib._generate_double_ts(start_time, 1)
    )
    qlib._write_points(
        qdbd_connection, t2, "the_col", tslib._generate_blob_ts(start_time, 1)
    )

    query = 'SELECT the_col FROM "{}", "{}"'.format(t1_name, t2_name)

    # The tag mismatch is detected during batch conversion, not at __enter__:
    # the probe only reads type tags and cannot fail on mixed columns.
    with qdbd_connection.stream_query(query) as stream:
        with pytest.raises(quasardb.IncompatibleTypeError):
            _ = list(stream)
