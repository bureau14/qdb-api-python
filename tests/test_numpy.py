# pylint: disable=missing-module-docstring,missing-function-docstring,not-an-iterable,invalid-name

from datetime import timedelta
import logging
import numpy as np
import numpy.ma as ma
import pandas as pd
import pytest
import conftest

import quasardb
import quasardb.numpy as qdbnp
import test_table as tslib
from utils import assert_indexed_arrays_equal, assert_ma_equal

logger = logging.getLogger("test-numpy")


def _unicode_to_object_array(xs):
    assert ma.isMA(xs)
    assert xs.dtype.kind == "U"

    logger.debug("converting unicode to object array for proper testing / comparison")

    mask = xs.mask
    data = np.array([str(x) for x in xs.data], dtype=np.dtype("O"))

    return ma.masked_array(data=data, mask=mask)


######
#
# Array tests
#
###
#
# The 'array' functions operate on just a single array. They use the column-oriented
# APIs, `qdb_ts_*_insert` under the hood.
#
######


@conftest.override_cdtypes("native")
def test_array_read_write_native_dtypes(array_with_index_and_table):
    """
    * qdbnp.write_array()
    * => qdb_ts_*_insert
    * => no conversion
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_array(data, index, table, column=col, dtype=dtype, infer_types=False)

    res = qdbnp.read_array(table, col)

    assert_indexed_arrays_equal((index, data), res)


@conftest.override_cdtypes("inferrable")
def test_array_read_write_inferrable_dtypes(array_with_index_and_table):
    """
    * qdbnp.write_array()
    * => qdb_ts_*_insert
    * => conversion in python
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_array(data, index, table, column=col, infer_types=True)

    res = qdbnp.read_array(table, col)
    assert_indexed_arrays_equal((index, data), res)


@conftest.override_cdtypes("native")
def test_arrays_read_write_native_dtypes(array_with_index_and_table, qdbd_connection):
    """
    * qdbnp.write_arrays()
    * => pinned writer
    * => no conversion
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays(
        [data],
        qdbd_connection,
        table,
        index=index,
        dtype=dtype,
        infer_types=False,
        push_mode=quasardb.WriterPushMode.Truncate,
    )

    res = qdbnp.read_array(table, col)

    assert_indexed_arrays_equal((index, data), res)


@conftest.override_cdtypes("inferrable")
def test_arrays_read_write_inferrable_dtypes(
    array_with_index_and_table, qdbd_connection
):
    """
    * qdbnp.write_arrays()
    * => pinned writer
    * => conversion in python
    """

    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays([data], qdbd_connection, table, index=index, infer_types=True)

    res = qdbnp.read_array(table, col)
    assert_indexed_arrays_equal((index, data), res)


@conftest.override_cdtypes("native")
def test_arrays_read_write_data_as_dict(array_with_index_and_table, qdbd_connection):
    """
    * qdbnp.write_arrays()
    * => pinned writer
    * => no conversion
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays(
        {col: data},
        qdbd_connection,
        table,
        index=index,
        dtype=dtype,
        infer_types=False,
        push_mode=quasardb.WriterPushMode.Truncate,
    )

    res = qdbnp.read_array(table, col)

    assert_indexed_arrays_equal((index, data), res)


@conftest.override_cdtypes("native")
def test_provide_index_as_dict(array_with_index_and_table, qdbd_connection):
    """
    For convenience, we allow the `$timestamp` index also to provided as a dict
    key.
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    dict_ = {"$timestamp": index, col: data}

    qdbnp.write_arrays(
        dict_,
        qdbd_connection,
        table,
        dtype=dtype,
        infer_types=False,
        truncate=True,
    )

    res = qdbnp.read_array(table, col)

    assert_indexed_arrays_equal((index, data), res)


@conftest.override_cdtypes("native")
def test_provide_index_as_dict_has_no_side_effects_sc16279(
    array_with_index_and_table, qdbd_connection
):
    """
    In earlier versions of the API, we `pop`'ed the $timestamp from the provided dict without making a
    shallow copy of the dict. This would cause re-invocations of the same function (e.g. in case of an
    error) to not work, as the original dict had been modified.

    The test below has been confirmed to trigger the original bug described in QDB-16279, and was fixed
    afterwards.
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)

    dict_ = {"$timestamp": index, col: data}
    qdbnp.write_arrays(
        dict_,
        qdbd_connection,
        table,
        dtype=dtype,
        infer_types=False,
        truncate=True,
    )

    assert "$timestamp" in dict_


######
#
# Arrays tests
#
###
#
# The 'arrays' functions operate on a collection of arrays, and use the 'exp' writer
# under the hood.
#
# They feature more advanced functionality, such as dropping duplicates and different
# insertion modes.
#
######


@conftest.override_sparsify("none")
def test_arrays_deduplicate(
    arrays_with_index_and_table, deduplication_mode, qdbd_connection
):
    """
    * qdbnp.write_arrays()
    * => pinned writer
    * => deduplicate=True or a list
    """
    (ctype, dtype, arrays, index, table) = arrays_with_index_and_table

    if ctype == quasardb.ColumnType.String:
        print("SKIP -- PROBLEM IN UNICODE STRING COMPARISONS")
        print(
            "See also: sc-11283/unicode-string-comparison-issue-with-masked-arrays-in-python-test-cases"
        )
        return

    assert len(arrays) > 1
    data1 = arrays[0]
    data2 = arrays[1]

    col = table.column_id_by_index(0)

    # Validation:
    #
    # 1. write once, no deduplicate
    # 2. read intermediate data
    # 3. write again, with deduplicate
    # 4. read data again
    #
    # data from step 2 and 4 should be identical, as the write in step 3 should
    # drop all duplicates

    def _do_write_read(xs):
        """
        Utility function that makes it easier to insert data for this test, as they will all use
        the same column/table/index/etc.
        """
        qdbnp.write_arrays(
            [xs],
            qdbd_connection,
            table,
            deduplicate="$timestamp",
            deduplication_mode=deduplication_mode,
            index=index,
            dtype=dtype,
            infer_types=False,
        )
        return qdbnp.read_array(table, col)

    res1 = _do_write_read(data1)
    assert_indexed_arrays_equal((index, data1), res1)

    # Regardless of deduplication_mode, since we're reinserting the same data, we always expect the results to be
    # identical.

    res2 = _do_write_read(data1)
    assert_indexed_arrays_equal((index, data1), res2)

    # Now, we're going to be inserting *different* data. Depending on the deduplication mode, the results will differ.
    res3 = _do_write_read(data2)

    if deduplication_mode == "drop":
        # If we drop when deduplicating, and only deduplicate on $timestamp, then we expect
        # all new data to be dropped, because the entire index (i.e. all $timestamp) conflicts.
        assert_indexed_arrays_equal((index, data1), res3)

    else:
        assert deduplication_mode == "upsert"
        # In this case, we expect all existing data to be replaced with the new data
        # assert_indexed_arrays_equal(res3, (index, data2))
        assert_indexed_arrays_equal((index, data2), res3)


######
#
# Miscellaneous tests
#
###
#
# Various tests which are important but don't belong anywhere else.
#
######


@conftest.override_cdtypes(np.dtype("unicode"))
def test_string_array_returns_unicode(array_with_index_and_table, qdbd_connection):
    """
    Validates that our C++ backend encodes unicode variable width arrays correctly.
    This yields significantly better performance than using objects, especially if
    you're not inspecting all of them.
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    assert dtype == np.dtype("unicode")

    col = table.column_id_by_index(0)
    qdbnp.write_arrays([data], qdbd_connection, table, index=index)

    (idx, xs) = qdbnp.read_array(table, col)
    assert qdbnp.dtypes_equal(xs.dtype, np.dtype("unicode"))


def test_read_arrays_reads_all_columns_when_columns_empty(qdbd_connection, table):
    index = np.array(
        [
            np.datetime64("2017-01-01T00:00:00", "ns"),
            np.datetime64("2017-01-01T00:00:01", "ns"),
            np.datetime64("2017-01-01T00:00:02", "ns"),
        ],
        dtype=np.dtype("datetime64[ns]"),
    )
    doubles = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    blobs = np.array([b"a\x00b", b"cd", b"ef"], dtype=np.object_)
    strings = np.array(["content_0", "content_1", "content_2"], dtype=np.dtype("U"))
    integers = np.array([10, 11, 12], dtype=np.int64)
    timestamps = np.array(
        [
            np.datetime64("2017-01-02T00:00:00", "ns"),
            np.datetime64("2017-01-02T00:00:01", "ns"),
            np.datetime64("2017-01-02T00:00:02", "ns"),
        ],
        dtype=np.dtype("datetime64[ns]"),
    )
    symbols = np.array(["sym_0", "sym_1", "sym_2"], dtype=np.dtype("U"))

    qdbnp.write_arrays(
        {
            tslib._double_col_name(table): doubles,
            tslib._blob_col_name(table): blobs,
            tslib._string_col_name(table): strings,
            tslib._int64_col_name(table): integers,
            tslib._ts_col_name(table): timestamps,
            tslib._symbol_col_name(table): symbols,
        },
        qdbd_connection,
        table,
        index=index,
        infer_types=False,
        dtype={
            tslib._double_col_name(table): doubles.dtype,
            tslib._blob_col_name(table): blobs.dtype,
            tslib._string_col_name(table): strings.dtype,
            tslib._int64_col_name(table): integers.dtype,
            tslib._ts_col_name(table): timestamps.dtype,
            tslib._symbol_col_name(table): symbols.dtype,
        },
    )

    idx, xs = qdbnp.read_arrays(qdbd_connection, [table], column_names=[])

    np.testing.assert_array_equal(idx, index)
    assert list(xs.keys()) == ["$table"] + [c.name for c in table.list_columns()]
    np.testing.assert_array_equal(
        xs["$table"], np.array([table.get_name()] * len(index))
    )
    np.testing.assert_array_equal(xs[tslib._double_col_name(table)], doubles)
    np.testing.assert_array_equal(xs[tslib._blob_col_name(table)], blobs)
    np.testing.assert_array_equal(xs[tslib._string_col_name(table)], strings)
    np.testing.assert_array_equal(xs[tslib._int64_col_name(table)], integers)
    np.testing.assert_array_equal(xs[tslib._ts_col_name(table)], timestamps)
    np.testing.assert_array_equal(xs[tslib._symbol_col_name(table)], symbols)


def test_read_arrays_reads_selected_columns_with_ranges(qdbd_connection, table):
    index = np.array(
        [
            np.datetime64("2017-01-01T00:00:00", "ns"),
            np.datetime64("2017-01-01T00:00:01", "ns"),
            np.datetime64("2017-01-01T00:00:02", "ns"),
            np.datetime64("2017-01-01T00:00:03", "ns"),
        ],
        dtype=np.dtype("datetime64[ns]"),
    )
    doubles = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float64)
    integers = np.array([10, 11, 12, 13], dtype=np.int64)

    qdbnp.write_arrays(
        {
            tslib._double_col_name(table): doubles,
            tslib._int64_col_name(table): integers,
        },
        qdbd_connection,
        table,
        index=index,
        infer_types=False,
        dtype={
            tslib._double_col_name(table): doubles.dtype,
            tslib._int64_col_name(table): integers.dtype,
        },
    )

    columns = [tslib._double_col_name(table), tslib._int64_col_name(table)]
    ranges = [(index[1], index[2] + np.timedelta64(1, "ns"))]

    idx, xs = qdbnp.read_arrays(
        qdbd_connection,
        [table],
        batch_size=1,
        column_names=columns,
        ranges=ranges,
    )

    np.testing.assert_array_equal(idx, index[1:3])
    assert list(xs.keys()) == ["$table"] + columns
    np.testing.assert_array_equal(
        xs["$table"], np.array([table.get_name()] * len(index[1:3]))
    )
    np.testing.assert_array_equal(xs[tslib._double_col_name(table)], doubles[1:3])
    np.testing.assert_array_equal(xs[tslib._int64_col_name(table)], integers[1:3])


def test_read_arrays_accepts_ranges(qdbd_connection, table):
    index = np.array(
        [
            np.datetime64("2017-01-01T00:00:00", "ns"),
            np.datetime64("2017-01-01T00:00:01", "ns"),
            np.datetime64("2017-01-01T00:00:02", "ns"),
        ],
        dtype=np.dtype("datetime64[ns]"),
    )
    doubles = np.array([1.0, 2.0, 3.0], dtype=np.float64)

    qdbnp.write_arrays(
        {tslib._double_col_name(table): doubles},
        qdbd_connection,
        table,
        index=index,
        infer_types=False,
        dtype={tslib._double_col_name(table): doubles.dtype},
    )

    ranges = [(index[1], index[2] + np.timedelta64(1, "ns"))]
    idx, xs = qdbnp.read_arrays(
        qdbd_connection,
        [table],
        column_names=[tslib._double_col_name(table)],
        ranges=ranges,
    )

    np.testing.assert_array_equal(idx, index[1:3])
    np.testing.assert_array_equal(
        xs["$table"], np.array([table.get_name()] * len(index[1:3]))
    )
    np.testing.assert_array_equal(xs[tslib._double_col_name(table)], doubles[1:3])


def test_read_arrays_supports_table_object(qdbd_connection, table):
    index = np.array(
        [
            np.datetime64("2017-01-01T00:00:00", "ns"),
            np.datetime64("2017-01-01T00:00:01", "ns"),
        ],
        dtype=np.dtype("datetime64[ns]"),
    )
    doubles = np.array([1.0, 2.0], dtype=np.float64)

    qdbnp.write_arrays(
        {tslib._double_col_name(table): doubles},
        qdbd_connection,
        table,
        index=index,
        infer_types=False,
        dtype={tslib._double_col_name(table): doubles.dtype},
    )

    idx, xs = qdbnp.read_arrays(
        qdbd_connection,
        [table],
        column_names=[tslib._double_col_name(table)],
    )

    np.testing.assert_array_equal(idx, index)
    np.testing.assert_array_equal(
        xs["$table"], np.array([table.get_name()] * len(index))
    )
    np.testing.assert_array_equal(xs[tslib._double_col_name(table)], doubles)


def test_read_arrays_supports_table_name(qdbd_connection, table):
    index = np.array(
        [
            np.datetime64("2017-01-01T00:00:00", "ns"),
            np.datetime64("2017-01-01T00:00:01", "ns"),
        ],
        dtype=np.dtype("datetime64[ns]"),
    )
    doubles = np.array([1.0, 2.0], dtype=np.float64)

    qdbnp.write_arrays(
        {tslib._double_col_name(table): doubles},
        qdbd_connection,
        table,
        index=index,
        infer_types=False,
        dtype={tslib._double_col_name(table): doubles.dtype},
    )

    idx, xs = qdbnp.read_arrays(
        qdbd_connection,
        [table.get_name()],
        column_names=[tslib._double_col_name(table)],
    )

    np.testing.assert_array_equal(idx, index)
    np.testing.assert_array_equal(
        xs["$table"], np.array([table.get_name()] * len(index))
    )
    np.testing.assert_array_equal(xs[tslib._double_col_name(table)], doubles)


def test_read_arrays_rejects_string_column_names(qdbd_connection, table):
    with pytest.raises(TypeError):
        qdbnp.read_arrays(qdbd_connection, [table], column_names="the_double")


def test_stream_arrays_reads_batched_results(qdbd_connection, table):
    index = np.array(
        [
            np.datetime64("2017-01-01T00:00:00", "ns"),
            np.datetime64("2017-01-01T00:00:01", "ns"),
            np.datetime64("2017-01-01T00:00:02", "ns"),
        ],
        dtype=np.dtype("datetime64[ns]"),
    )
    doubles = np.array([1.0, 2.0, 3.0], dtype=np.float64)

    qdbnp.write_arrays(
        {tslib._double_col_name(table): doubles},
        qdbd_connection,
        table,
        index=index,
        infer_types=False,
        dtype={tslib._double_col_name(table): doubles.dtype},
    )

    xs = list(
        qdbnp.stream_arrays(
            qdbd_connection,
            [table],
            batch_size=1,
            column_names=[tslib._double_col_name(table)],
        )
    )

    assert len(xs) == 3
    np.testing.assert_array_equal(np.concatenate([idx for idx, _ in xs]), index)
    np.testing.assert_array_equal(
        ma.concatenate([batch[tslib._double_col_name(table)] for _, batch in xs]),
        doubles,
    )


######
#
# Query tests
#
###
#
# Validation of numpy query component. It's not as much the queries themselves we
# want to validate, but rather how our wrapper behaves: the validations it checks
# and errors it raises, its ability to turn the data in a certain shape, etc.
#
######


def _prepare_query_test(array_with_index_and_table, qdbd_connection):
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays([data], qdbd_connection, table, index=index)
    q = 'SELECT $timestamp, {} FROM "{}"'.format(col, table.get_name())

    return (data, index, col, q)


def test_query_valid_results(array_with_index_and_table, qdbd_connection):
    (data, index, col, q) = _prepare_query_test(
        array_with_index_and_table, qdbd_connection
    )

    (idx, res) = qdbnp.query(qdbd_connection, q, index="$timestamp")
    assert_indexed_arrays_equal((index, data), (idx, res[0]))


def test_query_unknown_index(array_with_index_and_table, qdbd_connection):
    (data, index, col, q) = _prepare_query_test(
        array_with_index_and_table, qdbd_connection
    )

    with pytest.raises(KeyError):
        qdbnp.query(qdbd_connection, q, index="fooasbsdaog")


def test_query_iota_index(array_with_index_and_table, qdbd_connection):
    (data, index, col, q) = _prepare_query_test(
        array_with_index_and_table, qdbd_connection
    )

    # Simple test which just ensures when no explicit index is provided,
    # we get numbered "iota"-range back instead.
    (idx, res) = qdbnp.query(qdbd_connection, q)

    # A range of 1,2,3,4 etc will have a diff of exactly 1 for each item
    idx_ = np.diff(idx)
    assert np.all(idx_ == 1)


@conftest.override_sparsify("partial")
def test_query_masked_index(array_with_index_and_table, qdbd_connection):
    (data, index, col, q) = _prepare_query_test(
        array_with_index_and_table, qdbd_connection
    )

    with pytest.raises(ValueError):
        # We know we always have a partially masked array
        qdbnp.query(qdbd_connection, q, index=col)


def test_query_empty_result(array_with_index_and_table, qdbd_connection):
    (ctype, dtype, data, index, table) = array_with_index_and_table

    q = 'SELECT * FROM "{}"'.format(table.get_name())
    (idx, res) = qdbnp.query(qdbd_connection, q)

    assert len(idx) == 0
    assert len(res) == len(idx)


@conftest.override_sparsify("none")
@conftest.override_cdtypes([np.dtype("int64"), np.dtype("float64")])
def test_query_insert(array_with_index_and_table, qdbd_connection):
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    val = data[0]

    q = 'INSERT INTO "{}" ($timestamp, {}) VALUES (NOW(), {})'.format(
        table.get_name(), col, val
    )
    (idx, res) = qdbnp.query(qdbd_connection, q)

    assert len(idx) == 0
    assert len(res) == len(idx)


def test_regression_sc10919_sc10933(qdbd_connection, table_name, start_date, row_count):
    t = qdbd_connection.table(table_name)

    # Specific regresion test used in Python user guide / API documentation
    cols = [
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "open"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "close"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "high"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "low"),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, "volume"),
    ]

    t.create(cols)

    idx = np.array(
        [start_date + np.timedelta64(i, "s") for i in range(row_count)]
    ).astype("datetime64[ns]")

    # Note, partially sparse data just to increase some test surface
    data = {
        "open": np.random.uniform(100, 200, row_count),
        "close": np.random.uniform(100, 200, row_count),
        "volume": np.random.randint(10000, 20000, row_count),
    }

    qdbnp.write_arrays(data, qdbd_connection, t, index=idx, infer_types=False)

    # Retrieve just a single column, so that we know for sure we didn't
    # accidentally insert all null values.
    q = 'SELECT open FROM "{}"'.format(table_name)
    (idx, (res)) = qdbnp.query(qdbd_connection, q)


def test_regression_sc11333(qdbd_connection, table_name, start_date, row_count):
    """
    Ensures that we can provide data as numpy arrays as well as regular lists.
    """
    t = qdbd_connection.table(table_name)

    # Specific regresion test used in Python user guide / API documentation
    cols = [
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "open"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "close"),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, "volume"),
    ]
    t.create(cols)

    idx = np.array(
        [start_date + np.timedelta64(i, "s") for i in range(row_count)]
    ).astype("datetime64[ns]")

    # Note, partially sparse data just to increase some test surface
    data = {
        "open": np.random.uniform(100, 200, row_count),
        "close": np.random.uniform(100, 200, row_count),
        "volume": np.random.randint(10000, 20000, row_count),
    }

    df = pd.DataFrame(data, index=idx)
    data = df.to_numpy()

    # Degenerate use case: by default, df.to_numpy() pivots the data incorrectly, in which case we
    # expect an error to be raised.
    assert len(data) == row_count
    assert len(data[0]) == len(cols)

    with pytest.raises(qdbnp.InvalidDataCardinalityError):
        qdbnp.write_arrays(data, qdbd_connection, t, index=idx, infer_types=False)

    data = data.transpose()
    qdbnp.write_arrays(data, qdbd_connection, t, index=idx, infer_types=True)

    cols = ["open", "close", "volume"]
    for i in range(len(cols)):
        col = cols[i]

        res = qdbnp.read_array(t, col)
        assert_indexed_arrays_equal((idx, data[i]), res)


def test_write_through_flag(arrays_with_index_and_table, qdbd_connection):
    (ctype, dtype, data, index, table) = arrays_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays(
        [data[0]], qdbd_connection, table, index=index, write_through=True
    )

    res = qdbnp.read_array(table, col)
    assert_indexed_arrays_equal((index, data[0]), res)


def test_write_through_flag_throws_when_incorrect(
    arrays_with_index_and_table, qdbd_connection
):
    (ctype, dtype, data, index, table) = arrays_with_index_and_table

    with pytest.raises(quasardb.InvalidArgumentError):
        qdbnp.write_arrays(
            [data[0]], qdbd_connection, table, index=index, write_through="wrong!"
        )
