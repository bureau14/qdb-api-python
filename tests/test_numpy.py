# pylint: disable=missing-module-docstring,missing-function-docstring,not-an-iterable,invalid-name

from datetime import timedelta
import logging
import numpy as np
import numpy.ma as ma
import pytest
import conftest

import quasardb
import quasardb.numpy as qdbnp

logger = logging.getLogger("test-numpy")

def _assert_ma_equal(lhs, rhs):
    """
    A bit hacky way to compare two masked arrays for equality, as the default
    numpy way of doing so is a bit meh (first converts to np.array, and *then*
    compares, which defeats the point of using ma -> ergo meh est).
    """
    assert ma.isMA(lhs)
    assert ma.isMA(rhs)

    assert ma.count_masked(lhs) == ma.count_masked(rhs)

    lhs_ = lhs.torecords()
    rhs_ = rhs.torecords()

    for ((lval, lmask), (rval, rmask)) in zip(lhs_, rhs_):
        assert lmask == rmask

        if not lmask:
            assert lval == rval


def _unicode_to_object_array(xs):
    assert ma.isMA(xs)
    assert xs.dtype.kind == 'U'

    logger.debug("converting unicode to object array for proper testing / comparison")

    mask = xs.mask
    data = np.array([str(x) for x in xs.data],
                    dtype=np.dtype('O'))

    return ma.masked_array(data=data, mask=mask)


def _assert_arrays_equal(lhs, rhs):
    """
    Accepts two "array results", which is two tuples of a timestamp and a
    data array.

    lhs is typically the "generated" / "input" data, rhs is typically the data
    returned by qdb.
    """
    (lhs_idx, lhs_data) = lhs
    (rhs_idx, rhs_data) = rhs

    # The index (timestamps) is not allowed to be null, so should never be
    # a masked array anyway.
    assert not ma.isMA(lhs_idx)
    assert not ma.isMA(rhs_idx)
    np.testing.assert_array_equal(lhs_idx, rhs_idx)

    # If any of these is not a masked array, let's coerce it.
    # We do an extra check of 'isMA' because that protects somewhat against
    # silly bugs like ensure_ma always returning an empty list.
    if not ma.isMA(lhs_data):
        lhs_data = qdbnp.ensure_ma(lhs_data)

    if not ma.isMA(rhs_data):
        rhs_data = qdbnp.ensure_ma(rhs_data)

    _assert_ma_equal(lhs_data, rhs_data)


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

@conftest.override_cdtypes('native')
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

    if ctype == quasardb.ColumnType.String:
        print("SKIP -- PROBLEM IN UNICODE STRING COMPARISONS")
        print("See also: sc-11283/unicode-string-comparison-issue-with-masked-arrays-in-python-test-cases")
    else:
        _assert_arrays_equal((index, data), res)


@conftest.override_cdtypes('inferrable')
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
    _assert_arrays_equal((index, data), res)


@conftest.override_cdtypes('native')
def test_arrays_read_write_native_dtypes(array_with_index_and_table, qdbd_connection):
    """
     * qdbnp.write_arrays()
     * => pinned writer
     * => no conversion
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays([data], qdbd_connection, table, index=index, dtype=dtype, infer_types=False, truncate=True)

    res = qdbnp.read_array(table, col)

    if ctype == quasardb.ColumnType.String:
        print("SKIP -- PROBLEM IN UNICODE STRING COMPARISONS")
        print("See also: sc-11283/unicode-string-comparison-issue-with-masked-arrays-in-python-test-cases")
    else:
        _assert_arrays_equal((index, data), res)


@conftest.override_cdtypes('inferrable')
def test_arrays_read_write_inferrable_dtypes(array_with_index_and_table, qdbd_connection):
    """
     * qdbnp.write_arrays()
     * => pinned writer
     * => conversion in python
    """

    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays([data], qdbd_connection, table, index=index, infer_types=True)

    res = qdbnp.read_array(table, col)
    _assert_arrays_equal((index, data), res)

@conftest.override_cdtypes('native')
def test_arrays_read_write_data_as_dict(array_with_index_and_table, qdbd_connection):
    """
     * qdbnp.write_arrays()
     * => pinned writer
     * => no conversion
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    qdbnp.write_arrays({col: data}, qdbd_connection, table, index=index, dtype=dtype, infer_types=False, truncate=True)

    res = qdbnp.read_array(table, col)

    if ctype == quasardb.ColumnType.String:
        print("SKIP -- PROBLEM IN UNICODE STRING COMPARISONS")
        print("See also: sc-11283/unicode-string-comparison-issue-with-masked-arrays-in-python-test-cases")
    else:
        _assert_arrays_equal((index, data), res)


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

@conftest.override_sparsify('none')
def test_arrays_deduplicate(arrays_with_index_and_table, deduplication_mode, qdbd_connection):
    """
     * qdbnp.write_arrays()
     * => pinned writer
     * => deduplicate=True or a list
    """
    (ctype, dtype, arrays, index, table) = arrays_with_index_and_table

    if ctype == quasardb.ColumnType.String:
        print("SKIP -- PROBLEM IN UNICODE STRING COMPARISONS")
        print("See also: sc-11283/unicode-string-comparison-issue-with-masked-arrays-in-python-test-cases")
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
        qdbnp.write_arrays([xs],
                           qdbd_connection,
                           table,
                           deduplicate='$timestamp',
                           deduplication_mode=deduplication_mode,
                           index=index,
                           dtype=dtype,
                           infer_types=False)
        return qdbnp.read_array(table, col)

    res1 = _do_write_read(data1)
    _assert_arrays_equal((index, data1), res1)

    # Regardless of deduplication_mode, since we're reinserting the same data, we always expect the results to be
    # identical.

    res2 = _do_write_read(data1)
    _assert_arrays_equal((index, data1), res2)


    # Now, we're going to be inserting *different* data. Depending on the deduplication mode, the results will differ.
    res3 = _do_write_read(data2)

    if deduplication_mode == 'drop':
        # If we drop when deduplicating, and only deduplicate on $timestamp, then we expect
        # all new data to be dropped, because the entire index (i.e. all $timestamp) conflicts.
        _assert_arrays_equal((index, data1), res3)

    else:
        assert deduplication_mode == 'upsert'
        # In this case, we expect all existing data to be replaced with the new data
        # _assert_arrays_equal(res3, (index, data2))
        _assert_arrays_equal((index, data2), res3)



######
#
# Miscellaneous tests
#
###
#
# Various tests which are important but don't belong anywhere else.
#
######

@conftest.override_cdtypes(np.dtype('unicode'))
def test_string_array_returns_unicode(array_with_index_and_table, qdbd_connection):
    """
    Validates that our C++ backend encodes unicode variable width arrays correctly.
    This yields significantly better performance than using objects, especially if
    you're not inspecting all of them.
    """
    (ctype, dtype, data, index, table) = array_with_index_and_table

    assert dtype == np.dtype('unicode')

    col = table.column_id_by_index(0)
    qdbnp.write_arrays([data], qdbd_connection, table, index=index)

    (idx, xs) = qdbnp.read_array(table, col)
    assert qdbnp.dtypes_equal(xs.dtype, np.dtype('unicode'))

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
    (data, index, col, q) = _prepare_query_test(array_with_index_and_table, qdbd_connection)

    (idx, res) = qdbnp.query(qdbd_connection, q, index='$timestamp')
    _assert_arrays_equal((index, data), (idx, res[0]))


def test_query_unknown_index(array_with_index_and_table, qdbd_connection):
    (data, index, col, q) = _prepare_query_test(array_with_index_and_table, qdbd_connection)

    with pytest.raises(KeyError):
        qdbnp.query(qdbd_connection, q, index='fooasbsdaog')


def test_query_iota_index(array_with_index_and_table, qdbd_connection):
    (data, index, col, q) = _prepare_query_test(array_with_index_and_table, qdbd_connection)

    # Simple test which just ensures when no explicit index is provided,
    # we get numbered "iota"-range back instead.
    (idx, res) = qdbnp.query(qdbd_connection, q)

    # A range of 1,2,3,4 etc will have a diff of exactly 1 for each item
    idx_ = np.diff(idx)
    assert np.all(idx_ == 1)


@conftest.override_sparsify('partial')
def test_query_masked_index(array_with_index_and_table, qdbd_connection):
    (data, index, col, q) = _prepare_query_test(array_with_index_and_table, qdbd_connection)

    with pytest.raises(ValueError):
        # We know we always have a partially masked array
        qdbnp.query(qdbd_connection, q, index=col)


def test_query_empty_result(array_with_index_and_table, qdbd_connection):
    (ctype, dtype, data, index, table) = array_with_index_and_table

    q = 'SELECT * FROM "{}"'.format(table.get_name())
    (idx, res) = qdbnp.query(qdbd_connection, q)

    assert len(idx) == 0
    assert len(res) == len(idx)


@conftest.override_sparsify('none')
@conftest.override_cdtypes([np.dtype('int64'),
                            np.dtype('float64')])
def test_query_insert(array_with_index_and_table, qdbd_connection):
    (ctype, dtype, data, index, table) = array_with_index_and_table

    col = table.column_id_by_index(0)
    val = data[0]

    q = 'INSERT INTO "{}" ($timestamp, {}) VALUES (NOW(), {})'.format(table.get_name(), col, val)
    (idx, res) = qdbnp.query(qdbd_connection, q)

    assert len(idx) == 0
    assert len(res) == len(idx)


def test_regression_sc10919_sc10933(qdbd_connection, table_name, start_date, row_count):
    t = qdbd_connection.table(table_name)

    # Specific regresion test used in Python user guide / API documentation
    cols = [quasardb.ColumnInfo(quasardb.ColumnType.Double, "open"),
            quasardb.ColumnInfo(quasardb.ColumnType.Double, "close"),
            quasardb.ColumnInfo(quasardb.ColumnType.Double, "high"),
            quasardb.ColumnInfo(quasardb.ColumnType.Double, "low"),
            quasardb.ColumnInfo(quasardb.ColumnType.Int64, "volume")]

    t.create(cols)

    idx = np.array([start_date + np.timedelta64(i, 's')
                    for i in range(row_count)]).astype('datetime64[ns]')

    # Note, partially sparse data just to increase some test surface
    data = {'open': np.random.uniform(100, 200, row_count),
            'close': np.random.uniform(100, 200, row_count),
            'volume': np.random.randint(10000, 20000, row_count)}

    qdbnp.write_arrays(data, qdbd_connection, t, index=idx, infer_types=False)

    # Retrieve just a single column, so that we know for sure we didn't
    # accidentally insert all null values.
    q = 'SELECT open FROM "{}"'.format(table_name)
    (idx, (res)) = qdbnp.query(qdbd_connection, q)
