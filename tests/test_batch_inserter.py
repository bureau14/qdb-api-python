# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange, int as long  # pylint: disable=W0622
from functools import reduce  # pylint: disable=W0622
import datetime
import test_table as tslib
from time import sleep

import pytest
import quasardb
import numpy as np


def _row_insertion_method(
        inserter,
        dates,
        doubles,
        blobs,
        strings,
        integers,
        timestamps):
    for i in range(len(dates)):
        inserter.start_row(dates[i])
        inserter.set_double(0, doubles[i])
        inserter.set_blob(1, blobs[i])
        inserter.set_string(2, strings[i])
        inserter.set_int64(3, integers[i])
        inserter.set_timestamp(4, timestamps[i])


def _regular_push(inserter):
    inserter.push()


def _async_push(inserter):
    inserter.push_async()
    # Wait for push_async to complete
    # Ideally we could be able to get the proper flush interval
    sleep(8)


def _fast_push(inserter):
    inserter.push_fast()


def _make_inserter_info(table):
    return [quasardb.BatchColumnInfo(table.get_name(), tslib._double_col_name(table), 100),
            quasardb.BatchColumnInfo(
                table.get_name(), tslib._blob_col_name(table), 100),
            quasardb.BatchColumnInfo(
                table.get_name(), tslib._string_col_name(table), 100),
            quasardb.BatchColumnInfo(
                table.get_name(), tslib._int64_col_name(table), 100),
            quasardb.BatchColumnInfo(table.get_name(), tslib._ts_col_name(table), 100)]


def test_non_existing_bulk_insert(qdbd_connection, entry_name):
    with pytest.raises(quasardb.Error):
        qdbd_connection.inserter(
            [quasardb.BatchColumnInfo(entry_name, "col", 10)])


def _test_with_table(
        inserter,
        table,
        intervals,
        insertion_method,
        push_method=_regular_push):
    start_time = np.datetime64('2017-01-01', 'ns')

    count = len(intervals)

    # range is right exclusive, so the timestamp has to be beyond
    whole_range = (intervals[0], intervals[-1:][0] + np.timedelta64(2, 's'))

    doubles = np.random.uniform(-100.0, 100.0, count)
    integers = np.random.randint(-100, 100, count)
    blobs = np.array([(b"content_" + bytes(i)) for i in range(count)])
    strings = np.array([("content_" + str(item)) for item in range(count)])
    timestamps = tslib._generate_dates(
        start_time + np.timedelta64('1', 'D'), count)

    insertion_method(
        inserter,
        intervals,
        doubles,
        blobs,
        strings,
        integers,
        timestamps)

    # before the push, there is nothing
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])
    assert len(results[0]) == 0

    results = table.blob_get_ranges(tslib._blob_col_name(table), [whole_range])
    assert len(results[0]) == 0

    results = table.string_get_ranges(tslib._string_col_name(table), [whole_range])
    assert len(results[0]) == 0

    results = table.int64_get_ranges(
        tslib._int64_col_name(table), [whole_range])
    assert len(results[0]) == 0

    results = table.timestamp_get_ranges(
        tslib._ts_col_name(table), [whole_range])
    assert len(results[0]) == 0

    # after push, there is everything
    push_method(inserter)
    if push_method == _async_push:
        sleep(10)

    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], doubles)

    results = table.blob_get_ranges(tslib._blob_col_name(table), [whole_range])
    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], blobs)

    results = table.string_get_ranges(tslib._string_col_name(table), [whole_range])
    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], strings)

    results = table.int64_get_ranges(
        tslib._int64_col_name(table), [whole_range])
    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], integers)

    results = table.timestamp_get_ranges(
        tslib._ts_col_name(table), [whole_range])
    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], timestamps)

    return doubles, blobs, strings, integers, timestamps


def test_successful_bulk_row_insert(qdbd_connection, table, many_intervals):
    inserter = qdbd_connection.inserter(_make_inserter_info(table))

    _test_with_table(
        inserter,
        table,
        many_intervals,
        _row_insertion_method,
        _regular_push)


def test_successful_async_bulk_row_insert(
        qdbd_connection, table, many_intervals):

    # Same test as `test_successful_bulk_row_insert` but using `push_async` to push the entries
    # This allows us to test the `push_async` feature

    inserter = qdbd_connection.inserter(_make_inserter_info(table))
    _test_with_table(
        inserter,
        table,
        many_intervals,
        _row_insertion_method,
        _async_push)


def test_successful_fast_bulk_row_insert(
        qdbd_connection, table, many_intervals):
    # Same test as `test_successful_bulk_row_insert` but using `push_async` to push the entries
    # This allows us to test the `push_async` feature

    inserter = qdbd_connection.inserter(_make_inserter_info(table))
    _test_with_table(
        inserter,
        table,
        many_intervals,
        _row_insertion_method,
        _fast_push)


def test_failed_local_table_with_wrong_columns(qdbd_connection, entry_name):
    columns = [quasardb.BatchColumnInfo(entry_name, "1000flavorsofwrong", 10)]
    with pytest.raises(quasardb.Error):
        qdbd_connection.inserter(columns)
