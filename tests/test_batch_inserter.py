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
    sleep(15)


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


def _generate_data(count, start=np.datetime64('2017-01-01', 'ns')):
    doubles = np.random.uniform(-100.0, 100.0, count)
    integers = np.random.randint(-100, 100, count)
    blobs = np.array(
        list(
            np.random.bytes(
                np.random.randint(
                    8,
                    16)) for i in range(count)),
        'O')
    strings = np.array([("content_" + str(item)) for item in range(count)])
    timestamps = tslib._generate_dates(
        start + np.timedelta64('1', 'D'), count)

    return (doubles, integers, blobs, strings, timestamps)


def _set_batch_inserter_data(inserter, intervals, data, start=0):
    (doubles, integers, blobs, strings, timestamps) = data

    for i in range(start, len(intervals)):
        inserter.start_row(intervals[i])
        inserter.set_double(0, doubles[i])
        inserter.set_blob(1, blobs[i])
        inserter.set_string(2, strings[i])
        inserter.set_int64(3, integers[i])
        inserter.set_timestamp(4, timestamps[i])


def _assert_results(table, intervals, data):
    (doubles, integers, blobs, strings, timestamps) = data

    whole_range = (intervals[0], intervals[-1:][0] + np.timedelta64(2, 's'))
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], doubles)

    results = table.blob_get_ranges(tslib._blob_col_name(table), [whole_range])
    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], blobs)

    results = table.string_get_ranges(
        tslib._string_col_name(table), [whole_range])
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


def _test_with_table(
        inserter,
        table,
        intervals,
        push_method=_regular_push,
        data=None):

    if data is None:
        data = _generate_data(len(intervals))

    # range is right exclusive, so the timestamp has to be beyond
    whole_range = (intervals[0], intervals[-1:][0] + np.timedelta64(2, 's'))

    (doubles, integers, blobs, strings, timestamps) = data

    _set_batch_inserter_data(inserter, intervals, data)

    # before the push, there is nothing
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])
    assert len(results[0]) == 0

    results = table.blob_get_ranges(tslib._blob_col_name(table), [whole_range])
    assert len(results[0]) == 0

    results = table.string_get_ranges(
        tslib._string_col_name(table), [whole_range])
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
        sleep(20)

    _assert_results(table, intervals, data)

    return doubles, blobs, strings, integers, timestamps


def test_successful_bulk_row_insert(qdbd_connection, table, many_intervals):
    inserter = qdbd_connection.inserter(_make_inserter_info(table))

    _test_with_table(
        inserter,
        table,
        many_intervals,
        _regular_push)


def test_successful_secure_bulk_row_insert(
        qdbd_secure_connection,
        secure_table,
        many_intervals):
    inserter = qdbd_secure_connection.inserter(
        _make_inserter_info(secure_table))

    _test_with_table(
        inserter,
        secure_table,
        many_intervals,
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
        _fast_push)


def test_failed_local_table_with_wrong_columns(qdbd_connection, entry_name):
    columns = [quasardb.BatchColumnInfo(entry_name, "1000flavorsofwrong", 10)]
    with pytest.raises(quasardb.Error):
        qdbd_connection.inserter(columns)


def test_push_truncate_implicit_range(qdbd_connection, table, many_intervals):

    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    (doubles, integers, blobs, strings, timestamps) = data

    # Insert once
    inserter = qdbd_connection.inserter(_make_inserter_info(table))
    _set_batch_inserter_data(inserter, many_intervals, data)
    inserter.push()

    # Compare results, should be equal
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)

    # Insert regular, twice
    _set_batch_inserter_data(inserter, many_intervals, data)
    inserter.push()

    # Compare results, should now have the same data twice
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    assert len(results[1]) == 2 * len(doubles)

    # Insert truncate, should now have original data again
    _set_batch_inserter_data(inserter, many_intervals, data)
    inserter.push_truncate()

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)


def test_push_truncate_explicit_range(qdbd_connection, table, many_intervals):

    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    (doubles, integers, blobs, strings, timestamps) = data

    inserter = qdbd_connection.inserter(_make_inserter_info(table))

    # Insert once
    truncate_range = (whole_range[0],
                      whole_range[1] + np.timedelta64(1, 'ns'))

    _set_batch_inserter_data(inserter, many_intervals, data)
    inserter.push()

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)

    # If we now set the same data, skip the first element, but keep
    # the same time range, the first element will *not* be present in
    # the resulting dataset.
    _set_batch_inserter_data(inserter, many_intervals, data, start=1)
    inserter.push_truncate(range=truncate_range)

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals[1:])
    np.testing.assert_array_equal(results[1], doubles[1:])


def test_push_truncate_throws_error_on_invalid_range(
        qdbd_connection, table, many_intervals):
    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    (doubles, integers, blobs, strings, timestamps) = data

    # Insert truncate with explicit timerange, we point the start right after the
    # first element in our dataset. This means that the range does not overlap all
    # the data anymore.
    truncate_range = (whole_range[0] + np.timedelta64(1, 'ns'),
                      whole_range[1] + np.timedelta64(1, 'ns'))

    inserter = qdbd_connection.inserter(_make_inserter_info(table))
    _set_batch_inserter_data(inserter, many_intervals, data)
    with pytest.raises(quasardb.Error):
        inserter.push_truncate(range=truncate_range)
