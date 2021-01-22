# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange, int as long  # pylint: disable=W0622
from functools import reduce  # pylint: disable=W0622
import datetime
import test_table as tslib
from time import sleep

import pytest
import quasardb
import numpy as np


def _generate_data(count, start=np.datetime64('2017-01-01', 'ns')):
    integers = np.random.randint(-100, 100, count)
    timestamps = tslib._generate_dates(
        start + np.timedelta64('1', 'D'), count)

    return (integers, timestamps)


def test_rows_with_none_values(qdbd_connection, table_name):
    table = qdbd_connection.table(table_name)
    node_id = quasardb.ColumnInfo(quasardb.ColumnType.Int64, "node_id")
    x = quasardb.ColumnInfo(quasardb.ColumnType.Int64, "x")
    y = quasardb.ColumnInfo(quasardb.ColumnType.Int64, "y")

    table.create([node_id, x, y])

    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns'),
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns')]
    node_id = [10, 12]
    x = [None, 1]
    y = [2, None]

    pinned_writer.set_int64_column(0, timestamps, node_id)
    pinned_writer.set_int64_column(1, timestamps, x)
    pinned_writer.set_int64_column(2, timestamps, y)

    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","node_id","x","y" FROM "{}"'.format(
            table.get_name()))

    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row['$timestamp'] == timestamps[idx]
        assert row['node_id'] == node_id[idx]
        assert row['x'] == x[idx]
        assert row['y'] == y[idx]


def test_incorrect_type_double(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(5):
        if idx == 0:
            continue
        with pytest.raises(quasardb.Error):
            pinned_writer.set_double(idx, 1.1)


def test_successful_type_double(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = 1.1

    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(timestamp)
    pinned_writer.set_double(0, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_double" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_double'] == value


def test_incorrect_type_blob(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(5):
        if idx == 1:
            continue
        with pytest.raises(quasardb.Error):
            pinned_writer.set_blob(idx, b'aaa')


def test_successful_type_blob(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = b'aaa'

    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(timestamp)
    pinned_writer.set_blob(1, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_blob" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_blob'] == value


def test_incorrect_type_string(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(5):
        if idx == 2:
            continue
        with pytest.raises(quasardb.Error):
            pinned_writer.set_string(idx, 'aaa')


def test_successful_type_string(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = 'aaa'

    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(timestamp)
    pinned_writer.set_string(2, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_string" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_string'] == value


def test_incorrect_type_int64(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(5):
        if idx == 3:
            continue
        with pytest.raises(quasardb.Error):
            pinned_writer.set_int64(idx, 1)


def test_successful_type_int64(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = 1

    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(timestamp)
    pinned_writer.set_int64(3, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_int64'] == value


def test_incorrect_type_timestamp(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(5):
        if idx == 4:
            continue
        with pytest.raises(quasardb.Error):
            pinned_writer.set_timestamp(
                idx, np.datetime64(
                    '2020-01-01T00:00:00', 'ns'))


def test_successful_type_timestamp(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = np.datetime64('2020-01-01T00:00:00', 'ns')

    pinned_writer = qdbd_connection.pinned_writer([table])
    pinned_writer.start_row(timestamp)
    pinned_writer.set_timestamp(4, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_ts" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_ts'] == value


def test_insert_data_ordered_single_shard(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns'),
        np.datetime64(
            '2020-01-01T00:00:01',
            'ns')]
    values = [0, 1]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row['$timestamp'] == timestamps[idx]
        assert row['the_int64'] == values[idx]


def test_insert_data_ordered_multiple_shards(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [
        np.datetime64(
            '2020-01-01',
            'ns'),
        np.datetime64(
            '2020-01-02',
            'ns')]
    values = [1, 2]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row['$timestamp'] == timestamps[idx]
        assert row['the_int64'] == values[idx]


def test_insert_data_unordered_single_shard(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [
        np.datetime64(
            '2020-01-01T00:00:01',
            'ns'),
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns')]
    values = [1, 0]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))

    assert res[0]['$timestamp'] == timestamps[1]
    assert res[0]['the_int64'] == values[1]
    assert res[1]['$timestamp'] == timestamps[0]
    assert res[1]['the_int64'] == values[0]


def test_insert_data_unordered_multiple_shards(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [
        np.datetime64(
            '2020-01-02',
            'ns'),
        np.datetime64(
            '2020-01-01',
            'ns')]
    values = [2, 1]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))

    assert res[0]['$timestamp'] == timestamps[1]
    assert res[0]['the_int64'] == values[1]
    assert res[1]['$timestamp'] == timestamps[0]
    assert res[1]['the_int64'] == values[0]


def test_insert_data_ordered_single_shard_two_push(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns'),
        np.datetime64(
            '2020-01-01T00:00:01',
            'ns')]
    values = [0, 1]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.push()

    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row['$timestamp'] == timestamps[idx]
        assert row['the_int64'] == values[idx]


def test_insert_data_ordered_multiple_shards_two_push(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [
        np.datetime64(
            '2020-01-01',
            'ns'),
        np.datetime64(
            '2020-01-02',
            'ns')]
    values = [1, 2]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.push()

    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row['$timestamp'] == timestamps[idx]
        assert row['the_int64'] == values[idx]

# generative tests


def _row_insertion_method(
        writer,
        dates,
        doubles,
        blobs,
        strings,
        integers,
        timestamps):
    for i in range(len(dates)):
        writer.start_row(dates[i])
        writer.set_double(0, doubles[i])
        writer.set_blob(1, blobs[i])
        writer.set_string(2, strings[i])
        writer.set_int64(3, integers[i])
        writer.set_timestamp(4, timestamps[i])


def _regular_push(writer):
    writer.push()


def _async_push(writer):
    writer.push_async()
    # Wait for push_async to complete
    # Ideally we could be able to get the proper flush interval
    sleep(15)


def _fast_push(writer):
    writer.push_fast()


def _generate_data(count, start=np.datetime64('2017-01-01', 'ns')):
    doubles = np.random.uniform(-100.0, 100.0, count)
    integers = np.random.randint(-100, 100, count)
    blobs = np.array(list(np.random.bytes(np.random.randint(8, 16))
                          for i in range(count)), 'O')
    strings = np.array([("content_" + str(item)) for item in range(count)])
    timestamps = tslib._generate_dates(
        start + np.timedelta64('1', 'D'), count)

    return (doubles, integers, blobs, strings, timestamps)


def _set_batch_writer_data(writer, intervals, data, start=0):
    (doubles, integers, blobs, strings, timestamps) = data

    for i in range(start, len(intervals)):
        writer.start_row(intervals[i])
        writer.set_double(0, doubles[i])
        writer.set_blob(1, blobs[i])
        writer.set_string(2, strings[i])
        writer.set_int64(3, integers[i])
        writer.set_timestamp(4, timestamps[i])


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
        writer,
        table,
        intervals,
        push_method=_regular_push,
        set_method=_set_batch_writer_data,
        data=None):

    if data is None:
        data = _generate_data(len(intervals))

    # range is right exclusive, so the timestamp has to be beyond
    whole_range = (intervals[0], intervals[-1:][0] + np.timedelta64(2, 's'))

    (doubles, integers, blobs, strings, timestamps) = data

    set_method(writer, intervals, data)

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
    push_method(writer)
    if push_method == _async_push:
        sleep(20)

    _assert_results(table, intervals, data)

    return doubles, blobs, strings, integers, timestamps


def test_insert(qdbd_connection, table, many_intervals):
    writer = qdbd_connection.pinned_writer([table])

    _test_with_table(
        writer,
        table,
        many_intervals,
        _regular_push)


def test_insert_secure(qdbd_secure_connection, secure_table, many_intervals):
    writer = qdbd_secure_connection.pinned_writer([secure_table])

    _test_with_table(
        writer,
        secure_table,
        many_intervals,
        _regular_push)


def test_insert_async(
        qdbd_connection, table, many_intervals):

    # Same test as `test_insert` but using `push_async` to push the entries
    # This allows us to test the `push_async` feature

    writer = qdbd_connection.pinned_writer([table])
    _test_with_table(
        writer,
        table,
        many_intervals,
        _async_push)


def test_insert_fast(
        qdbd_connection, table, many_intervals):
    # Same test as `test_insert` but using `push_fast` to push the entries
    # This allows us to test the `push_fast` feature

    writer = qdbd_connection.pinned_writer([table])
    _test_with_table(
        writer,
        table,
        many_intervals,
        _fast_push)


def test_insert_truncate_implicit_range(
        qdbd_connection, table, many_intervals):

    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    # (doubles, integers, blobs, strings, timestamps) = data
    (doubles, _, _, _, _) = data

    # Insert once
    writer = qdbd_connection.pinned_writer([table])
    _set_batch_writer_data(writer, many_intervals, data)
    writer.push()

    # Compare results, should be equal
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)

    # Insert regular, twice
    _set_batch_writer_data(writer, many_intervals, data)
    writer.push()

    # Compare results, should now have the same data twice
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    assert len(results[1]) == 2 * len(doubles)

    # Insert truncate, should now have original data again
    _set_batch_writer_data(writer, many_intervals, data)
    writer.push_truncate()

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)


def test_insert_truncate_explicit_range(
        qdbd_connection, table, many_intervals):

    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    # (doubles, integers, blobs, strings, timestamps) = data
    (doubles, _, _, _, _) = data

    writer = qdbd_connection.pinned_writer([table])

    # Insert once
    truncate_range = (whole_range[0],
                      whole_range[1] + np.timedelta64(1, 'ns'))

    _set_batch_writer_data(writer, many_intervals, data)
    writer.push()

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)

    # If we now set the same data, skip the first element, but keep
    # the same time range, the first element will *not* be present in
    # the resulting dataset.
    _set_batch_writer_data(writer, many_intervals, data, start=1)
    writer.push_truncate(range=truncate_range)

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals[1:])
    np.testing.assert_array_equal(results[1], doubles[1:])


def test_insert_truncate_throws_error_on_invalid_range(
        qdbd_connection, table, many_intervals):
    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    # (doubles, integers, blobs, strings, timestamps) = data
    (_, _, _, _, _) = data

    # Insert truncate with explicit timerange, we point the start right after the
    # first element in our dataset. This means that the range does not overlap all
    # the data anymore.
    truncate_range = (whole_range[0] + np.timedelta64(1, 'ns'),
                      whole_range[1] + np.timedelta64(1, 'ns'))

    writer = qdbd_connection.pinned_writer([table])
    _set_batch_writer_data(writer, many_intervals, data)
    with pytest.raises(quasardb.Error):
        writer.push_truncate(range=truncate_range)


def _set_batch_writer_column_data(writer, intervals, data, start=0):
    (doubles, integers, blobs, strings, timestamps) = data

    writer.set_double_column(0, intervals[start:], doubles[start:])
    writer.set_blob_column(1, intervals[start:], blobs[start:])
    writer.set_string_column(2, intervals[start:], strings[start:])
    writer.set_int64_column(3, intervals[start:], integers[start:])
    writer.set_timestamp_column(4, intervals[start:], timestamps[start:])


def test_insert_column(qdbd_connection, table, many_intervals):
    writer = qdbd_connection.pinned_writer([table])

    _test_with_table(
        writer,
        table,
        many_intervals,
        _regular_push,
        _set_batch_writer_column_data)


def test_insert_column_secure(
        qdbd_secure_connection,
        secure_table,
        many_intervals):
    writer = qdbd_secure_connection.pinned_writer([secure_table])

    _test_with_table(
        writer,
        secure_table,
        many_intervals,
        _regular_push,
        _set_batch_writer_column_data)


def test_insert_column_async(
        qdbd_connection, table, many_intervals):

    # Same test as `test_insert_column` but using `push_async` to push the entries
    # This allows us to test the `push_async` feature

    writer = qdbd_connection.pinned_writer([table])
    _test_with_table(
        writer,
        table,
        many_intervals,
        _async_push,
        _set_batch_writer_column_data)


def test_insert_column_fast(
        qdbd_connection, table, many_intervals):
    # Same test as `test_insert_column` but using `push_fast` to push the entries
    # This allows us to test the `push_fast` feature

    writer = qdbd_connection.pinned_writer([table])
    _test_with_table(
        writer,
        table,
        many_intervals,
        _fast_push,
        _set_batch_writer_column_data)


def test_insert_column_truncate_implicit_range(
        qdbd_connection, table, many_intervals):

    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    # (doubles, integers, blobs, strings, timestamps) = data
    (doubles, _, _, _, _) = data

    # Insert once
    writer = qdbd_connection.pinned_writer([table])
    _set_batch_writer_column_data(writer, many_intervals, data)
    writer.push()

    # Compare results, should be equal
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)

    # Insert regular, twice
    _set_batch_writer_column_data(writer, many_intervals, data)
    writer.push()

    # Compare results, should now have the same data twice
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    assert len(results[1]) == 2 * len(doubles)

    # Insert truncate, should now have original data again
    _set_batch_writer_column_data(writer, many_intervals, data)
    writer.push_truncate()

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)


def test_insert_column_truncate_explicit_range(
        qdbd_connection, table, many_intervals):

    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    # (doubles, integers, blobs, strings, timestamps) = data
    (doubles, _, _, _, _) = data

    writer = qdbd_connection.pinned_writer([table])

    # Insert once
    truncate_range = (whole_range[0],
                      whole_range[1] + np.timedelta64(1, 'ns'))

    _set_batch_writer_column_data(writer, many_intervals, data)
    writer.push()

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals)
    np.testing.assert_array_equal(results[1], doubles)

    # If we now set the same data, skip the first element, but keep
    # the same time range, the first element will *not* be present in
    # the resulting dataset.
    _set_batch_writer_column_data(writer, many_intervals, data, start=1)
    writer.push_truncate(range=truncate_range)

    # Verify results, truncating should now make things the same
    # as the beginning again.
    results = table.double_get_ranges(
        tslib._double_col_name(table), [whole_range])

    np.testing.assert_array_equal(results[0], many_intervals[1:])
    np.testing.assert_array_equal(results[1], doubles[1:])


def test_insert_column_truncate_throws_error_on_invalid_range(
        qdbd_connection, table, many_intervals):
    whole_range = (
        many_intervals[0], many_intervals[-1:][0] + np.timedelta64(2, 's'))

    # Generate our dataset
    data = _generate_data(len(many_intervals))
    # (doubles, integers, blobs, strings, timestamps) = data
    (_, _, _, _, _) = data

    # Insert truncate with explicit timerange, we point the start right after the
    # first element in our dataset. This means that the range does not overlap all
    # the data anymore.
    truncate_range = (whole_range[0] + np.timedelta64(1, 'ns'),
                      whole_range[1] + np.timedelta64(1, 'ns'))

    writer = qdbd_connection.pinned_writer([table])
    _set_batch_writer_column_data(writer, many_intervals, data)
    with pytest.raises(quasardb.Error):
        writer.push_truncate(range=truncate_range)
