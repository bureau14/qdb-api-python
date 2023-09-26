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

def test_incorrect_type_double(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(6):
        if idx == 0:
            continue
        with pytest.raises(quasardb.IncompatibleTypeError):
            pinned_writer.set_double(idx, 1.1)


def test_successful_type_double(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = 1.1

    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, timestamp)
    pinned_writer.set_double(0, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_double" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_double'] == value


def test_incorrect_type_blob(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, np.datetime64('2020-01-01T00:00:00', 'ns'))
    with pytest.raises(quasardb.IncompatibleTypeError):
        pinned_writer.set_int64(1, 1234)

def test_successful_type_blob(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = b'aaa'

    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, timestamp)
    pinned_writer.set_blob(1, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_blob" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_blob'] == value


def test_incorrect_type_string(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, np.datetime64('2020-01-01T00:00:00', 'ns'))
    with pytest.raises(quasardb.IncompatibleTypeError):
        pinned_writer.set_int64(2, 1234)

def test_successful_type_string(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = 'aaa'

    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, timestamp)
    pinned_writer.set_string(2, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_string" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_string'] == value


def test_incorrect_type_int64(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(6):
        if idx == 3:
            continue
        with pytest.raises(quasardb.IncompatibleTypeError):
            pinned_writer.set_int64(idx, 1)


def test_successful_type_int64(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = 1

    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, timestamp)
    pinned_writer.set_int64(3, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_int64'] == value


def test_incorrect_type_timestamp(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, np.datetime64('2020-01-01T00:00:00', 'ns'))
    for idx in range(6):
        if idx == 4:
            continue
        with pytest.raises(quasardb.IncompatibleTypeError):
            pinned_writer.set_timestamp(
                idx, np.datetime64(
                    '2020-01-01T00:00:00', 'ns'))


def test_successful_type_timestamp(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = np.datetime64('2020-01-01T00:00:00', 'ns')

    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, timestamp)
    pinned_writer.set_timestamp(4, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_ts" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_ts'] == value


def test_incorrect_type_symbol(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, np.datetime64('2020-01-01T00:00:00', 'ns'))
    with pytest.raises(quasardb.IncompatibleTypeError):
        pinned_writer.set_int64(5, 1234)


def test_successful_type_symbol(qdbd_connection, table):
    timestamp = np.datetime64('2020-01-01T00:00:00', 'ns')
    value = 'aaa'

    pinned_writer = qdbd_connection.pinned_writer()
    pinned_writer.start_row(table, timestamp)
    pinned_writer.set_string(5, value)
    pinned_writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_symbol" FROM "{}"'.format(
            table.get_name()))
    assert len(res) == 1
    assert res[0]['$timestamp'] == timestamp
    assert res[0]['the_symbol'] == value


def test_insert_data_ordered_single_shard(qdbd_connection, table):
    pinned_writer = qdbd_connection.pinned_writer()

    timestamps = [
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns'),
        np.datetime64(
            '2020-01-01T00:00:01',
            'ns')]
    values = [0, 1]

    pinned_writer.start_row(table, timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(table, timestamps[1])
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
    pinned_writer = qdbd_connection.pinned_writer()

    timestamps = [
        np.datetime64(
            '2020-01-01',
            'ns'),
        np.datetime64(
            '2020-01-02',
            'ns')]
    values = [1, 2]

    pinned_writer.start_row(table, timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(table, timestamps[1])
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
    pinned_writer = qdbd_connection.pinned_writer()

    timestamps = [
        np.datetime64(
            '2020-01-01T00:00:01',
            'ns'),
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns')]
    values = [1, 0]

    pinned_writer.start_row(table, timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(table, timestamps[1])
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
    pinned_writer = qdbd_connection.pinned_writer()

    timestamps = [
        np.datetime64(
            '2020-01-02',
            'ns'),
        np.datetime64(
            '2020-01-01',
            'ns')]
    values = [2, 1]

    pinned_writer.start_row(table, timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(table, timestamps[1])
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
    pinned_writer = qdbd_connection.pinned_writer()

    timestamps = [
        np.datetime64(
            '2020-01-01T00:00:00',
            'ns'),
        np.datetime64(
            '2020-01-01T00:00:01',
            'ns')]
    values = [0, 1]

    pinned_writer.start_row(table, timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.push()

    pinned_writer.start_row(table, timestamps[1])
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
    pinned_writer = qdbd_connection.pinned_writer()

    timestamps = [
        np.datetime64(
            '2020-01-01',
            'ns'),
        np.datetime64(
            '2020-01-02',
            'ns')]
    values = [1, 2]

    pinned_writer.start_row(table, timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.push()

    pinned_writer.start_row(table, timestamps[1])
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
        table,
        dates,
        doubles,
        blobs,
        strings,
        integers,
        timestamps,
        symbols):
    for i in range(len(dates)):
        writer.start_row(table, dates[i])
        writer.set_double(0, doubles[i])
        writer.set_blob(1, blobs[i])
        writer.set_string(2, strings[i])
        writer.set_int64(3, integers[i])
        writer.set_timestamp(4, timestamps[i])
        writer.set_string(5, symbols[i])


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
    symbols = np.array([("sym_" + str(item)) for item in range(count)])

    return (doubles, integers, blobs, strings, timestamps, symbols)


def _set_batch_writer_data(writer, table, intervals, data, start=0):
    (doubles, integers, blobs, strings, timestamps, symbols) = data

    for i in range(start, len(intervals)):
        writer.start_row(table, intervals[i])
        writer.set_double(0, doubles[i])
        writer.set_blob(1, blobs[i])
        writer.set_string(2, strings[i])
        writer.set_int64(3, integers[i])
        writer.set_timestamp(4, timestamps[i])
        writer.set_string(5, symbols[i])


def _assert_results(table, intervals, data):
    (doubles, integers, blobs, strings, timestamps, symbols) = data

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

    results = table.string_get_ranges(
        tslib._symbol_col_name(table), [whole_range])
    np.testing.assert_array_equal(results[0], intervals)
    np.testing.assert_array_equal(results[1], symbols)
