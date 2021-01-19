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

def test_successful_bulk_row_insert_data_ordered_single_shard(qdbd_connection, table, many_intervals):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [np.datetime64('2020-01-01T00:00:00', 'ns'), np.datetime64('2020-01-01T00:00:01', 'ns')]
    values = [0,1]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()
    
    res = qdbd_connection.query('SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name()))
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row['$timestamp'] == timestamps[idx]
        assert row['the_int64'] == values[idx]

def test_successful_bulk_row_insert_data_ordered_multiple_shards(qdbd_connection, table, many_intervals):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [np.datetime64('2020-01-01', 'ns'), np.datetime64('2020-01-02', 'ns')]
    values = [0,1]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()
    
    res = qdbd_connection.query('SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name()))
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row['$timestamp'] == timestamps[idx]
        assert row['the_int64'] == values[idx]

def test_successful_bulk_row_insert_data_unordered_single_shard(qdbd_connection, table, many_intervals):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [np.datetime64('2020-01-01T00:00:01', 'ns'), np.datetime64('2020-01-01T00:00:00', 'ns')]
    values = [1,0]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()
    
    res = qdbd_connection.query('SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name()))
    
    assert res[0]['$timestamp'] == timestamps[1]
    assert res[0]['the_int64'] == values[1]
    assert res[1]['$timestamp'] == timestamps[0]
    assert res[1]['the_int64'] == values[0]

def test_successful_bulk_row_insert_data_unordered_multiple_shards(qdbd_connection, table, many_intervals):
    pinned_writer = qdbd_connection.pinned_writer([table])

    timestamps = [np.datetime64('2020-01-02', 'ns'), np.datetime64('2020-01-01', 'ns')]
    values = [2,1]

    pinned_writer.start_row(timestamps[0])
    pinned_writer.set_int64(3, values[0])
    pinned_writer.start_row(timestamps[1])
    pinned_writer.set_int64(3, values[1])

    pinned_writer.push()
    
    res = qdbd_connection.query('SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name()))

    assert res[0]['$timestamp'] == timestamps[1]
    assert res[0]['the_int64'] == values[1]
    assert res[1]['$timestamp'] == timestamps[0]
    assert res[1]['the_int64'] == values[0]