# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange, int as long  # pylint: disable=W0622
from functools import reduce  # pylint: disable=W0622
import datetime
import test_table as tslib
from time import sleep

import pytest
import quasardb
import numpy as np

def _make_inserter_info(table):
    return [quasardb.BatchColumnInfo(table.get_name(), tslib._int64_col_name(table), 100)]


def _generate_data(count, start=np.datetime64('2017-01-01', 'ns')):
    integers = np.random.randint(-100, 100, count)
    timestamps = tslib._generate_dates(
        start + np.timedelta64('1', 'D'), count)

    return (integers, timestamps)

def test_non_existing_pinned_insert(qdbd_connection, entry_name):
    with pytest.raises(quasardb.Error):
        qdbd_connection.pinned_inserter(
            [quasardb.BatchColumnInfo(entry_name, "col", 10)])

def test_successful_bulk_row_insert(qdbd_connection, table, many_intervals):
    pinned_inserter = qdbd_connection.pinned_inserter(_make_inserter_info(table))

    pinned_inserter.start_row(np.datetime64('2020-01-02', 'ns'))
    pinned_inserter.set_value(0, 2)
    pinned_inserter.start_row(np.datetime64('2020-01-01', 'ns'))
    pinned_inserter.set_value(0, 1)

    pinned_inserter.push()
    
    res = qdbd_connection.query('select * from ' + table.get_name())
    print("res: {}".format(res))
