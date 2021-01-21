# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import numpy as np
import quasardb
import test_table as tslib


def test_double_get_ranges__when_timeseries_is_empty(table, intervals):
    results = table.double_get_ranges(tslib._double_col_name(table), intervals)
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0


def test_double_erase_ranges__when_timeseries_is_empty(table, intervals):
    erased_count = table.erase_ranges(tslib._double_col_name(table), intervals)
    assert erased_count == 0


def test_double_get_ranges(table, intervals):
    start_time = tslib._start_time(intervals)
    column_name = tslib._double_col_name(table)

    inserted_double_data = tslib._generate_double_ts(start_time, 1000)
    table.double_insert(column_name,
                        inserted_double_data[0],
                        inserted_double_data[1])

    results = table.double_get_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    tslib._check_ts_results(results, inserted_double_data, 10)

    results = table.double_get_ranges(
        column_name,
        [
            (start_time,
             start_time + np.timedelta64(10, 's')),
            (start_time + np.timedelta64(10, 's'),
             start_time + np.timedelta64(20, 's'))])

    tslib._check_ts_results(results, inserted_double_data, 20)

    # empty result
    out_of_time = start_time + np.timedelta64(10, 'h')
    results = table.double_get_ranges(
        column_name, [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0

    # error: column doesn't exist
    with pytest.raises(quasardb.Error):
        table.double_get_ranges(
            "lolilol", [(start_time, start_time + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.Error):
        table.double_insert(
            "lolilol",
            inserted_double_data[0],
            inserted_double_data[1])

    with pytest.raises(quasardb.IncompatibleTypeError):
        table.blob_get_ranges(
            column_name, [(start_time, start_time + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.IncompatibleTypeError):
        table.blob_insert(
            column_name,
            inserted_double_data[0],
            inserted_double_data[1])


def test_double_erase_ranges(table, intervals):
    start_time = tslib._start_time(intervals)
    column_name = tslib._double_col_name(table)

    inserted_double_data = tslib._generate_double_ts(start_time, 1000)
    table.double_insert(
        column_name,
        inserted_double_data[0],
        inserted_double_data[1])

    results = table.double_get_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    erased_count = table.erase_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    assert erased_count == len(results[0])

    erased_count = table.erase_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    assert erased_count == 0

    results = table.double_get_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    assert len(results[0]) == 0
