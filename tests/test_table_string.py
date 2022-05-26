# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import numpy as np
import quasardb
import test_table as tslib


def test_string_get_ranges__when_timeseries_is_empty(table, intervals):
    results = table.string_get_ranges(tslib._string_col_name(table), intervals)
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0


def test_string_erase_ranges__when_timeseries_is_empty(table, intervals):
    erased_count = table.erase_ranges(tslib._string_col_name(table), intervals)
    assert erased_count == 0


def test_string_get_ranges(table, intervals):
    start_time = tslib._start_time(intervals)
    column_name = tslib._string_col_name(table)

    inserted_string_data = tslib._generate_string_ts(start_time, 25)
    table.string_insert(column_name,
                        inserted_string_data[0],
                        inserted_string_data[1])

    results = table.string_get_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    tslib._check_ts_results(results, inserted_string_data, 10)

    results = table.string_get_ranges(column_name,
                                      [(start_time,
                                        start_time + np.timedelta64(10, 's')),
                                       (start_time + np.timedelta64(10, 's'),
                                          start_time + np.timedelta64(20, 's'))])

    tslib._check_ts_results(results, inserted_string_data, 20)

    # Everything
    results = table.string_get_ranges(column_name)
    tslib._check_ts_results(results, inserted_string_data, 25)

    # empty result
    out_of_time = start_time + np.timedelta64(10, 'h')
    results = table.string_get_ranges(
        column_name, [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0

    # error: column doesn't exist
    with pytest.raises(quasardb.Error):
        table.string_get_ranges(
            "lolilol", [(start_time, start_time + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.Error):
        table.string_insert(
            "lolilol",
            inserted_string_data[0],
            inserted_string_data[1])

    with pytest.raises(quasardb.IncompatibleTypeError):
        table.int64_get_ranges(
            column_name, [(start_time, start_time + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.IncompatibleTypeError):
        table.int64_insert(
            column_name,
            inserted_string_data[0],
            inserted_string_data[1])


def test_string_erase_ranges(table, intervals):
    start_time = tslib._start_time(intervals)
    column_name = tslib._string_col_name(table)

    inserted_string_data = tslib._generate_string_ts(start_time, 1000)
    table.string_insert(
        column_name,
        inserted_string_data[0],
        inserted_string_data[1])

    results = table.string_get_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    erased_count = table.erase_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    assert erased_count == len(results[0])

    erased_count = table.erase_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    assert erased_count == 0

    results = table.string_get_ranges(column_name, [(
        start_time, start_time + np.timedelta64(10, 's'))])

    assert len(results[0]) == 0
