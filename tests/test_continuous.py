# pylint: disable=missing-module-docstring,missing-function-docstring,not-an-iterable,invalid-name
import pytest
import quasardb
import numpy as np
import test_table as tslib
import time
import datetime


def _insert_double_points(table, start_time, points=10):
    inserted_double_data = tslib._generate_double_ts(start_time, points)
    table.double_insert(
        tslib._double_col_name(table), inserted_double_data[0], inserted_double_data[1]
    )
    return inserted_double_data


def test_returns_invalid_argument_for_null_query_full(qdbd_connection):
    with pytest.raises(TypeError):
        qdbd_connection.query_continuous_full()


def test_returns_invalid_argument_for_null_query_full(qdbd_connection):
    with pytest.raises(TypeError):
        qdbd_connection.query_continuous_new_values()


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_empty_result_full(qdbd_connection, table):
    # return empty result
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_full(
        q, datetime.timedelta(milliseconds=100)
    )
    res = cont.results()
    assert len(res) == 0


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_empty_result_new_values(qdbd_connection, table):
    # return empty result
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_new_values(
        q, datetime.timedelta(milliseconds=100)
    )
    res = cont.results()
    assert len(res) == 0


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_empty_result_new_values_probe(qdbd_connection, table):
    # return empty result
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_new_values(
        q, datetime.timedelta(milliseconds=100)
    )
    res = cont.probe_results()
    assert len(res) == 0


def _test_against_table(res, table, data):
    for row, v in zip(res, data):
        assert "$timestamp" in row

        assert row["the_blob"] == None
        assert row["the_int64"] == None
        assert row["the_ts"] == None
        assert row["$table"] == table.get_name()
        assert row["the_double"] == v


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_full(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_full(
        q, datetime.timedelta(milliseconds=100)
    )
    res = cont.results()
    assert len(res) == 1
    _test_against_table(res, table, inserted_double_data[1])


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_full_probe(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_full(
        q, datetime.timedelta(milliseconds=100)
    )
    res = []
    while len(res) == 0:
        res = cont.probe_results()
    assert len(res) == 1
    _test_against_table(res, table, inserted_double_data[1])


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_full_iterator(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_full(
        q, datetime.timedelta(milliseconds=100)
    )
    for res in cont:
        assert len(res) == 1
        _test_against_table(res, table, inserted_double_data[1])
        break


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_new_values(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_new_values(
        q, datetime.timedelta(milliseconds=100)
    )
    res = cont.results()
    assert len(res) == 1
    _test_against_table(res, table, inserted_double_data[1])


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_new_values_probe(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_new_values(
        q, datetime.timedelta(milliseconds=100)
    )
    res = []
    while len(res) == 0:
        res = cont.probe_results()
    assert len(res) == 1
    _test_against_table(res, table, inserted_double_data[1])


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_new_value_iterator(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_new_values(
        q, datetime.timedelta(milliseconds=100)
    )
    for res in cont:
        assert len(res) == 1
        _test_against_table(res, table, inserted_double_data[1])
        break


def __wait_for(cont, f, max_ticks=10):
    i = 0

    for res in cont:

        v = f(res)
        if v:
            return v

        i += 1

        # we must not wait for the value too long
        assert i < max_ticks


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_full_value_iterator_multiple(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_full(
        q, datetime.timedelta(milliseconds=100)
    )

    assert True is __wait_for(cont, lambda x: len(x) == 1)

    start_time += np.timedelta64(1, "m")
    inserted_double_data = _insert_double_points(table, start_time, 1)

    assert True is __wait_for(cont, lambda x: len(x) == 2)


@pytest.mark.skip(reason="Flaky test -- tracked in QDB-14766 in shortcut")
def test_returns_rows_new_value_iterator_multiple(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 1)
    q = 'select * from "' + table.get_name() + '"'
    cont = qdbd_connection.query_continuous_new_values(
        q, datetime.timedelta(milliseconds=100)
    )

    assert True is __wait_for(cont, lambda x: len(x) == 1)

    start_time += np.timedelta64(1, "m")
    inserted_double_data = _insert_double_points(table, start_time, 1)

    assert True is __wait_for(cont, lambda x: len(x) == 1)
