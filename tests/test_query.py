# # pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
import numpy as np

import test_ts as tslib


def _insert_double_points(table, start_time, points=10):
    inserted_double_data = tslib._generate_double_ts(start_time, points)
    table.double_insert(tslib._double_col_name(table),
                        inserted_double_data[0],
                        inserted_double_data[1])
    return inserted_double_data


def _insert_blob_points(table, start_time, points=10):
    inserted_blob_data = tslib._generate_blob_ts(start_time, points)
    table.blob_insert(tslib._blob_col_name(table),
                      inserted_blob_data[0],
                      inserted_blob_data[1])
    return inserted_blob_data


def _insert_int64_points(table, start_time, points=10):
    inserted_int64_data = tslib._generate_int64_ts(start_time, points)
    table.int64_insert(tslib._int64_col_name(table),
                       inserted_int64_data[0],
                       inserted_int64_data[1])
    return inserted_int64_data


def _insert_timestamp_points(table, start_time, points=10):
    inserted_timestamp_data = tslib._generate_timestamp_ts(
        start_time, start_time, points)
    table.timestamp_insert(tslib._ts_col_name(table),
                           inserted_timestamp_data[0],
                           inserted_timestamp_data[1])
    return inserted_timestamp_data

##
# Query failure tests


def test_returns_invalid_argument_for_null_query(qdbd_connection):
    with pytest.raises(TypeError):
        qdbd_connection.query()


def test_returns_invalid_argument_for_empty_query(qdbd_connection):
    q = qdbd_connection.query('')
    with pytest.raises(quasardb.Error):
        q.run()


def test_returns_invalid_argument_with_invalid_query(qdbd_connection):
    q = qdbd_connection.query('select * from')
    with pytest.raises(quasardb.Error):
        q.run()


def test_returns_alias_not_found_when_ts_doesnt_exist(qdbd_connection):
    q = qdbd_connection.query(
        'select * from ' + 'this_ts_doesnt_exist' + ' in range(2017, +10d)')
    with pytest.raises(quasardb.Error):
        q.run()


def test_returns_alias_not_found_when_untagged(qdbd_connection, tag_name):
    q = qdbd_connection.query(
        "select * from find(tag='" + tag_name + "') in range(2017, +10d)")
    with pytest.raises(quasardb.Error):
        q.run()


def test_returns_columns_not_found(qdbd_connection, table, column_name):
    q = qdbd_connection.query(
        "select " +
        column_name +
        " from " +
        table.get_name() +
        " in range(2017, +10d)")
    with pytest.raises(quasardb.Error):
        q.run()


##
# Double data tests

def sanity_check(ts_name, scanned_point_count, res, rows_count, columns_count):
    assert res.scanned_point_count == scanned_point_count
    assert len(res.tables) == 1
    assert len(res.tables[ts_name][0].data) == rows_count
    assert len(res.tables[ts_name]) == columns_count
    assert res.tables[ts_name][0].name == "timestamp"


def test_returns_empty_result(qdbd_connection, table):
    res = qdbd_connection.query(
        "select * from " +
        table.get_name() +
        " in range(2016-01-01 , 2016-12-12)").run()
    assert res.scanned_point_count == 0
    assert len(res.tables) == 0


def test_returns_inserted_data_with_star_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select * from " + table.get_name() + \
        " in range(" + str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()

    # Column count is 5, because, uninit, int64, blob, timestamp, double
    sanity_check(table.get_name(),
                 len(inserted_double_data[0]),
                 res,
                 len(inserted_double_data[0]),
                 5)

    assert res.tables[table.get_name()][0].name == "timestamp"
    np.testing.assert_array_equal(
        res.tables[table.get_name()][0].data, inserted_double_data[0])
    assert res.tables[table.get_name(
    )][1].name == tslib._double_col_name(table)
    np.testing.assert_array_equal(
        res.tables[table.get_name()][1].data, inserted_double_data[1])


def test_returns_inserted_data_with_star_select_and_tag_lookup(
        qdbd_connection, table, tag_name, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    table.attach_tag(tag_name)
    query = "select * from find(tag = " + '"' + tag_name + '")' + \
        " in range(" + str(tslib._start_year(intervals)) + ", +100d)"

    res = qdbd_connection.query(query).run()
    sanity_check(table.get_name(),
                 len(inserted_double_data[0]),
                 res,
                 len(inserted_double_data[0]),
                 5)

    assert res.tables[table.get_name()][0].name == "timestamp"
    np.testing.assert_array_equal(
        res.tables[table.get_name()][0].data, inserted_double_data[0])

    assert res.tables[table.get_name(
    )][1].name == tslib._double_col_name(table)
    np.testing.assert_array_equal(
        res.tables[table.get_name()][1].data, inserted_double_data[1])


def test_returns_inserted_data_with_column_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select " + tslib._double_col_name(table) + " from " + table.get_name(
    ) + " in range(" + str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()

    sanity_check(table.get_name(),
                 len(inserted_double_data[0]),
                 res,
                 len(inserted_double_data[0]),
                 2)

    assert res.tables[table.get_name()][0].name == "timestamp"
    np.testing.assert_array_equal(
        res.tables[table.get_name()][0].data, inserted_double_data[0])
    assert res.tables[table.get_name(
    )][1].name == tslib._double_col_name(table)
    np.testing.assert_array_equal(
        res.tables[table.get_name()][1].data, inserted_double_data[1])


def test_returns_inserted_data_twice_with_double_column_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select " + tslib._double_col_name(table) + "," + tslib._double_col_name(table) + \
        " from " + table.get_name() + \
        " in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()
    sanity_check(table.get_name(), len(inserted_double_data[0]),
                 res, len(inserted_double_data[0]), 2)

    assert res.tables[table.get_name()][0].name == "timestamp"
    np.testing.assert_array_equal(
        res.tables[table.get_name()][0].data, inserted_double_data[0])
    assert res.tables[table.get_name(
    )][1].name == tslib._double_col_name(table)
    np.testing.assert_array_equal(
        res.tables[table.get_name()][1].data, inserted_double_data[1])


def test_returns_sum_with_sum_select(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select sum(" + tslib._double_col_name(table) + ") from " + table.get_name() + \
        " in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()
    sanity_check(table.get_name(), len(inserted_double_data[0]), res, 1, 2)

    res_table = res.tables[table.get_name()]
    assert res_table[0].name == "timestamp"
    assert np.isnat(res_table[0].data[0])
    assert res_table[1].name == "sum(" + tslib._double_col_name(table) + ")"
    assert pytest.approx(res_table[1].data[0], 0.1) == np.sum(
        inserted_double_data[1])


def test_returns_sum_with_sum_divided_by_count_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select sum(" + tslib._double_col_name(table) + ")/count(" + \
        tslib._double_col_name(table) + ") from " + table.get_name() + \
        " in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()
    sanity_check(table.get_name(), len(inserted_double_data[0]) * 2, res, 1, 2)

    res_table = res.tables[table.get_name()]
    assert res_table[0].name == "timestamp"
    assert np.isnat(res_table[0].data[0])
    assert res_table[1].name == "(sum(" + tslib._double_col_name(
        table) + ")/count(" + tslib._double_col_name(table) + "))"
    assert pytest.approx(res_table[1].data[0], 0.1) == np.average(
        inserted_double_data[1])


def test_returns_max_minus_min_select(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select max(" + tslib._double_col_name(table) + ") - min(" + \
        tslib._double_col_name(table) + ") from " + table.get_name() + \
        " in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()

    sanity_check(table.get_name(), len(inserted_double_data[0]) * 2, res, 1, 2)

    res_table = res.tables[table.get_name()]
    assert res_table[0].name == "timestamp"
    assert np.isnat(res_table[0].data[0])
    assert res_table[1].name == "(max(" + tslib._double_col_name(
        table) + ")-min(" + tslib._double_col_name(table) + "))"
    assert pytest.approx(res_table[1].data[0], 0.1) == np.max(
        inserted_double_data[1]) - np.min(inserted_double_data[1])


def test_returns_max_minus_1_select(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select max(" + tslib._double_col_name(table) + ") - 1 from " + \
        table.get_name() + \
        " in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()

    sanity_check(table.get_name(), len(inserted_double_data[0]), res, 1, 2)

    res_table = res.tables[table.get_name()]
    assert res_table[0].name == "timestamp"
    assert res_table[0].data[0] >= start_time
    assert res_table[1].name == "(max(" + \
        tslib._double_col_name(table) + ")-1)"
    assert pytest.approx(res_table[1].data[0], 0.1) == np.max(
        inserted_double_data[1]) - 1


def test_returns_max_and_scalar_1_select(qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select max(" + tslib._double_col_name(table) + "), 1 from " + \
        table.get_name() + \
        " in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()

    assert len(res.tables) == 2

    res_table = res.tables["$none"]
    assert res_table[0].name == "timestamp"
    assert np.isnat(res_table[0].data[0])
    assert res_table[1].name == "max(" + tslib._double_col_name(table) + ")"
    assert res_table[2].name == "1"
    assert res_table[2].data[0] == 1

    res_table = res.tables[table.get_name()]
    assert res_table[0].name == "timestamp"
    assert res_table[0].data[0] >= start_time
    assert res_table[1].name == "max(" + tslib._double_col_name(table) + ")"
    assert pytest.approx(res_table[1].data[0], 0.1) == np.max(
        inserted_double_data[1])
    assert res_table[2].name == "1"


def test_returns_inserted_multi_data_with_star_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    inserted_blob_data = _insert_blob_points(table, start_time, 10)
    inserted_int64_data = _insert_int64_points(table, start_time, 10)
    inserted_timestamp_data = _insert_timestamp_points(table, start_time, 10)

    query = "select * from " + table.get_name() + \
        " in range(" + str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query).run()

    # Column count is 5, because, uninit, int64, blob, timestamp, double
    sanity_check(table.get_name(), 4 * len(inserted_double_data[0]),
                 res, len(inserted_double_data[0]), 5)

    assert res.tables[table.get_name()][0].name == "timestamp"
    assert res.tables[table.get_name(
    )][1].name == tslib._double_col_name(table)
    assert res.tables[table.get_name()][2].name == tslib._blob_col_name(table)
    assert res.tables[table.get_name()][3].name == tslib._int64_col_name(table)
    assert res.tables[table.get_name()][4].name == tslib._ts_col_name(table)

    np.testing.assert_array_equal(
        res.tables[table.get_name()][0].data, inserted_double_data[0])
    np.testing.assert_array_equal(
        res.tables[table.get_name()][1].data, inserted_double_data[1])
    np.testing.assert_array_equal(
        res.tables[table.get_name()][2].data, inserted_blob_data[1])
    np.testing.assert_array_equal(
        res.tables[table.get_name()][3].data, inserted_int64_data[1])
    np.testing.assert_array_equal(
        res.tables[table.get_name()][4].data, inserted_timestamp_data[1])
