# # pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
import numpy as np

import test_table as tslib


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
    with pytest.raises(quasardb.Error):
        qdbd_connection.query('')


def test_returns_invalid_argument_with_invalid_query(qdbd_connection):
    with pytest.raises(quasardb.Error):
        qdbd_connection.query('select * from')


def test_returns_alias_not_found_when_ts_doesnt_exist(qdbd_connection):
    with pytest.raises(quasardb.Error):
        qdbd_connection.query(
            'select * from ' +
            'this_ts_doesnt_exist' +
            ' in range(2017, +10d)')


def test_returns_alias_not_found_when_untagged(qdbd_connection, tag_name):
    with pytest.raises(quasardb.Error):
        qdbd_connection.query(
            "select * from find(tag='" + tag_name + "') in range(2017, +10d)")


def test_returns_columns_not_found(qdbd_connection, table, column_name):
    with pytest.raises(quasardb.Error):
        qdbd_connection.query(
            "select " +
            column_name +
            " from " +
            table.get_name() +
            " in range(2017, +10d)")

def test_invalid_query_message(qdbd_connection, table):
    with pytest.raises(quasardb.Error) as excinfo:
        qdbd_connection.query(
            "select asd_col from \"" +
            table.get_name() +
            "\" in range(2016-01-01 , 2016-12-12)")
    assert str(excinfo.value) == 'The timeseries does not contain this column. Could not find column \'asd_col\'.'


##
# Double data tests

def sanity_check(ts_name, scanned_point_count, res, rows_count, columns_count):
    assert res.scanned_point_count == scanned_point_count
    assert len(res.tables) == 1
    assert len(res.tables[ts_name][0].data) == rows_count
    assert len(res.tables[ts_name]) == columns_count
    assert res.tables[ts_name][0].name == "$timestamp"


def test_returns_empty_result(qdbd_connection, table):
    res = qdbd_connection.query(
        "select * from \"" +
        table.get_name() +
        "\" in range(2016-01-01 , 2016-12-12)")
    assert len(res) == 0


def test_returns_table_as_string(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select * from \"" + table.get_name() + "\" in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query)

    assert len(res) == 10

    for row, _ in zip(res, inserted_double_data[1]):
        assert row['$table'] == table.get_name()


def test_returns_table_as_blob(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select * from \"" + table.get_name() + "\" in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query, blobs=['$table'])

    assert len(res) == 10

    for row, _ in zip(res, inserted_double_data[1]):
        assert row['$table'] == table.get_name()


def test_returns_inserted_data_with_star_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select * from \"" + table.get_name() + "\" in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query)

    assert len(res) == 10

    for row, v in zip(res, inserted_double_data[1]):
        assert '$timestamp' in row

        assert row['the_blob'] is None
        assert row['the_int64'] is None
        assert row['the_ts'] is None
        assert row['$table'] == table.get_name()
        assert row['the_double'] == v


def test_returns_inserted_data_with_column_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select " + tslib._double_col_name(table) + \
        " from \"" + table.get_name() + "\" in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query)

    assert len(res) == 10

    for row, v in zip(res, inserted_double_data[1]):
        assert '$table' not in row
        assert '$timestamp' not in row
        assert 'the_blob' not in row
        assert 'the_int64' not in row
        assert 'the_ts' not in row

        assert row['the_double'] == v


def test_returns_inserted_data_with_specific_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select $timestamp, $table, " + tslib._double_col_name(table) + \
        " from \"" + table.get_name() + "\" in range(" + \
        str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query)

    assert len(res) == 10

    for row, v in zip(res, inserted_double_data[1]):
        assert '$timestamp' in row
        assert 'the_blob' not in row
        assert 'the_int64' not in row
        assert 'the_ts' not in row

        assert row['the_double'] == v


def test_returns_count_data_with_count_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    _ = _insert_double_points(table, start_time, 10)
    query = "select count(" + tslib._double_col_name(table) + ") from \"" + table.get_name() + \
        "\" in range(" + str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query)

    assert len(res) == 1
    assert res[0]['count(the_double)'] == 10


def test_returns_count_data_with_sum_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 10)
    query = "select sum(" + tslib._double_col_name(table) + ") from \"" + table.get_name() + \
        "\" in range(" + str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query)

    assert len(res) == 1
    assert pytest.approx(res[0]['sum(the_double)'],
                         np.sum(inserted_double_data[1]))


def test_returns_inserted_multi_data_with_star_select(
        qdbd_connection, table, intervals):
    start_time = tslib._start_time(intervals)
    inserted_double_data = _insert_double_points(table, start_time, 100)
    inserted_blob_data = _insert_blob_points(table, start_time, 100)
    inserted_int64_data = _insert_int64_points(table, start_time, 100)
    inserted_timestamp_data = _insert_timestamp_points(table, start_time, 100)

    query = "select * from \"" + table.get_name() + \
        "\" in range(" + str(tslib._start_year(intervals)) + ", +100d)"
    res = qdbd_connection.query(query, blobs=['the_blob'])

    assert len(res) == 100

    print("blobs: ", type(inserted_blob_data[1][0]))

    for row, double, blob, int64, ts in zip(res,
                                            inserted_double_data[1],
                                            inserted_blob_data[1],
                                            inserted_int64_data[1],
                                            inserted_timestamp_data[1]):
        assert '$timestamp' in row

        # Note that this is a string
        assert row['$table'] == table.get_name()

        # And this is a blob
        assert row['the_blob'] == blob

        assert row['the_double'] == double
        assert row['the_int64'] == int64
        assert row['the_ts'] == ts


def test_create_table(qdbd_connection, entry_name):
    query = "create table \"{}\" (col int64)".format(entry_name)
    _ = qdbd_connection.query(query)
