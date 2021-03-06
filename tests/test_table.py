# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import numpy as np
import quasardb


def _generate_dates(start_time, count):
    return np.array([(start_time + np.timedelta64(i, 's'))
                     for i in range(count)]).astype('datetime64[ns]')


def _generate_double_ts(start_time, count):
    return (_generate_dates(start_time, count),
            np.random.uniform(-100.0, 100.0, count))


def _generate_int64_ts(start_time, count):
    return (_generate_dates(start_time, count),
            np.random.randint(-100, 100, count))


def _generate_timestamp_ts(start_time, start_val, count):
    return (
        _generate_dates(start_time, count),
        _generate_dates(start_val, count))


def _generate_blob_ts(start_time, count):
    dates = _generate_dates(start_time, count)
    values = np.array(
        list(
            np.random.bytes(
                np.random.randint(
                    16,
                    32)) for i in range(count)),
        dtype=np.object_)
    return (dates, values)


def _generate_string_ts(start_time, count):
    dates = _generate_dates(start_time, count)
    values = np.array([("content_" + str(item)) for item in range(count)],
                      dtype=np.unicode)

    return (dates, values)


def _generate_symbol_ts(start_time, count):
    return _generate_string_ts(start_time, count)


def _check_ts_results(results, generated, count):
    assert len(results) == 2

    np.testing.assert_array_equal(results[0][:count], generated[0][:count])
    np.testing.assert_array_equal(results[1][:count], generated[1][:count])


def test_list_columns_throws_when_timeseries_does_not_exist(
        qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)
    with pytest.raises(quasardb.Error):
        table.list_columns()


def test_insert_throws_when_timeseries_does_not_exist(
        qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)

    with pytest.raises(quasardb.Error):
        table.double_insert("the_double",
                            np.array(np.datetime64('2011-01-01', 'ns')),
                            np.array([1.0]))


def test_get_ranges_throws_when_timeseries_does_not_exist(
        qdbd_connection, entry_name, intervals):
    table = qdbd_connection.table(entry_name)
    with pytest.raises(quasardb.Error):
        table.double_get_ranges("blah", intervals)


def test_erase_ranges_throw_when_timeseries_does_not_exist(
        qdbd_connection, entry_name, intervals):
    table = qdbd_connection.table(entry_name)
    with pytest.raises(quasardb.Error):
        table.erase_ranges("blah", intervals)


def test_create_without_columns(qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)
    table.create([])

    assert len(table.list_columns()) == 0


def test_create_with_shard_size_less_than_1_millisecond_throws(
        qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)
    with pytest.raises(quasardb.Error):
        table.create([], datetime.timedelta(milliseconds=0))


def test_create_with_shard_size_of_1_millisecond(qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)
    table.create([], datetime.timedelta(milliseconds=1))
    assert len(table.list_columns()) == 0


def test_create_with_shard_size_of_1_day(qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)
    table.create([], datetime.timedelta(hours=24))
    assert len(table.list_columns()) == 0


def test_create_with_shard_size_of_4_weeks(qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)
    table.create([], datetime.timedelta(weeks=4))
    assert len(table.list_columns()) == 0


def test_create_with_shard_size_of_more_than_1_year(
        qdbd_connection, entry_name):
    table = qdbd_connection.table(entry_name)
    table.create([], datetime.timedelta(weeks=52))
    assert len(table.list_columns()) == 0


def test_table_layout(table):
    col_list = table.list_columns()
    assert len(col_list) == 6

    assert col_list[0].name == "the_double"
    assert col_list[0].type == quasardb.ColumnType.Double
    assert col_list[0].symtable == ""

    assert col_list[1].name == "the_blob"
    assert col_list[1].type == quasardb.ColumnType.Blob
    assert col_list[1].symtable == ""

    assert col_list[2].name == "the_string"
    assert col_list[2].type == quasardb.ColumnType.String
    assert col_list[2].symtable == ""

    assert col_list[3].name == "the_int64"
    assert col_list[3].type == quasardb.ColumnType.Int64
    assert col_list[3].symtable == ""

    assert col_list[4].name == "the_ts"
    assert col_list[4].type == quasardb.ColumnType.Timestamp
    assert col_list[4].symtable == ""

    assert col_list[5].name == "the_symbol"
    assert col_list[5].type == quasardb.ColumnType.Symbol
    assert col_list[5].symtable == "symtable"


def test_column_lookup(table):
    assert table.column_index_by_id("the_double") == 0
    assert table.column_index_by_id("the_blob") == 1
    assert table.column_index_by_id("the_string") == 2
    assert table.column_index_by_id("the_int64") == 3
    assert table.column_index_by_id("the_ts") == 4
    assert table.column_index_by_id("the_symbol") == 5

    with pytest.raises(quasardb.Error):
        table.column_index_by_id('foobar')

    with pytest.raises(TypeError):
        table.column_index_by_id(None)

    with pytest.raises(TypeError):
        table.column_index_by_id()


def test_cannot_double_create(table, entry_name):
    with pytest.raises(quasardb.AliasAlreadyExistsError):
        table.create([quasardb.ColumnInfo(
            quasardb.ColumnType.Double, entry_name)])


def _double_col_name(table):
    return table.list_columns()[0].name


def _blob_col_name(table):
    return table.list_columns()[1].name


def _string_col_name(table):
    return table.list_columns()[2].name


def _int64_col_name(table):
    return table.list_columns()[3].name


def _ts_col_name(table):
    return table.list_columns()[4].name


def _symbol_col_name(table):
    return table.list_columns()[5].name


def _start_time(intervals):
    return intervals[0][0]


def _start_year(intervals):
    return intervals[0][0]
