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


def _check_ts_results(results, generated, count):
    assert len(results) == 2

    np.testing.assert_array_equal(results[0][:count], generated[0][:count])
    np.testing.assert_array_equal(results[1][:count], generated[1][:count])

def test_column_info_repr(column_name):
    double = quasardb.ColumnInfo(quasardb.ColumnType.Double, column_name)
    blob = quasardb.ColumnInfo(quasardb.ColumnType.Blob, column_name)
    int64 = quasardb.ColumnInfo(quasardb.ColumnType.Int64, column_name)
    timestamp = quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, column_name)
    string = quasardb.ColumnInfo(quasardb.ColumnType.String, column_name)

    assert column_name in str(double)
    assert column_name in str(blob)
    assert column_name in str(int64)
    assert column_name in str(timestamp)
    assert column_name in str(string)

    assert 'double'    in str(double)
    assert 'blob'      in str(blob)
    assert 'int64'     in str(int64)
    assert 'timestamp' in str(timestamp)
    assert 'string'    in str(string)


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
    assert len(col_list) == 5

    assert col_list[0].name == "the_double"
    assert col_list[0].type == quasardb.ColumnType.Double

    assert col_list[1].name == "the_blob"
    assert col_list[1].type == quasardb.ColumnType.Blob

    assert col_list[2].name == "the_string"
    assert col_list[2].type == quasardb.ColumnType.String

    assert col_list[3].name == "the_int64"
    assert col_list[3].type == quasardb.ColumnType.Int64

    assert col_list[4].name == "the_ts"
    assert col_list[4].type == quasardb.ColumnType.Timestamp


def test_column_lookup(table):
    assert table.column_index_by_id("the_double") == 0
    assert table.column_index_by_id("the_blob") == 1
    assert table.column_index_by_id("the_string") == 2
    assert table.column_index_by_id("the_int64") == 3
    assert table.column_index_by_id("the_ts") == 4

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


def _start_time(intervals):
    return intervals[0][0]


def _start_year(intervals):
    return intervals[0][0]


###
# Double tests
#

def test_double_get_ranges__when_timeseries_is_empty(table, intervals):
    results = table.double_get_ranges(_double_col_name(table), intervals)
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0


def test_double_erase_ranges__when_timeseries_is_empty(table, intervals):
    erased_count = table.erase_ranges(_double_col_name(table), intervals)
    assert erased_count == 0


def test_double_get_ranges(table, intervals):
    inserted_double_data = _generate_double_ts(_start_time(intervals), 1000)
    table.double_insert(_double_col_name(table),
                        inserted_double_data[0],
                        inserted_double_data[1])

    results = table.double_get_ranges(_double_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    _check_ts_results(results, inserted_double_data, 10)

    results = table.double_get_ranges(
        _double_col_name(table),
        [
            (_start_time(intervals),
             _start_time(intervals) + np.timedelta64(10, 's')),
            (_start_time(intervals) + np.timedelta64(10, 's'),
             _start_time(intervals) + np.timedelta64(20, 's'))])

    _check_ts_results(results, inserted_double_data, 20)

    # empty result
    out_of_time = _start_time(intervals) + np.timedelta64(10, 'h')
    results = table.double_get_ranges(_double_col_name(
        table), [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0

    # error: column doesn't exist
    with pytest.raises(quasardb.Error):
        table.double_get_ranges("lolilol", [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.Error):
        table.double_insert(
            "lolilol",
            inserted_double_data[0],
            inserted_double_data[1])

    with pytest.raises(TypeError):
        table.blob_get_ranges(_double_col_name, [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.IncompatibleTypeError):
        table.blob_insert(
            _double_col_name(table),
            inserted_double_data[0],
            inserted_double_data[1])


def test_double_erase_ranges(table, intervals):
    inserted_double_data = _generate_double_ts(_start_time(intervals), 1000)
    table.double_insert(
        _double_col_name(table),
        inserted_double_data[0],
        inserted_double_data[1])

    results = table.double_get_ranges(_double_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    erased_count = table.erase_ranges(_double_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert erased_count == len(results[0])

    erased_count = table.erase_ranges(_double_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert erased_count == 0

    results = table.double_get_ranges(_double_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert len(results[0]) == 0


###
# Int64 tests
#

def test_int64_get_ranges__when_timeseries_is_empty(table, intervals):
    results = table.int64_get_ranges(_int64_col_name(table), intervals)
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0


def test_int64_erase_ranges__when_timeseries_is_empty(table, intervals):
    erased_count = table.erase_ranges(_int64_col_name(table), intervals)
    assert erased_count == 0


def test_int64_get_ranges(table, intervals):
    inserted_int64_data = _generate_int64_ts(_start_time(intervals), 1000)
    table.int64_insert(_int64_col_name(table),
                       inserted_int64_data[0],
                       inserted_int64_data[1])

    results = table.int64_get_ranges(_int64_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    _check_ts_results(results, inserted_int64_data, 10)

    results = table.int64_get_ranges(_int64_col_name(table),
                                     [(_start_time(intervals),
                                       _start_time(intervals) + np.timedelta64(10, 's')),
                                      (_start_time(intervals) + np.timedelta64(10, 's'),
                                       _start_time(intervals) + np.timedelta64(20, 's'))])

    _check_ts_results(results, inserted_int64_data, 20)

    # Everything
    results = table.int64_get_ranges(_int64_col_name(table))
    _check_ts_results(results, inserted_int64_data, 1000)

    # empty result
    out_of_time = _start_time(intervals) + np.timedelta64(10, 'h')
    results = table.int64_get_ranges(_int64_col_name(
        table), [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0

    # error: column doesn't exist
    with pytest.raises(quasardb.Error):
        table.int64_get_ranges("lolilol", [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.Error):
        table.int64_insert(
            "lolilol",
            inserted_int64_data[0],
            inserted_int64_data[1])

    with pytest.raises(TypeError):
        table.blob_get_ranges(_int64_col_name, [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.IncompatibleTypeError):
        table.blob_insert(
            _int64_col_name(table),
            inserted_int64_data[0],
            inserted_int64_data[1])


def test_int64_erase_ranges(table, intervals):
    inserted_int64_data = _generate_int64_ts(_start_time(intervals), 1000)
    table.int64_insert(
        _int64_col_name(table),
        inserted_int64_data[0],
        inserted_int64_data[1])

    results = table.int64_get_ranges(_int64_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    erased_count = table.erase_ranges(_int64_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert erased_count == len(results[0])

    erased_count = table.erase_ranges(_int64_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert erased_count == 0

    results = table.int64_get_ranges(_int64_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert len(results[0]) == 0


###
# Blob tests
#

def test_blob_get_ranges__when_timeseries_is_empty(table, intervals):
    results = table.blob_get_ranges(_blob_col_name(table), intervals)
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0


def test_blob_erase_ranges__when_timeseries_is_empty(table, intervals):
    erased_count = table.erase_ranges(_blob_col_name(table), intervals)
    assert erased_count == 0


def test_blob_get_ranges(table, intervals):
    inserted_blob_data = _generate_blob_ts(_start_time(intervals), 1000)
    table.blob_insert(_blob_col_name(table),
                      inserted_blob_data[0],
                      inserted_blob_data[1])

    results = table.blob_get_ranges(_blob_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    _check_ts_results(results, inserted_blob_data, 10)

    results = table.blob_get_ranges(_blob_col_name(table),
                                    [(_start_time(intervals),
                                      _start_time(intervals) + np.timedelta64(10, 's')),
                                     (_start_time(intervals) + np.timedelta64(10, 's'),
                                      _start_time(intervals) + np.timedelta64(20, 's'))])

    _check_ts_results(results, inserted_blob_data, 20)

    # Everything
    results = table.blob_get_ranges(_blob_col_name(table))
    _check_ts_results(results, inserted_blob_data, 1000)

    # empty result
    out_of_time = _start_time(intervals) + np.timedelta64(10, 'h')
    results = table.blob_get_ranges(_blob_col_name(
        table), [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0

    # error: column doesn't exist
    with pytest.raises(quasardb.Error):
        table.blob_get_ranges("lolilol", [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.Error):
        table.blob_insert(
            "lolilol",
            inserted_blob_data[0],
            inserted_blob_data[1])

    with pytest.raises(TypeError):
        table.int64_get_ranges(_blob_col_name, [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(TypeError):
        table.int64_insert(
            _blob_col_name(table),
            inserted_blob_data[0],
            inserted_blob_data[1])


def test_blob_erase_ranges(table, intervals):
    inserted_blob_data = _generate_blob_ts(_start_time(intervals), 1000)
    table.blob_insert(
        _blob_col_name(table),
        inserted_blob_data[0],
        inserted_blob_data[1])

    results = table.blob_get_ranges(_blob_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    erased_count = table.erase_ranges(_blob_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert erased_count == len(results[0])

    erased_count = table.erase_ranges(_blob_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert erased_count == 0

    results = table.blob_get_ranges(_blob_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    assert len(results[0]) == 0

###
# Blob tests
#


def test_string_get_ranges__when_timeseries_is_empty(table, intervals):
    results = table.string_get_ranges(_blob_col_name(table), intervals)
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0


def test_string_erase_ranges__when_timeseries_is_empty(table, intervals):
    erased_count = table.erase_ranges(_string_col_name(table), intervals)
    assert erased_count == 0


def test_string_get_ranges(table, intervals):
    inserted_string_data = _generate_string_ts(_start_time(intervals), 25)
    table.string_insert(_string_col_name(table),
                        inserted_string_data[0],
                        inserted_string_data[1])

    results = table.string_get_ranges(_string_col_name(table), [(
        _start_time(intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    _check_ts_results(results, inserted_string_data, 10)

    results = table.string_get_ranges(
        _string_col_name(table),
        [
            (_start_time(intervals),
             _start_time(intervals) +
             np.timedelta64(
                10,
                's')),
            (_start_time(intervals) +
             np.timedelta64(
                10,
                's'),
                _start_time(intervals) +
                np.timedelta64(
                20,
                's'))])

    _check_ts_results(results, inserted_string_data, 20)

    # Everything
    results = table.string_get_ranges(_string_col_name(table))
    _check_ts_results(results, inserted_string_data, 25)

    # empty result
    out_of_time = _start_time(intervals) + np.timedelta64(10, 'h')
    results = table.string_get_ranges(_string_col_name(
        table), [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
    assert len(results) == 2
    assert len(results[0]) == 0
    assert len(results[1]) == 0

    # error: column doesn't exist
    with pytest.raises(quasardb.Error):
        table.string_get_ranges("lolilol", [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(quasardb.Error):
        table.string_insert(
            "lolilol",
            inserted_string_data[0],
            inserted_string_data[1])

    with pytest.raises(TypeError):
        table.int64_get_ranges(_string_col_name, [(_start_time(
            intervals), _start_time(intervals) + np.timedelta64(10, 's'))])

    with pytest.raises(TypeError):
        table.int64_insert(
            _string_col_name(table),
            inserted_string_data[0],
            inserted_string_data[1])
