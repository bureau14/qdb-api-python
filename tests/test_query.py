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

def _insert_string_points(table, start_time, points=10):
    xs = tslib._generate_string_ts(start_time, points)
    table.string_insert(tslib._string_col_name(table),
                        xs[0],
                        xs[1])
    return xs


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


def _insert_symbol_points(table, start_time, points=10):
    inserted_symbol_data = tslib._generate_symbol_ts(
        start_time, points)
    table.symbol_insert(tslib._ts_col_name(table),
                        inserted_symbol_data[0],
                        inserted_symbol_data[1])
    return inserted_symbol_data

point_inserter_by_type = {'double': _insert_double_points,
                          'blob': _insert_blob_points,
                          'string': _insert_string_points,
                          'int64': _insert_int64_points,
                          'timestamp': _insert_timestamp_points,
                          'symbol': _insert_symbol_points}

def _insert_points(value_type, table, start_time=None, intervals=None, points=10):
    if start_time is None:
        assert intervals is not None
        start_time = tslib._start_time(intervals)

    assert start_time is not None

    fn = point_inserter_by_type[value_type]
    return fn(table, start_time, points)


def _column_name(table, value_type):
    value_type_to_fn = {'double': tslib._double_col_name,
                        'int64': tslib._int64_col_name,
                        'blob': tslib._blob_col_name,
                        'string': tslib._string_col_name,
                        'timestamp': tslib._ts_col_name,
                        'symbol': tslib._symbol_col_name}

    fn = value_type_to_fn[value_type]
    return fn(table)


##
# Query failure tests


def test_returns_invalid_argument_for_null_query(qdbd_connection):
    with pytest.raises(TypeError):
        qdbd_connection.query()


def test_returns_invalid_argument_for_empty_query(qdbd_connection):
    with pytest.raises(quasardb.InvalidQueryError):
        qdbd_connection.query('')


def test_returns_invalid_argument_with_invalid_query(qdbd_connection):
    with pytest.raises(quasardb.InvalidQueryError):
        qdbd_connection.query('select * from')


def test_returns_alias_not_found_when_ts_doesnt_exist(qdbd_connection):
    with pytest.raises(quasardb.AliasNotFoundError):
        qdbd_connection.query(
            'select * from ' +
            'this_ts_doesnt_exist' +
            ' in range(2017, +10d)')


def test_returns_alias_not_found_when_untagged(qdbd_connection, tag_name):
    with pytest.raises(quasardb.AliasNotFoundError):
        qdbd_connection.query(
            "select * from find(tag='" + tag_name + "') in range(2017, +10d)")


def test_returns_columns_not_found(qdbd_connection, table, column_name):
    with pytest.raises(quasardb.Error):
        qdbd_connection.query(
            "select " +
            column_name +
            " from \"" +
            table.get_name() +
            "\" in range(2017, +10d)")

def test_invalid_query_message(qdbd_connection, table):
    with pytest.raises(quasardb.Error) as excinfo:
        qdbd_connection.query(
            "select asd_col from \"" +
            table.get_name() +
            "\" in range(2016-01-01 , 2016-12-12)")
    assert str(excinfo.value) == 'at qdb_query: The timeseries does not contain this column.'

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



@pytest.mark.parametrize("query_handler", ['dict',
                                           'numpy'])
@pytest.mark.parametrize("value_type", ['double',
                                        'int64',
                                        'blob',
                                        'string',
                                        'timestamp'])
def test_supports_all_column_types(value_type, query_handler, qdbd_connection, table, intervals):
    inserted_data = _insert_points(value_type, table, intervals=intervals)
    column_name = _column_name(table, value_type)
    query = "SELECT \"{}\" FROM \"{}\"".format(column_name, table.get_name())

    if query_handler == 'numpy':
        res = qdbd_connection.query_numpy(query)
        (col, xs) = res[0]
        assert col == column_name
        np.testing.assert_array_equal(xs,
                                      inserted_data[1])
    elif query_handler == 'dict':
        res = qdbd_connection.query(query)
        assert len(res) == len(inserted_data[1])

        for i in range(len(res)):
            assert column_name in res[i]

            val_in  = inserted_data[1][i]
            val_out = res[i][column_name]

            assert val_in == val_out

    else:
        raise RuntimeError("Unrecognized query handler: {}".format(query_handler))


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
@pytest.mark.parametrize("query_handler", ['dict',
                                           'numpy'])
@pytest.mark.parametrize("value_type", ['double',
                                        'int64',
                                        'blob',
                                        'string',
                                        'timestamp'])
@pytest.mark.parametrize("row_count", [16384, 131072, 1048576])
def test_query_handler_benchmark(benchmark, value_type, row_count, query_handler, qdbd_connection, table, intervals):



    query_fn = None
    if query_handler == 'numpy':
        query_fn = qdbd_connection.query_numpy
    elif query_handler == 'dict':
        query_fn = qdbd_connection.query
    else:
        raise RuntimeError("Unrecognized query handler: {}".format(query_handler))



    inserted_data = _insert_points(value_type, table, intervals=intervals, points=row_count)
    column_name = _column_name(table, value_type)
    query = "SELECT \"{}\" FROM \"{}\"".format(column_name, table.get_name())

    benchmark(query_fn, query)



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
    assert res[0]['sum(the_double)'] == pytest.approx(np.sum(inserted_double_data[1]))


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
