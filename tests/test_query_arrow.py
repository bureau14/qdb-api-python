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

def test_returns_alias_not_found_when_ts_doesnt_exist(qdbd_connection):
    with pytest.raises(quasardb.AliasNotFoundError):
        qdbd_connection.query_arrow(
            'select * from ' +
            'this_ts_doesnt_exist' +
            ' in range(2017, +10d)')

def test_returns_empty_result(qdbd_connection, table):
    res = qdbd_connection.query_arrow(
        "select * from \"" +
        table.get_name() +
        "\" in range(2016-01-01 , 2016-12-12)")
    # We have 8 null columns
    assert len(res) == 1
    for x in res.columns:
        assert x.is_null()

@pytest.mark.parametrize("value_type", ['double',
                                        'int64',
                                        'blob',
                                        'string',
                                        'timestamp'])
def test_supports_all_column_types(value_type, qdbd_connection, table, intervals):
    inserted_data = _insert_points(value_type, table, intervals=intervals)
    column_name = _column_name(table, value_type)
    query = "SELECT \"{}\" FROM \"{}\"".format(column_name, table.get_name())

    res = qdbd_connection.query_arrow(query)
    print(res)


# def test_returns_inserted_data_with_column_select(
#         qdbd_connection, table, intervals):
#     start_time = tslib._start_time(intervals)
#     inserted_double_data = _insert_double_points(table, start_time, 10)
#     query = "select " + tslib._double_col_name(table) + \
#         " from \"" + table.get_name() + "\" in range(" + \
#         str(tslib._start_year(intervals)) + ", +100d)"

#     res = qdbd_connection.query(query)

#     assert len(res) == 10

#     for row, v in zip(res, inserted_double_data[1]):
#         assert '$table' not in row
#         assert '$timestamp' not in row
#         assert 'the_blob' not in row
#         assert 'the_int64' not in row
#         assert 'the_ts' not in row

#         assert row['the_double'] == v