# pylint: disable=missing-module-docstring,missing-function-docstring,not-an-iterable,invalid-name

import logging
import numpy as np
import numpy.ma as ma
import pandas as pd
import pytest

import quasardb
import quasardb.pandas as qdbpd

ROW_COUNT = 1000


def gen_df(start_time, count):
    idx = pd.date_range(start_time, periods=count, freq='S')

    return pd.DataFrame(data={"the_double": np.random.uniform(-100.0, 100.0, count),
                              "the_int64": np.random.randint(-100, 100, count),
                              "the_blob": np.array(list(np.random.bytes(np.random.randint(16, 32)) for i in range(count)),
                                                   'O'),
                              "the_string": np.array([("content_" + str(item))
                                                      for item in range(count)], 'U'),
                              "the_ts": np.array([
                                  (start_time + np.timedelta64(i, 's'))
                                  for i in range(count)]).astype('datetime64[ns]')},
                        index=idx)


def gen_series(start_time, count):
    idx = pd.date_range(start_time, periods=count, freq='S')

    return {"the_double": pd.Series(np.random.uniform(-100.0, 100.0, count),
                                    index=idx),
            "the_int64": pd.Series(np.random.randint(-100, 100, count),
                                   index=idx),
            "the_blob": pd.Series(np.array(list(np.random.bytes(np.random.randint(16, 32)) for i in range(count)),
                                           'O'),
                                  index=idx),
            "the_string": pd.Series(np.array([("content_" + str(item)) for item in range(count)],
                                             'U'),
                                    index=idx),
            "the_ts": pd.Series(np.array([(start_time + np.timedelta64(i, 's'))
                                          for i in range(count)]).astype('datetime64[ns]'),
                                index=idx)}


def test_series_read_write(table):
    series = gen_series(np.datetime64('2017-01-01', 'ns'), ROW_COUNT)

    # Insert everything
    for col in series:
        qdbpd.write_series(series[col], table, col)

    # Read everything
    for col in series:
        read_series = qdbpd.read_series(table, col)
        assert isinstance(read_series, pd.core.series.Series)
        assert read_series.size == ROW_COUNT
        np.testing.assert_array_equal(
            read_series.to_numpy(), series[col].to_numpy())


def test_dataframe(qdbd_connection, table):
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


def test_dataframe_can_read_columns(qdbd_connection, table):
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table)

    df2 = qdbpd.read_dataframe(table, columns=['the_double', 'the_int64'])

    assert len(df1.columns) != len(df2.columns)
    assert len(df2.columns) == 2
    for col in df2.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


def test_dataframe_can_read_ranges(qdbd_connection, table):
    start = np.datetime64('2017-01-01', 'ns')
    df1 = gen_df(start, 10)
    qdbpd.write_dataframe(df1, qdbd_connection, table)

    first_range = (start, start + np.timedelta64(1, 's'))
    second_range = (start + np.timedelta64(1, 's'),
                    start + np.timedelta64(2, 's'))

    df2 = qdbpd.read_dataframe(table)
    df3 = qdbpd.read_dataframe(table, ranges=[first_range])
    df4 = qdbpd.read_dataframe(table, ranges=[first_range, second_range])

    assert df2.shape[0] == 10
    assert df3.shape[0] == 1
    assert df4.shape[0] == 2


def test_write_dataframe(qdbd_connection, table):
    # Ensures that we can do a full-circle write and read of a dataframe
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


def test_write_dataframe_push_fast(qdbd_connection, table):
    # Ensures that we can do a full-circle write and read of a dataframe
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table, fast=True)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


@pytest.mark.parametrize("truncate", [True,
                                      (np.datetime64('2017-01-01', 'ns'),
                                       np.datetime64('2017-01-02', 'ns'))])
def test_write_dataframe_push_truncate(truncate, qdbd_connection, table):
    # Ensures that we can do a full-circle write and read of a dataframe
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table, truncate=truncate)
    qdbpd.write_dataframe(df1, qdbd_connection, table, truncate=truncate)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


def test_write_dataframe_create_table(caplog, qdbd_connection, entry_name):
    caplog.set_level(logging.DEBUG)
    table = qdbd_connection.ts(entry_name)
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table, create=True)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


def test_write_dataframe_create_table_twice(qdbd_connection, table):
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table, create=True)


def check_equal(expected, actual):
    if np.isnan(expected):
        assert np.isnan(actual)
    else:
        assert expected == actual


def test_dataframe_read_fast_is_unordered(qdbd_connection, table):
    # As of now, when reading a dataframe fast, when it contains null values,
    # rows are not guaranteed to be ordered; they might be matched to the
    # wrong rows.
    #
    # This will be fixed when the reader supports pinned columns, so we can
    # read the data fast (as numpy arrays) and at the same time keep ordering.

    df1 = gen_df(np.datetime64('2017-01-01'), 2)
    df2 = gen_df(np.datetime64('2017-01-01'), 2)

    ts1 = np.datetime64('2017-01-01 00:00:00', 'ns')
    ts2 = np.datetime64('2017-01-01 00:00:01', 'ns')

    # Now, we set the wrong value for a first row in df1, and for
    # a second row in df2
    df1.at[ts1, 'the_double'] = None
    df2.at[ts2, 'the_double'] = None

    df3 = pd.concat([df1, df2]).sort_index()
    qdbpd.write_dataframe(df3, qdbd_connection, table)

    df4 = qdbpd.read_dataframe(table)

    assert len(df3.columns) == len(df4.columns)
    for col in df3.columns:
        expected = True
        if col == 'the_double':
            expected = False

        assert np.array_equal(df3[col].to_numpy(),
                              df4[col].to_numpy()) == expected

    df5 = qdbpd.read_dataframe(table, row_index=True)

    assert df5.at[0, 'the_int64'] == df1.at[ts1, 'the_int64']
    assert df5.at[1, 'the_int64'] == df2.at[ts1, 'the_int64']
    assert df5.at[2, 'the_int64'] == df1.at[ts2, 'the_int64']
    assert df5.at[3, 'the_int64'] == df2.at[ts2, 'the_int64']

    # QDB-2418
    check_equal(df5.at[0, 'the_double'], df1.at[ts1, 'the_double'])
    check_equal(df5.at[1, 'the_double'], df2.at[ts1, 'the_double'])
    check_equal(df5.at[2, 'the_double'], df1.at[ts2, 'the_double'])
    check_equal(df5.at[3, 'the_double'], df2.at[ts2, 'the_double'])


def test_query(qdbd_connection, table):
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd.write_dataframe(df1, qdbd_connection, table)

    res = qdbpd.query(qdbd_connection, "SELECT * FROM \"" +
                      table.get_name() + "\"", blobs=['the_blob'])

    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), res[col].to_numpy())


def _gen_floating(n, low=-100.0, high=100.0):
    return np.random.uniform(low, high, n)


def _gen_integer(n):
    return np.random.randint(-100, 100, n)


def _gen_string(n):
    # Slightly hacks, but for testing purposes we'll ensure that we are generating
    # blobs that can be cast to other types as well.
    return list(str(x) for x in _gen_floating(n, low=0))


def _gen_blob(n):
    return list(x.encode("utf-8") for x in _gen_string(n))


def _gen_timestamp(n):
    start_time = np.datetime64('2017-01-01', 'ns')
    return np.array([(start_time + np.timedelta64(i, 's'))
                     for i in range(n)]).astype('datetime64[ns]')


def _sparsify(xs):
    # Randomly make a bunch of elements null, we make use of Numpy's masked
    # arrays for this: keep track of a separate boolean 'mask' array, which
    # determines whether or not an item in the array is None.
    mask = np.random.choice(a=[True, False], size=len(xs))
    return ma.masked_array(data=xs,
                           mask=mask)


@pytest.mark.parametrize("sparse", [True, False])
@pytest.mark.parametrize("input_gen", [_gen_floating,
                                       _gen_integer,
                                       _gen_blob,
                                       _gen_string,
                                       _gen_timestamp])
@pytest.mark.parametrize("column_type", [quasardb.ColumnType.Int64,
                                         quasardb.ColumnType.Double,
                                         quasardb.ColumnType.Blob,
                                         quasardb.ColumnType.String,
                                         quasardb.ColumnType.Timestamp])
def test_inference(qdbd_connection, table_name, input_gen, column_type, sparse):

    # Create table
    t = qdbd_connection.ts(table_name)
    t.create([quasardb.ColumnInfo(column_type, "x")])

    n = 100
    idx = pd.date_range(np.datetime64('2017-01-01'), periods=n, freq='S')
    xs = input_gen(n)
    if sparse is True:
        xs = _sparsify(xs)

    df = pd.DataFrame(data={"x": xs}, index=idx)

    # Note that there are no tests; we effectively only test whether it doesn't
    # throw.
    qdbpd.write_dataframe(df, qdbd_connection, t)
