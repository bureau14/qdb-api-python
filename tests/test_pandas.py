# pylint: disable=missing-module-docstring,missing-function-docstring,not-an-iterable,invalid-name

from datetime import timedelta
import logging
import numpy as np
import numpy.ma as ma
import pandas as pd
import pytest

import quasardb
import quasardb.pandas as qdbpd

ROW_COUNT = 1000

def _to_numpy_masked(xs):
    data = xs.to_numpy()
    mask = xs.isna()
    return ma.masked_array(data=data, mask=mask)

def _assert_df_equal(lhs, rhs):
    """
    Verifies DataFrames lhs and rhs are equal(ish). We're not pedantic that we're comparing
    metadata and things like that.

    Typically one would use `lhs` for the DataFrame that was generated in code, and
    `rhs` for the DataFrame that's returned by qdbpd.
    """

    np.testing.assert_array_equal(lhs.index.to_numpy(), rhs.index.to_numpy())
    assert len(lhs.columns) == len(rhs.columns)
    for col in lhs.columns:

        lhs_ = _to_numpy_masked(lhs[col])
        rhs_ = _to_numpy_masked(rhs[col])

        np.testing.assert_array_equal(lhs_, rhs_)


def gen_idx(start_time, count, step=1, unit='D'):
    return pd.Index(data=pd.date_range(start=start_time,
                                       end=(start_time + (np.timedelta64(step, unit) * count)),
                                       periods=count),
                    dtype='datetime64[ns]',
                    name='$timestamp')


def gen_df(start_time, count, step=1, unit='D'):
    idx = gen_idx(start_time, count, step, unit)

    return pd.DataFrame(data={"the_double": np.random.uniform(-100.0, 100.0, count),
                              "the_int64": np.random.randint(-100, 100, count),
                              "the_blob": np.array(list(np.random.bytes(np.random.randint(16, 32)) for i in range(count)),
                                                   'O'),
                              "the_string": np.array([("content_" + str(item))
                                                      for item in range(count)], 'U'),
                              "the_ts": np.array([
                                  (start_time + np.timedelta64(i, 's'))
                                  for i in range(count)]).astype('datetime64[ns]'),
                              "the_symbol": np.array([("symbol_" + str(item))
                                                      for item in range(count)], 'U')},

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
                                index=idx),
            "the_symbol": pd.Series(np.array([("symbol_" + str(item)) for item in range(count)],
                                             'U'),
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

def test_dataframe(qdbpd_write_fn, df_with_table, qdbd_connection):
    (_, df1, table) = df_with_table
    qdbpd_write_fn(df1, qdbd_connection, table)

    df2 = qdbpd.read_dataframe(table)

    _assert_df_equal(df1, df2)

def test_dataframe_can_read_columns(qdbpd_write_fn, df_with_table, qdbd_connection, column_name, table_name):
    (_, df1, table) = df_with_table
    assert column_name in df1.columns
    assert len(df1.columns) == 1

    qdbpd_write_fn(df1, qdbd_connection, table)

    df2 = qdbpd.read_dataframe(table, columns=[column_name])

    assert len(df2.columns) == 1
    for col in df2.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


def test_dataframe_can_read_ranges(qdbpd_write_fn, qdbd_connection, table):
    start = np.datetime64('2017-01-01', 'ns')
    df1 = gen_df(start, 10)
    qdbpd_write_fn(df1, qdbd_connection, table)

    first_range = (start, start + np.timedelta64(1, 'D'))
    second_range = (start + np.timedelta64(1, 'D'),
                    start + np.timedelta64(2, 'D'))

    df2 = qdbpd.read_dataframe(table)
    df3 = qdbpd.read_dataframe(table, ranges=[first_range])
    df4 = qdbpd.read_dataframe(table, ranges=[first_range, second_range])

    assert df2.shape[0] == 10
    assert df3.shape[0] == 1
    assert df4.shape[0] == 2


def test_write_dataframe(qdbpd_write_fn, df_with_table, qdbd_connection):
    (_, df, table) = df_with_table

    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False)
    res = qdbpd.read_dataframe(table)

    _assert_df_equal(df, res)

@pytest.mark.parametrize("row_count", [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024])
@pytest.mark.parametrize("df_count", [1, 2, 4, 8, 16])
def test_write_unindexed_dataframe(qdbpd_write_fn, df_count, row_count, qdbd_connection, table):
    # CF. QDB-10203, we generate a dataframe which is unordered. The easiest
    # way to do this is reuse our gen_df function with overlapping timestamps,
    # and concatenate the data.

    # We generate a whole bunch of dataframes. Their timestamps overlap, but never collide: we
    # ensure that by using a 'step' of the dataframe count, and let each dataframe start at
    # the appropriate offset.
    #
    # e.g. with 8 dataframes, the first dataframe starts at 2017-01-01 and the second row's
    # timestap is 2017-01-09, while the second dataframe starts at 2017-01-02 and has 2017-01-10
    # as second row, etc.
    dfs = [gen_df((np.datetime64('2017-01-01') + np.timedelta64(i, 'D')) ,
                  row_count,
                  step=df_count)
           for i in range(df_count)]
    df_unsorted = pd.concat(dfs)
    df_sorted = df_unsorted.sort_index().reindex()
    assert len(df_sorted.columns) == len(df_unsorted.columns)

    # We store unsorted
    qdbpd_write_fn(df_unsorted, qdbd_connection, table)


    # We expect to receive sorted data
    df_read = qdbpd.read_dataframe(table)

    assert len(df_sorted.columns) == len(df_read.columns)
    for col in df_sorted.columns:
        np.testing.assert_array_equal(df_sorted[col].to_numpy(),
                                      df_read[col].to_numpy())


def test_write_dataframe_push_fast(qdbpd_write_fn, qdbd_connection, table):
    # Ensures that we can do a full-circle write and read of a dataframe
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    qdbpd_write_fn(df1, qdbd_connection, table, fast=True)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


@pytest.mark.parametrize("write_fn", [qdbpd.write_dataframe])
@pytest.mark.parametrize("truncate", [True])
def test_write_dataframe_push_truncate(write_fn, truncate, qdbd_connection, table):
    # Ensures that we can do a full-circle write and read of a dataframe
    df1 = gen_df(np.datetime64('2017-01-01'), count=1000)

    write_fn(df1, qdbd_connection, table, truncate=truncate)
    write_fn(df1, qdbd_connection, table, truncate=truncate)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) >= len(df2.columns)
    for col in df2.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(),
                                      df2[col].to_numpy())

@pytest.mark.parametrize("write_fn", [qdbpd.write_dataframe,
                                      qdbpd.write_pinned_dataframe])
def test_write_dataframe_create_table(write_fn, caplog, qdbd_connection, entry_name):
    caplog.set_level(logging.DEBUG)
    table = qdbd_connection.ts(entry_name)
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    write_fn(df1, qdbd_connection, table, create=True)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())


@pytest.mark.parametrize("write_fn", [qdbpd.write_dataframe,
                                      qdbpd.write_pinned_dataframe])
def test_write_dataframe_create_table_twice(write_fn, qdbd_connection, table):
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    write_fn(df1, qdbd_connection, table, create=True)

@pytest.mark.parametrize("write_fn", [qdbpd.write_dataframe,
                                      qdbpd.write_pinned_dataframe])
def test_write_dataframe_create_table_with_shard_size(write_fn, caplog, qdbd_connection, entry_name):
    caplog.set_level(logging.DEBUG)
    table = qdbd_connection.ts(entry_name)
    df1 = gen_df(np.datetime64('2017-01-01'), ROW_COUNT)
    write_fn(df1, qdbd_connection, table, create=True, shard_size=timedelta(weeks=4))

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for col in df1.columns:
        np.testing.assert_array_equal(df1[col].to_numpy(), df2[col].to_numpy())

def test_query(qdbpd_write_fn, # parametrized
               qdbpd_query_fn, # parametrized
               df_with_table,  # parametrized
               qdbd_connection,
               column_name,
               table_name):
    (_, df1, table) = df_with_table
    qdbpd_write_fn(df1, qdbd_connection, table)

    df2 = qdbpd_query_fn(qdbd_connection,
                         "SELECT $timestamp, {} FROM \"{}\"".format(column_name, table_name)
                         ).set_index('$timestamp').reindex()

    _assert_df_equal(df1, df2)

def test_inference(
        qdbpd_write_fn,
        gen_df,
        qdbd_connection,
        table_1col):
    # Note that there are no tests; we effectively only test whether it doesn't
    # throw.
    (_, df) = gen_df
    qdbpd_write_fn(df, qdbd_connection, table_1col)
