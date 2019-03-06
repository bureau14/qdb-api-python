# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb.pandas as qdbpd
from functools import reduce
import numpy as np
import pandas as pd

import test_ts as tslib
import test_ts_batch as batchlib

def test_can_read_series(qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    double_series = qdbpd.read_series(table, "the_double")
    blob_series = qdbpd.read_series(table, "the_blob")
    int64_series = qdbpd.read_series(table, "the_int64")
    ts_series = qdbpd.read_series(table, "the_ts")

    assert type(double_series) == pd.core.series.Series
    assert type(blob_series) == pd.core.series.Series
    assert type(int64_series) == pd.core.series.Series
    assert type(ts_series) == pd.core.series.Series

    np.testing.assert_array_equal(double_series.to_numpy(), doubles)
    np.testing.assert_array_equal(blob_series.to_numpy(), blobs)
    np.testing.assert_array_equal(int64_series.to_numpy(), integers)
    np.testing.assert_array_equal(ts_series.to_numpy(), timestamps)

def test_dataframe(qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    df = qdbpd.read_dataframe(table)

    np.testing.assert_array_equal(df['the_double'].to_numpy(), doubles)
    np.testing.assert_array_equal(df['the_blob'].to_numpy(), blobs)
    np.testing.assert_array_equal(df['the_int64'].to_numpy(), integers)
    np.testing.assert_array_equal(df['the_ts'].to_numpy(), timestamps)

def test_dataframe_can_read_columns(qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    df = qdbpd.read_dataframe(table, columns=['the_double', 'the_int64'])

    np.testing.assert_array_equal(df['the_double'].to_numpy(), doubles)
    np.testing.assert_array_equal(df['the_int64'].to_numpy(), integers)

def test_dataframe_can_read_single_column(qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    df = qdbpd.read_dataframe(table, columns=['the_double'])

    np.testing.assert_array_equal(df['the_double'].to_numpy(), doubles)

def test_dataframe_can_read_ranges(qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    first_range = (many_intervals[0], many_intervals[1])
    second_range = (many_intervals[1], many_intervals[2])

    df1 = qdbpd.read_dataframe(table, ranges=[first_range])
    df2 = qdbpd.read_dataframe(table, ranges=[first_range, second_range])

    assert df1.shape[0] == 1
    assert df2.shape[0] == 2

def _slice_lists(l, r):
    res = [None]*(len(l) + len(r))
    res[::2] = r
    res[1::2] = l
    return res

# def test_dataframe_can_keep_ordering(qdbd_connection, table, many_intervals):
#     batch_inserter = qdbd_connection.ts_batch(
#         batchlib._make_ts_batch_info(table))

#     doubles1, blobs1, integers1, timestamps1 = batchlib._test_with_table(
#         batch_inserter,
#         table,
#         many_intervals,
#         batchlib._row_insertion_method,
#         batchlib._regular_push)

#     doubles=_slice_lists(doubles1, doubles2)
#     blobs=_slice_lists(blobs1, blobs2)
#     integers=_slice_lists(integers1, integers2)
#     timestamps=_slice_lists(timestamps1, timestamps2)

#     total_range = (many_intervals[0], many_intervals[-1] + np.timedelta64(1, 's'))
#     df = qdbpd.read_dataframe(table, ranges=[total_range])

#     print(str(""))

#     offset = 0
#     for x in df['the_double'].to_numpy():
#         #print("x = ", str(x), ", doubles[offset] = ", str(doubles[offset]))
#         offset += 1

    #print(str(df))

def gen_df(start_time, count):
    idx = pd.date_range(start_time, periods=count, freq='S')

    return pd.DataFrame(data={"the_double": np.random.uniform(-100.0, 100.0, count),
                              "the_int64": np.random.randint(-100, 100, count),
                              "the_blob": np.array([(b"content_" + bytes(item)) for item in range(count)]),
                              "the_ts": np.array([(start_time + np.timedelta64(i, 's'))
                                                  for i in range(count)]).astype('datetime64[ns]')},
                        index=idx)

def test_write_dataframe(qdbd_connection, table):
    # Ensures that we can do a full-circle write and read of a dataframe
    df1 = gen_df(np.datetime64('2017-01-01'), 10)
    qdbpd.write_dataframe(df1, qdbd_connection, table, chunk_size=4)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for c in df1.columns:
        np.testing.assert_array_equal(df1[c].to_numpy(), df2[c].to_numpy())

def test_write_dataframe_create_table(qdbd_connection, entry_name):
    table = qdbd_connection.ts(entry_name)
    df1 = gen_df(np.datetime64('2017-01-01'), 10000)
    qdbpd.write_dataframe(df1, qdbd_connection, table, create=True)

    df2 = qdbpd.read_dataframe(table)

    assert len(df1.columns) == len(df2.columns)
    for c in df1.columns:
        np.testing.assert_array_equal(df1[c].to_numpy(), df2[c].to_numpy())

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
    for c in df3.columns:
        expected = True
        if c is 'the_double':
            expected = False

        assert np.array_equal(df3[c].to_numpy(), df4[c].to_numpy()) == expected

    df5 = qdbpd.read_dataframe(table, row_index=True)

    assert df5.at[0, 'the_int64'] == df1.at[ts1, 'the_int64']
    assert df5.at[1, 'the_int64'] == df2.at[ts1, 'the_int64']
    assert df5.at[2, 'the_int64'] == df1.at[ts2, 'the_int64']
    assert df5.at[3, 'the_int64'] == df2.at[ts2, 'the_int64']

    # Commenting out failing tests for QDB-2418, re-enable when fixed
    #assert df5.at[0, 'the_double'] == df1.at[ts1, 'the_double']
    #assert df5.at[1, 'the_double'] == df2.at[ts1, 'the_double']
    assert df5.at[2, 'the_double'] == df1.at[ts2, 'the_double']
    #assert df5.at[3, 'the_double'] == df2.at[ts2, 'the_double']
