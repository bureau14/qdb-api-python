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
    df1 = gen_df(np.datetime64('2017-01-01'), 10000)
    qdbpd.write_dataframe(df1, qdbd_connection, table, chunk_size=4000)

    df2 = qdbpd.read_dataframe(table)

     # Ensure equal sorting of columns
    df2 = df2.reindex(df1.columns, axis=1)

    print("")
    print("df1:")
    print(str(df1))
    print("df2:")
    print(str(df2))

    assert df1.equals(df2)

def test_write_dataframe_create_table(qdbd_connection, entry_name):
    table = qdbd_connection.ts(entry_name)
    df1 = gen_df(np.datetime64('2017-01-01'), 10000)
    qdbpd.write_dataframe(df1, qdbd_connection, table, create=True)

    df2 = qdbpd.read_dataframe(table)

     # Ensure equal sorting of columns
    df2 = df2.reindex(df1.columns, axis=1)

    assert df1.equals(df2)
