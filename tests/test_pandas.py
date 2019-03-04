# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb.pandas as qdbpd
from functools import reduce
import numpy as np
import pandas as pd
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

    total_range = (many_intervals[0], many_intervals[-1] + np.timedelta64(1, 's'))
    double_series = qdbpd.as_series(table, "the_double", [total_range])
    blob_series = qdbpd.as_series(table, "the_blob", [total_range])
    int64_series = qdbpd.as_series(table, "the_int64", [total_range])
    ts_series = qdbpd.as_series(table, "the_ts", [total_range])

    assert type(double_series) == pd.core.series.Series
    assert type(blob_series) == pd.core.series.Series
    assert type(int64_series) == pd.core.series.Series
    assert type(ts_series) == pd.core.series.Series

    np.testing.assert_array_equal(double_series.to_numpy(), doubles)
    np.testing.assert_array_equal(blob_series.to_numpy(), blobs)
    np.testing.assert_array_equal(int64_series.to_numpy(), integers)
    np.testing.assert_array_equal(ts_series.to_numpy(), timestamps)

def test_bench_double_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    total_range = (many_intervals[0], many_intervals[-1] + np.timedelta64(1, 's'))
    benchmark(qdbpd.as_series, table, "the_double", [total_range])

def test_bench_blob_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    total_range = (many_intervals[0], many_intervals[-1] + np.timedelta64(1, 's'))
    benchmark(qdbpd.as_series, table, "the_blob", [total_range])

def test_bench_int64_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    total_range = (many_intervals[0], many_intervals[-1] + np.timedelta64(1, 's'))
    benchmark(qdbpd.as_series, table, "the_int64", [total_range])

def test_bench_timestamp_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    total_range = (many_intervals[0], many_intervals[-1] + np.timedelta64(1, 's'))
    benchmark(qdbpd.as_series, table, "the_ts", [total_range])


def test_can_read_dataframe(qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    df = qdbpd.as_dataframe(table)
    np.testing.assert_array_equal(df['the_double'].to_numpy(), doubles)
    np.testing.assert_array_equal(df['the_blob'].to_numpy(), blobs)
    np.testing.assert_array_equal(df['the_int64'].to_numpy(), integers)
    np.testing.assert_array_equal(df['the_ts'].to_numpy(), timestamps)

def test_benchmark_dataframe(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    benchmark(qdbpd.as_dataframe, table)
