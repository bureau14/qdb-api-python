# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb.pandas as qdbpd
from functools import reduce
import numpy as np
import pandas as pd
import test_ts_batch as batchlib

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
    benchmark(qdbpd.read_series, table, "the_double", [total_range])

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
    benchmark(qdbpd.read_series, table, "the_blob", [total_range])

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
    benchmark(qdbpd.read_series, table, "the_int64", [total_range])

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
    benchmark(qdbpd.read_series, table, "the_ts", [total_range])


def test_benchmark_dataframe(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    benchmark(qdbpd.read_dataframe, table)
