# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb.pandas as qdbpd
from functools import reduce
import numpy as np
import pandas as pd
import test_ts_batch as batchlib
import test_pandas as pandaslib

row_count = 10000

def test_bench_double_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_double")

def test_bench_blob_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_blob")

def test_bench_int64_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_int64")

def test_bench_timestamp_series(qdbd_connection, table, many_intervals, benchmark):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_ts")

def test_benchmark_dataframe_read(qdbd_connection, table, benchmark):
    df = pandaslib.gen_df(np.datetime64('2017-01-01'), row_count)
    qdbpd.write_dataframe(df, qdbd_connection, table)
    benchmark(qdbpd.read_dataframe, table)

def test_benchmark_dataframe_write(qdbd_connection, table, benchmark):
    # Ensures that we can do a full-circle write and read of a dataframe
    df = pandaslib.gen_df(np.datetime64('2017-01-01'), row_count)
    benchmark(qdbpd.write_dataframe, df, qdbd_connection, table)
