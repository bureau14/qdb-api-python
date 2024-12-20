# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import numpy as np
import test_batch_inserter as batchlib
import test_pandas as pandaslib
import quasardb.pandas as qdbpd

row_count = 10000


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_bench_double_series(
        qdbd_connection,
        table,
        many_intervals,
        benchmark):
    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps, symbols =
    # batchlib._test_with_table(
    _, _, _, _, _, _ = batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_double")


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_bench_blob_series(qdbd_connection, table, many_intervals, benchmark):
    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps, symbols =
    # batchlib._test_with_table(
    _, _, _, _, _, _ = batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_blob")


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_bench_string_series(
        qdbd_connection,
        table,
        many_intervals,
        benchmark):
    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps, symbols =
    # batchlib._test_with_table(
    _, _, _, _, _, _ = batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_string")


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_bench_int64_series(qdbd_connection, table, many_intervals, benchmark):
    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps, symbols =
    # batchlib._test_with_table(
    _, _, _, _, _, _ = batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_int64")


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_bench_timestamp_series(
        qdbd_connection,
        table,
        many_intervals,
        benchmark):
    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps, symbols =
    # batchlib._test_with_table(
    _, _, _, _, _, _ = batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_ts")


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_bench_symbol_series(
        qdbd_connection,
        table,
        many_intervals,
        benchmark):
    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps, symbols =
    # batchlib._test_with_table(
    _, _, _, _, _, _ = batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    benchmark(qdbpd.read_series, table, "the_symbol")


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_benchmark_dataframe_read(qdbd_connection, table, benchmark):
    df = pandaslib.gen_df(np.datetime64('2017-01-01'), row_count)
    qdbpd.write_dataframe(df, qdbd_connection, table)
    benchmark(qdbpd.read_dataframe, table)


@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_benchmark_dataframe_write(qdbd_connection, table, benchmark):
    # Ensures that we can do a full-circle write and read of a dataframe
    df = pandaslib.gen_df(np.datetime64('2017-01-01'), row_count)
    benchmark(qdbpd.write_dataframe, df, qdbd_connection, table)

@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
@pytest.mark.parametrize("query_handler", ['dict', 'numpy'])
@pytest.mark.parametrize("row_count", [(256),
                                       (256 * 8),
                                       (256 * 8 * 8),
                                       (256 * 8 * 8 * 8),
                                       (256 * 8 * 8 * 8 * 8),])
@pytest.mark.parametrize("column_name", ['the_double',
                                         'the_int64',
                                         'the_blob',
                                         'the_string',
                                         'the_ts'])
def test_benchmark_dataframe_query(query_handler, row_count, column_name, qdbd_connection, table, benchmark):
    qdbd_connection.options().set_client_max_in_buf_size(4 * 1024 * 1024 * 1024)

    # Ensures that we can do a full-circle write and read of a dataframe
    df = pandaslib.gen_df(np.datetime64('2017-01-01'), row_count, unit='s')
    qdbpd.write_pinned_dataframe(df, qdbd_connection, table, fast=True, infer_types=False)

    q = "SELECT \"{}\" FROM \"{}\"".format(column_name,
                                           table.get_name())

    numpy = False
    if query_handler == 'numpy':
        numpy = True

    benchmark(qdbpd.query, qdbd_connection, q, numpy=numpy)
