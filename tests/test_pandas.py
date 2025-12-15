# pylint: disable=missing-module-docstring,missing-function-docstring,not-an-iterable,invalid-name

from datetime import datetime, timedelta
import logging
import numpy as np
import numpy.ma as ma
import pandas as pd
import pytest
import conftest

import quasardb
import quasardb.pandas as qdbpd

ROW_COUNT = 1000

logger = logging.getLogger("test-pandas")


def _to_numpy_masked(xs):
    data = xs.to_numpy()
    mask = xs.isna()
    return ma.masked_array(data=data, mask=mask)


def _assert_series_equal(lhs, rhs):
    lhs_ = _to_numpy_masked(lhs)
    rhs_ = _to_numpy_masked(rhs)

    assert ma.count_masked(lhs_) == ma.count_masked(rhs_)

    logger.debug("lhs: %s", lhs_[:10])
    logger.debug("rhs: %s", rhs_[:10])

    lhs_ = lhs_.torecords()
    rhs_ = rhs_.torecords()

    for (lval, lmask), (rval, rmask) in zip(lhs_, rhs_):
        assert lmask == rmask

        if lmask is False:
            assert lval == rval


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
        _assert_series_equal(lhs[col], rhs[col])


def gen_idx(start_time, count, step=1, unit="D"):
    return pd.Index(
        data=pd.date_range(
            start=start_time,
            end=(start_time + (np.timedelta64(step, unit) * count)),
            periods=count,
        ),
        dtype="datetime64[ns]",
        name="$timestamp",
    )


def gen_df(start_time, count, step=1, unit="D"):
    idx = gen_idx(start_time, count, step, unit)

    return pd.DataFrame(
        data={
            "the_double": np.random.uniform(-100.0, 100.0, count),
            "the_int64": np.random.randint(-100, 100, count),
            "the_blob": np.array(
                list(np.random.bytes(np.random.randint(16, 32)) for i in range(count)),
                "O",
            ),
            "the_string": np.array(
                [("content_" + str(item)) for item in range(count)], "U"
            ),
            "the_ts": np.array(
                [(start_time + np.timedelta64(i, "s")) for i in range(count)]
            ).astype("datetime64[ns]"),
            "the_symbol": np.array(
                [("symbol_" + str(item)) for item in range(count)], "U"
            ),
        },
        index=idx,
    )


def gen_series(start_time, count):
    idx = pd.date_range(start_time, periods=count, freq="S")

    return {
        "the_double": pd.Series(np.random.uniform(-100.0, 100.0, count), index=idx),
        "the_int64": pd.Series(np.random.randint(-100, 100, count), index=idx),
        "the_blob": pd.Series(
            np.array(
                list(np.random.bytes(np.random.randint(16, 32)) for i in range(count)),
                "O",
            ),
            index=idx,
        ),
        "the_string": pd.Series(
            np.array([("content_" + str(item)) for item in range(count)], "U"),
            index=idx,
        ),
        "the_ts": pd.Series(
            np.array(
                [(start_time + np.timedelta64(i, "s")) for i in range(count)]
            ).astype("datetime64[ns]"),
            index=idx,
        ),
        "the_symbol": pd.Series(
            np.array([("symbol_" + str(item)) for item in range(count)], "U"), index=idx
        ),
    }


def test_series_read_write(series_with_table):
    (ctype, dtype, series, table) = series_with_table

    col = table.column_id_by_index(0)
    qdbpd.write_series(series, table, col, dtype=dtype)

    # Read everything
    res = qdbpd.read_series(table, col)

    _assert_series_equal(series, res)


def test_dataframe(qdbpd_write_fn, df_with_table, qdbd_connection):
    (_, _, df1, table) = df_with_table
    qdbpd_write_fn(df1, qdbd_connection, table)

    df2 = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df1, df2)


@pytest.mark.parametrize("sparsify", conftest.no_sparsify)
def test_dataframe_can_read_columns(
    qdbpd_write_fn, df_with_table, qdbd_connection, column_name, table_name
):
    (ctype, dtype, df1, table) = df_with_table
    assert column_name in df1.columns
    assert len(df1.columns) == 1

    qdbpd_write_fn(df1, qdbd_connection, table, infer_types=False, dtype=dtype)

    df2 = qdbpd.read_dataframe(qdbd_connection, table, column_names=[column_name])

    _assert_df_equal(df1, df2)


def test_dataframe_can_read_ranges(
    qdbpd_write_fn, qdbd_connection, df_with_table, start_date, row_count
):
    (ctype, dtype, df1, table) = df_with_table

    qdbpd_write_fn(df1, qdbd_connection, table)

    offset_25 = start_date + np.timedelta64(int(row_count / 4), "s")
    offset_75 = start_date + (3 * np.timedelta64(int(row_count / 4), "s"))

    range_0_25 = (start_date, offset_25)
    range_25_75 = (offset_25, offset_75)

    df2 = qdbpd.read_dataframe(qdbd_connection, table)
    df3 = qdbpd.read_dataframe(qdbd_connection, table, ranges=[range_0_25])
    df4 = qdbpd.read_dataframe(qdbd_connection, table, ranges=[range_25_75])
    df5 = qdbpd.read_dataframe(qdbd_connection, table, ranges=[range_0_25, range_25_75])

    assert df2.shape[0] == row_count
    assert df3.shape[0] == row_count / 4
    assert df4.shape[0] == row_count / 2
    assert df5.shape[0] == (row_count / 4) * 3


def test_write_dataframe(qdbpd_write_fn, df_with_table, qdbd_connection):
    (ctype, dtype, df, table) = df_with_table

    # We always need to infer
    qdbpd_write_fn(df, qdbd_connection, table, infer_types=True)
    res = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df, res)


def test_multiple_dataframe(
    qdbpd_writes_fn, dfs_with_tables, qdbd_connection, reader_batch_size
):
    """
    Tests both writing and reader from multiple tables.
    """

    payload = []
    input_dfs = []
    tables = []

    for _, _, df, table in dfs_with_tables:
        # Fill the dataframe with the table name, as this is what's returned by the bulk reader
        # as well
        payload.append((table, df))
        df["$table"] = table.get_name()
        input_dfs.append(df)
        tables.append(table)

    qdbpd_writes_fn(
        payload,
        qdbd_connection,
        infer_types=True,
        push_mode=quasardb.WriterPushMode.Fast,
    )

    xs = list(
        qdbpd.stream_dataframes(qdbd_connection, tables, batch_size=reader_batch_size)
    )

    # Ensure batch sizes are what we expect
    for x in xs:
        assert len(x.index) > 0
        assert len(x.index) <= reader_batch_size

    # Concatenate all results into a single dataframe and compare
    df1 = pd.concat(input_dfs)
    df2 = pd.concat(xs)

    _assert_df_equal(df1, df2)


# Pandas is a retard when it comes to null values, so make sure to just only
# generate "full" arrays when we don't infer / convert array types.
@pytest.mark.parametrize("sparsify", conftest.no_sparsify)
def test_write_dataframe_no_infer(
    qdbpd_write_fn, df_with_table, qdbd_connection, reader_batch_size
):
    (ctype, dtype, df, table) = df_with_table

    # We always need to infer
    if dtype == np.str_:
        logger.warn("Skipping test because pandas messes up strings")
        with pytest.raises(
            TypeError,
            match=r"Data for column \'([a-z]{16})\' with type \'ColumnType.String\' was provided in dtype \'",
        ):
            qdbpd_write_fn(df, qdbd_connection, table, infer_types=False)
    else:
        qdbpd_write_fn(df, qdbd_connection, table, infer_types=False)
        res = qdbpd.read_dataframe(qdbd_connection, table)

        _assert_df_equal(df, res)


def test_write_unindexed_dataframe(
    qdbpd_write_fn, df_with_table, df_count, qdbd_connection, caplog
):
    # CF. QDB-10203, we generate a dataframe which is unordered. The easiest
    # way to do this is reuse our gen_df function with overlapping timestamps,
    # and concatenate the data.

    # We generate a whole bunch of dataframes. Their timestamps overlap, but
    # never collide: we ensure that by using a 'step' of the dataframe count,
    # and let each dataframe start at the appropriate offset.
    #
    # e.g. with 8 dataframes, the first dataframe starts at 2017-01-01 and the
    # second row's timestap is 2017-01-09, while the second dataframe starts at
    # 2017-01-02 and has 2017-01-10 as second row, etc.

    (ctype, dtype, df, table) = df_with_table

    dfs = (df.copy(deep=True) for i in range(df_count))
    dfs = (df.sample(frac=1) for df in dfs)

    df_unsorted = pd.concat(dfs)
    df_sorted = df_unsorted.sort_index().reindex()
    assert len(df_sorted.columns) == len(df_unsorted.columns)

    # We store unsorted
    caplog.clear()
    caplog.set_level(logging.WARN, logger="quasardb.pandas")
    qdbpd_write_fn(df_unsorted, qdbd_connection, table)
    msgs = [x.message for x in caplog.records]
    assert "dataframe index is unsorted, resorting dataframe based on index" in msgs

    # We expect to receive sorted data
    df_read = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df_sorted, df_read)


def test_write_dataframe_push_fast(qdbpd_write_fn, qdbd_connection, df_with_table):
    (_, _, df1, table) = df_with_table

    # Ensures that we can do a full-circle write and read of a dataframe
    qdbpd_write_fn(df1, qdbd_connection, table, push_mode=quasardb.WriterPushMode.Fast)

    df2 = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df1, df2)


def test_write_dataframe_push_truncate(qdbpd_write_fn, qdbd_connection, df_with_table):
    (_, _, df1, table) = df_with_table

    # For Arrow Push we need to have the truncate range
    step = df1.index[1] - df1.index[0]
    start = np.datetime64(df1.index[0].to_datetime64(), "ns")
    end = np.datetime64((df1.index[-1] + step).to_datetime64(), "ns")
    ranges = (start, end)

    # Ensures that we can do a full-circle write and read of a dataframe
    qdbpd_write_fn(
        df1,
        qdbd_connection,
        table,
        push_mode=quasardb.WriterPushMode.Truncate,
        range=ranges,
    )
    qdbpd_write_fn(
        df1,
        qdbd_connection,
        table,
        push_mode=quasardb.WriterPushMode.Truncate,
        range=ranges,
    )

    df2 = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df1, df2)


def test_write_dataframe_deduplicate(
    qdbpd_write_fn, qdbd_connection, df_with_table, deduplicate
):
    (_, _, df1, table) = df_with_table

    # Main validation:
    # 1. write once, no deduplication
    # 2. write again, with deduplication
    # 3. read data again
    #
    # data from step 3 should be identical to the input dataframe, as all data
    # should be deduplicated upon insertion.

    # Ensures that we can do a full-circle write and read of a dataframe
    qdbpd_write_fn(
        df1, qdbd_connection, table, deduplicate=False, deduplication_mode="drop"
    )
    qdbpd_write_fn(
        df1, qdbd_connection, table, deduplicate=deduplicate, deduplication_mode="drop"
    )

    df2 = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df1, df2)


def test_write_dataframe_create_table(
    qdbpd_write_fn, qdbd_connection, gen_df, table_name
):
    (_, _, df1) = gen_df

    table = qdbd_connection.ts(table_name)
    qdbpd_write_fn(df1, qdbd_connection, table, create=True)

    df2 = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df1, df2)


def test_write_dataframe_create_table_twice(
    qdbpd_write_fn, qdbd_connection, df_with_table
):
    (_, _, df, table) = df_with_table
    qdbpd_write_fn(df, qdbd_connection, table, create=True)


def test_write_dataframe_create_table_with_shard_size(
    qdbpd_write_fn, qdbd_connection, gen_df, table_name
):
    (_, _, df1) = gen_df
    table = qdbd_connection.ts(table_name)

    qdbpd_write_fn(
        df1, qdbd_connection, table, create=True, shard_size=timedelta(weeks=4)
    )

    df2 = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df1, df2)


def test_query(
    qdbpd_write_fn,  # parametrized
    qdbpd_query_fn,  # parametrized
    df_with_table,  # parametrized
    qdbd_connection,
    column_name,
    table_name,
):
    (_, _, df1, table) = df_with_table
    qdbpd_write_fn(df1, qdbd_connection, table)

    df2 = (
        qdbpd_query_fn(
            qdbd_connection,
            'SELECT $timestamp, {} FROM "{}"'.format(column_name, table_name),
        )
        .set_index("$timestamp")
        .reindex()
    )

    _assert_df_equal(df1, df2)


def test_inference(qdbpd_write_fn, df_with_table, qdbd_connection):  # parametrized
    # Note that there are no tests; we effectively only test whether it doesn't
    # throw.
    (_, _, df, table) = df_with_table
    qdbpd_write_fn(df, qdbd_connection, table, infer_types=True)


def test_shuffled_columns(
    qdbpd_write_fn, qdbd_connection, table_name, start_date, row_count
):
    t = qdbd_connection.table(table_name)

    # Specific regresion test used in Python user guide / API documentation
    cols = [
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "open"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "close"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "high"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "low"),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, "volume"),
    ]

    t.create(cols)

    idx = np.array(
        [start_date + np.timedelta64(i, "s") for i in range(row_count)]
    ).astype("datetime64[ns]")

    data = {
        "$timestamp": idx,
        "$table": [table_name for i in range(row_count)],
        "close": np.random.uniform(100, 200, row_count),
        "volume": np.random.randint(10000, 20000, row_count),
        "open": np.random.uniform(100, 200, row_count),
    }

    df = pd.DataFrame(data).set_index("$timestamp").reindex()

    qdbpd_write_fn(df, qdbd_connection, t, infer_types=True)


def test_regression_sc11057(qdbd_connection, table_name):
    """
    Our query results (apparently) return datetime64 arrays with a itemsize of 8,
    but a stride size of 16 -- i.e. every other item is "skipped".

    By re-writing query results as a dataframe, we validate that our backend
    properly deals with these arrays.
    """

    idx = [
        np.datetime64("2022-09-08T12:00:01", "ns"),
        np.datetime64("2022-09-08T12:00:02", "ns"),
        np.datetime64("2022-09-08T12:00:03", "ns"),
    ]
    data = {
        "record_timestamp": [
            np.datetime64("2022-09-08T13:00:04", "ns"),
            np.datetime64("2022-09-08T13:00:05", "ns"),
            np.datetime64("2022-09-08T13:00:06", "ns"),
        ],
        "unique_tagname": np.array(["ABC", "DEF", "GHI"], dtype="unicode"),
    }
    df = pd.DataFrame(data=data, index=idx)

    qdbpd.write_dataframe(
        df, qdbd_connection, table_name, create=True, infer_types=True
    )

    q = 'select $timestamp, record_timestamp, unique_tagname from "{}"'.format(
        table_name
    )
    df_ = qdbpd.query(qdbd_connection, q)
    df_ = df_.groupby("unique_tagname").last().set_index("$timestamp")

    qdbpd.write_dataframe(
        df_, qdbd_connection, "{}_out".format(table_name), create=True, infer_types=True
    )

    result = qdbpd.read_dataframe(
        qdbd_connection, qdbd_connection.table("{}_out".format(table_name))
    )

    _assert_df_equal(df_, result)


def test_regression_sc11084(qdbd_connection, table_name):
    """
    This test validates that, if one array starts with a significant amount of
    NULL values, it works properly. This addresses a bug that our "probe_mask" function
    inside our C++ masked_array implementation considers more than just the first chunk
    of 256 values.
    """

    n_rows = 8192
    start = np.datetime64("2022-09-08T12:00:00", "ns")
    idx = [start + np.timedelta64(i, "ns") for i in range(0, n_rows)]

    data1 = np.full(int(n_rows / 2), np.nan, dtype="float64")
    data2 = np.full(int(n_rows / 2), 1.11, dtype="float64")
    data = {"val": np.concatenate([data1, data2])}

    df = pd.DataFrame(data=data, index=idx)
    qdbpd.write_dataframe(
        df, qdbd_connection, table_name, create=True, infer_types=True
    )

    df_ = qdbpd.query(
        qdbd_connection,
        'select $timestamp, val from "{}"'.format(table_name),
        index="$timestamp",
    )

    _assert_df_equal(df, df_)


@pytest.mark.parametrize("sparsify", conftest.no_sparsify)
def test_regression_sc11337(
    qdbpd_write_fn, df_with_table, qdbd_connection, column_name
):
    (ctype, dtype, df1, table) = df_with_table
    assert column_name in df1.columns
    assert len(df1.columns) == 1

    df1 = df1.astype("object")

    df1[column_name][4] = pd.NA

    qdbpd_write_fn(
        df1,
        qdbd_connection,
        table.get_name(),
        push_mode=quasardb.WriterPushMode.Fast,
        infer_types=True,
    )

    df2 = qdbpd.read_dataframe(qdbd_connection, table, column_names=[column_name])

    _assert_df_equal(df1, df2)


def test_write_through_flag(qdbpd_write_fn, df_with_table, qdbd_connection):
    (_, _, df, table) = df_with_table

    qdbpd_write_fn(df, qdbd_connection, table, write_through=True)

    df2 = qdbpd.read_dataframe(qdbd_connection, table)

    _assert_df_equal(df, df2)


def test_write_through_flag_throws_when_incorrect(
    qdbpd_write_fn, df_with_table, qdbd_connection
):
    (_, _, df, table) = df_with_table

    with pytest.raises(quasardb.InvalidArgumentError):
        qdbpd_write_fn(df, qdbd_connection, table, write_through="wrong!")


def test_push_mode(qdbpd_write_fn, df_with_table, push_mode, qdbd_connection, caplog):
    (_, _, df, table) = df_with_table

    expected = "transactional push mode"
    kwargs = {}
    if push_mode == quasardb.WriterPushMode.Fast:
        expected = "fast push mode"
        kwargs["fast"] = True
    elif push_mode == quasardb.WriterPushMode.Async:
        expected = "async push mode"
        kwargs["_async"] = True

    caplog.clear()
    logger_name = "quasardb.writer"
    if qdbpd_write_fn is conftest._write_dataframe_arrow:
        logger_name = "quasardb.batch_push_arrow"

    caplog.set_level(logging.DEBUG, logger=logger_name)
    qdbpd_write_fn(df, qdbd_connection, table, **kwargs)

    assert any(expected in x.message for x in caplog.records)


def test_retries(
    qdbpd_write_fn,
    df_with_table,
    qdbd_connection,
    retry_options,
    mock_failure_options,
    caplog,
):
    if qdbpd_write_fn is conftest._write_dataframe_arrow:
        pytest.skip("Arrow writer does not support retries")

    caplog.set_level(logging.INFO)

    (_, _, df, table) = df_with_table

    do_write_fn = lambda: qdbpd_write_fn(
        df,
        qdbd_connection,
        table,
        retries=retry_options,
        mock_failure_options=mock_failure_options,
    )

    retries_expected = mock_failure_options.failures_left
    expect_failure = False

    if retries_expected > retry_options.retries_left:
        retries_expected = retry_options.retries_left
        expect_failure = True
        logger.info(
            "mock failures(%d) > retries(%d), expecting failure",
            mock_failure_options.failures_left,
            retry_options.retries_left,
        )

    logger.info("expected retries: %d", retries_expected)

    if expect_failure == True:
        with pytest.raises(quasardb.AsyncPipelineFullError):
            do_write_fn()
    else:
        do_write_fn()

    # Verify retry count is exactly as expected by inspecting logs.
    retries_seen = 0

    for lr in caplog.records:
        if "Retrying push operation" in lr.message:
            retries_seen = retries_seen + 1

    assert retries_seen == retries_expected

    # Verify the amount of milliseconds we "sleep"
    import datetime

    delays_ms = []

    # We always log in milliseconds (because we use std::chrono::millis), and the recommended
    # way to convert a datetime to milliseconds is to divide it by a 1ms timedelta.
    next_delay_ms = int(retry_options.delay / datetime.timedelta(milliseconds=1))
    for i in range(retries_expected):
        delays_ms.append(next_delay_ms)

        # We could use the permutation .next() function of retry_options instead, but then if
        # there would be a bug in there we would not detect it. And this isn't really rocket
        # science.
        next_delay_ms = next_delay_ms * retry_options.exponent

    # Now, verify we have seen each and every one of these delays
    for delay_ms in delays_ms:
        seen = False
        needle = "Sleeping for {} milliseconds".format(delay_ms)

        for lr in caplog.records:
            if needle in lr.message:
                seen = True
                break

        if seen == False:
            logger.error(
                "expected to find delay of %d milliseconds, but could not find in logs",
                delay_ms,
            )

        assert seen == True


def test_read_dataframe_empty_table_sc16881(qdbd_connection, table_name):
    """
    Ensures qdbpd.read_dataframe returns an empty DataFrame when the table exists but contains no data.

    Previously this raised ValueError due to pd.concat() on an empty result set.
    """
    table = qdbd_connection.ts(table_name)

    table_config = [
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "d"),
    ]

    table.create(table_config)

    df = qdbpd.read_dataframe(qdbd_connection, table)

    assert df.empty
