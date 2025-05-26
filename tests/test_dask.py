import math
import pytest
import quasardb
import quasardb.pandas as qdbpd
import quasardb.dask as qdbdsk
import numpy.ma as ma
import logging
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import LocalCluster, Client
import conftest
from test_pandas import _assert_df_equal

logger = logging.getLogger("test-dask")


def _prepare_query_test(
    df_with_table,
    qdbd_connection,
    columns: str = "*",
    query_range: tuple[pd.Timestamp, pd.Timestamp] = None,
    group_by: str = None,
):
    (_, _, df, table) = df_with_table

    qdbpd.write_dataframe(df, qdbd_connection, table, write_through=True)
    table_name = table.get_name()
    q = 'SELECT {} FROM "{}"'.format(columns, table_name)

    if query_range:
        q += " IN RANGE({}, {})".format(query_range[0], query_range[1])

    if group_by:
        q += " GROUP BY {}".format(group_by)

    return (df, table, q)


def _get_subrange(
    df: pd.DataFrame, slice_size: int = 0.1
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Returns slice of the Dataframe index to be used in the query.
    """
    query_range = ()
    if slice_size != 1:
        start_str = df.index[0].to_pydatetime().strftime("%Y-%m-%dT%H:%M:%S.%f")
        end_row = int((len(df) - 1) * slice_size)
        end_str = df.index[end_row].to_pydatetime().strftime("%Y-%m-%dT%H:%M:%S.%f")
        query_range = (start_str, end_str)
    return query_range


#### Dask integration tests

@conftest.override_cdtypes([np.dtype("float64")])
@pytest.mark.parametrize("row_count", [224], ids=["row_count=224"], indirect=True)
@pytest.mark.parametrize("sparsify", [100], ids=["sparsify=none"], indirect=True)
def test_dask_query_meta_set(df_with_table, qdbd_connection, qdbd_settings):
    """
    tests that the columns are set correctly in the dask DataFrame
    """
    _, _, query = _prepare_query_test(df_with_table, qdbd_connection)
    df = qdbpd.query(qdbd_connection, query)
    ddf = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))

    dask_meta = ddf._meta_nonempty
    pandas_cols = df.columns

    assert dask_meta.columns.names == pandas_cols.names, "column names do not match"

    for col_name in pandas_cols:
        # treat string[pyarrow] as object
        if dask_meta[col_name].dtype == "string[pyarrow]":
            dask_meta[col_name] = dask_meta[col_name].astype("object")

        assert (
            dask_meta[col_name].dtype == df[col_name].dtype
        ), f"dtype of column {col_name} does not match"


@conftest.override_cdtypes([np.dtype("float64")])
@pytest.mark.parametrize("row_count", [224], ids=["row_count=224"], indirect=True)
@pytest.mark.parametrize("sparsify", [100], ids=["sparsify=none"], indirect=True)
def test_dask_query_lazy_evaluation(df_with_table, qdbd_connection, qdbd_settings):
    """
    tests that the function is lazy and does not return a Dataframe immediately.
    """

    _, _, query = _prepare_query_test(df_with_table, qdbd_connection)

    ddf = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))

    assert isinstance(ddf, dd.DataFrame)
    result = ddf.compute()
    assert isinstance(result, pd.DataFrame)


@conftest.override_cdtypes([np.dtype("float64")])
@pytest.mark.parametrize("row_count", [224], ids=["row_count=224"], indirect=True)
@pytest.mark.parametrize("sparsify", [100], ids=["sparsify=none"], indirect=True)
@pytest.mark.parametrize("frequency", ["h"], ids=["frequency=H"], indirect=True)
def test_dask_query_parallelized(df_with_table, qdbd_connection, qdbd_settings):
    _, _, df, table = df_with_table
    shard_size = table.get_shard_size()
    start, end = df.index[0], df.index[-1]

    _, _, query = _prepare_query_test(df_with_table, qdbd_connection)

    ddf = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))

    # value of npartitions determines number of delayed tasks
    # delayed tasks can be executed in parallel
    # currently tasks are created for each shard
    expected_number_of_partitions = math.ceil(
        (end - start).total_seconds() / shard_size.total_seconds()
    )
    assert ddf.npartitions == expected_number_of_partitions


@conftest.override_cdtypes([np.dtype("float64")])
@pytest.mark.parametrize("row_count", [224], ids=["row_count=224"], indirect=True)
@pytest.mark.parametrize("sparsify", [100], ids=["sparsify=none"], indirect=True)
def test_dask_compute_on_local_cluster(df_with_table, qdbd_connection, qdbd_settings):
    _, _, query = _prepare_query_test(df_with_table, qdbd_connection)

    with LocalCluster(n_workers=2) as cluster:
        with Client(cluster):
            ddf = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))
            res = ddf.compute()
            res.head()


### Query tests, we care about results of dask query matching those of pandas
# when using default index, it has to be reset to match pandas DataFrame.
# we neeed to check that each query is split into multiple dask partitions
#
# index for a Dask DataFrame will not be monotonically increasing from 0.
# Instead, it will restart at 0 for each partition (e.g. index1 = [0, ..., 10], index2 = [0, ...]).
# This is due to the inability to statically know the full length of the index.
# https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.reset_index.html


@pytest.mark.parametrize("frequency", ["h"], ids=["frequency=H"], indirect=True)
@pytest.mark.parametrize(
    "range_slice",
    [1, 0.5, 0.25],
    ids=["range_slice=1", "range_slice=0.5", "range_slice=0.25"],
)
@pytest.mark.parametrize(
    "query_options",
    [{"index": None}, {"index": "$timestamp"}],
    ids=["index=None", "index=$timestamp"],
)
def test_dask_df_select_star_equals_pandas_df(
    df_with_table, qdbd_connection, qdbd_settings, query_options, range_slice
):
    _, _, df, _ = df_with_table
    query_range = _get_subrange(df, range_slice)
    _, _, query = _prepare_query_test(df_with_table, qdbd_connection, "*", query_range)

    pandas_df = qdbpd.query(qdbd_connection, query, **query_options)
    dask_df = qdbdsk.query(
        query, qdbd_settings.get("uri").get("insecure"), **query_options
    )

    assert dask_df.npartitions > 1, "Dask DataFrame should have multiple partitions"
    dask_df = dask_df.compute()

    if query_options.get("index") is None:
        dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)


@pytest.mark.parametrize("frequency", ["h"], ids=["frequency=H"], indirect=True)
@pytest.mark.parametrize(
    "range_slice",
    [1, 0.5, 0.25],
    ids=["range_slice=1", "range_slice=0.5", "range_slice=0.25"],
)
@pytest.mark.parametrize(
    "use_alias", [False, True], ids=["use_alias=False", "use_alias=True"]
)
def test_dask_df_select_columns_equals_pandas_df(
    df_with_table, qdbd_connection, qdbd_settings, use_alias, range_slice
):
    _, _, df, _ = df_with_table

    columns = ", ".join(
        [f"{col} as {col}_alias" if use_alias else f"{col}" for col in df.columns]
    )

    query_range = _get_subrange(df, range_slice)
    _, _, query = _prepare_query_test(
        df_with_table, qdbd_connection, columns, query_range
    )
    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))

    assert dask_df.npartitions > 1, "Dask DataFrame should have multiple partitions"
    dask_df = dask_df.compute()

    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)


@pytest.mark.parametrize("frequency", ["h"], ids=["frequency=H"], indirect=True)
@pytest.mark.parametrize("group_by", ["1h", "1d"])
def test_dask_df_select_agg_group_by_time_equals_pandas_df(
    df_with_table, qdbd_connection, qdbd_settings, group_by
):
    _, _, df, _ = df_with_table
    columns = ", ".join([f"count({col})" for col in df.columns])
    df, _, query = _prepare_query_test(
        df_with_table,
        qdbd_connection,
        columns=f"$timestamp, {columns}",
        group_by=group_by,
    )

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))

    assert dask_df.npartitions > 1, "Dask DataFrame should have multiple partitions"
    dask_df = dask_df.compute()

    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)


@pytest.mark.parametrize(
    "query",
    [
        "INSERT INTO test ($timestamp, x) VALUES (now(), 2)",
        "DROP TABLE test",
        "DELETE FROM test",
        "CREATE TABLE test (x INT64)",
        "SHOW TABLE test",
        "ALTER TABLE test ADD COLUMN y INT64",
        "SHOW DISK USAGE ON test",
    ],
)
def test_dask_query_exception_on_non_select_query(qdbd_settings, query):
    """
    Tests that a non-select query raises an exception
    """
    with pytest.raises(NotImplementedError):
        qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))
