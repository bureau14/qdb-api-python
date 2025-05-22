import pytest
import quasardb.pandas as qdbpd
import quasardb.dask as qdbdsk
import numpy.ma as ma
import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test-dask")

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


def _prepare_query_test(qdbpd_write_fn, df_with_table, qdbd_connection, columns: str = "*", use_tag: bool=False, query_range: tuple[pd.Timestamp, pd.Timestamp]=None, group_by: str=None):
    (_, _, df, table) = df_with_table

    qdbpd_write_fn(df, qdbd_connection, table, write_through=True)
    table_name = table.get_name()
    q = "SELECT {} ".format(columns)

    if use_tag:
        table.attach_tag("dask_tag")
        q += "FROM find(tag='dask_tag')"
    else:
        q += "FROM \"{}\"".format(table_name)
    
    if query_range:
        q += " IN RANGE({}, {})".format(query_range[0], query_range[1])
    
    if group_by:
        q += " GROUP BY {}".format(group_by)

    return (df, table, q)

### Query tests, we care about results of dask matching those of pandas

@pytest.mark.parametrize("frequency", ["H"], ids=["frequency=H"], indirect=True)
def test_dask_df_select_star_equals_pandas_df(qdbpd_write_fn, df_with_table, qdbd_connection, qdbd_settings):
    df, table, query = _prepare_query_test(qdbpd_write_fn, df_with_table, qdbd_connection, "*")

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure")).compute()

    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)

@pytest.mark.parametrize("frequency", ["H"], ids=["frequency=H"], indirect=True)
@pytest.mark.parametrize("query_range_percentile", [1, 0.5, 0.25, 0.1])
def test_dask_df_select_star_in_range(qdbpd_write_fn, df_with_table, qdbd_connection, qdbd_settings, query_range_percentile):
    _, _, df, _ = df_with_table

    start_str = df.index[0].to_pydatetime().strftime("%Y-%m-%dT%H:%M:%S.%f")
    end_row = int((len(df)-1) * query_range_percentile)
    end_str = (df.index[end_row].to_pydatetime() + pd.Timedelta(microseconds=1)).strftime("%Y-%m-%dT%H:%M:%S.%f")

    df, _, query = _prepare_query_test(qdbpd_write_fn, df_with_table, qdbd_connection, "*", query_range=(start_str, end_str))

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure")).compute()
    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)

@pytest.mark.parametrize("frequency", ["H"], ids=["frequency=H"], indirect=True)
def test_dask_df_select_columns_equals_pandas_df(qdbpd_write_fn, df_with_table, qdbd_connection, qdbd_settings):
    _, _, df, _ = df_with_table
    columns = ", ".join([f"{col}" for col in df.columns])
    df, _, query = _prepare_query_test(qdbpd_write_fn, df_with_table, qdbd_connection, columns)

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure")).compute()
    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)

def test_dask_df_select_columns_with_alias_equals_pandas_df(qdbpd_write_fn, df_with_table, qdbd_connection, qdbd_settings):
    _, _, df, _ = df_with_table
    columns = ", ".join([f"{col} as alias_{col}" for col in df.columns])
    df, _, query = _prepare_query_test(qdbpd_write_fn, df_with_table, qdbd_connection, columns)

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure")).compute()
    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)


@pytest.mark.parametrize("frequency", ["H"], ids=["frequency=H"], indirect=True)
@pytest.mark.parametrize("group_by", ["1h", "1d"])
def test_dask_df_select_agg_group_by_time_equals_pandas_df(qdbpd_write_fn, df_with_table, qdbd_connection, qdbd_settings, group_by):
    _, _, df, _ = df_with_table
    columns = ", ".join([f"count({col})" for col in df.columns])
    df, _, query = _prepare_query_test(qdbpd_write_fn, df_with_table, qdbd_connection, columns=f"$timestamp, {columns}", group_by=group_by)

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure")).compute()
    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)


@pytest.mark.parametrize("frequency", ["H"], ids=["frequency=H"], indirect=True)
@pytest.mark.skip(reason="Not implemented yet")
def test_dask_df_select_find_tag_equals_pandas_df(qdbpd_write_fn, df_with_table, qdbd_connection, qdbd_settings):
    _, _, df, _ = df_with_table

    df, _, query = _prepare_query_test(qdbpd_write_fn, df_with_table, qdbd_connection, "*", use_tag=True)

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, qdbd_settings.get("uri").get("insecure")).compute()
    dask_df = dask_df.reset_index(drop=True)

    _assert_df_equal(pandas_df, dask_df)

@pytest.mark.parametrize(
        "query", [
            "INSERT INTO test ($timestamp, x) VALUES (now(), 2)",
            "DROP TABLE test",
            "DELETE FROM test",
            "CREATE TABLE test (x INT64)",
            "SHOW TABLE test",
            "ALTER TABLE test ADD COLUMN y INT64",
            "SHOW DISK USAGE ON test"
        ]
)
def test_dask_exception_on_non_select_query(qdbd_settings, query):
    """
    Tests that a non-select query raises an exception
    """
    with pytest.raises(NotImplementedError):
        qdbdsk.query(query, qdbd_settings.get("uri").get("insecure"))
