import pytest
import quasardb.pandas as qdbpd
import quasardb.dask as qdbdsk
import numpy.ma as ma
import logging
import numpy as np
import pandas as pd

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


def test_compare_pandas_dask_df(qdbpd_write_fn, df_with_table, qdbd_connection, qdbd_settings):
    (_, _, df, table) = df_with_table

    qdbpd_write_fn(df, qdbd_connection, table, write_through=True)

    table_name = table.get_name()
    
    query = f"SELECT * FROM \"{table_name}\""

    pandas_df = qdbpd.query(qdbd_connection, query)
    dask_df = qdbdsk.query(query, uri="qdb://127.0.0.1:2836").compute()

    _assert_df_equal(pandas_df, dask_df)
