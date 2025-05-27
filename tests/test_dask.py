import pytest
import quasardb.pandas as qdbpd
import quasardb.dask as qdbdsk
import logging

logger = logging.getLogger("test-dask")


def _prepare_query_test(
    df_with_table,
    qdbd_connection,
):
    (_, _, df, table) = df_with_table

    qdbpd.write_dataframe(df, qdbd_connection, table, write_through=True)
    return (df, table)


@pytest.mark.parametrize("frequency", ["h"], ids=["frequency=H"], indirect=True)
def test_can_query_from_dask_module(df_with_table, qdbd_connection, qdbd_settings):
    _, table = _prepare_query_test(df_with_table, qdbd_connection)
    query = f'SELECT * FROM "{table.get_name()}"'

    ddf = qdbdsk.query(query, cluster_uri=qdbd_settings.get("uri").get("insecure"))
    assert (
        ddf.npartitions > 1
    )  # we want to ensure that the query is distributed for this test
    ddf.compute()
