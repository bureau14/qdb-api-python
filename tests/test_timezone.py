# pylint: disable=C0103,C0111,C0302,W0212

import quasardb
import quasardb.pandas as qdbpd
import pytest

def test_set_timezone(qdbd_connection):
    qdbd_connection.options().set_timezone("Europe/Brussels")

def test_get_timezone(qdbd_connection):
    # We use Brussels, because on Windows the behavior is that timezones get "normalized" to
    # a single city if there are multiple cities that share the same properties.
    #
    # For example, in this case, Amsterdam and Brussels are 100% identical, and as such on
    # Windows it would just be stored as Brussels.
    qdbd_connection.options().set_timezone("Europe/Brussels")

    assert qdbd_connection.options().get_timezone() == "Europe/Brussels"

def test_query_timezone(qdbpd_write_fn, df_with_table, qdbd_connection):
    """
    This verifies data inserted (assumed to be UTC) is actually properly adjusted in query results.
    """

    (ctype, dtype, df, table) = df_with_table

    # We always need to infer
    qdbpd_write_fn(df, qdbd_connection, table, infer_types=True)


    q = "SELECT * FROM \"{}\"".format(table.get_name())

    # First select as UTC
    qdbd_connection.options().set_timezone("UTC")
    res1 = qdbpd.query(qdbd_connection, q)

    # First select as Europe/Amsterdam, which is always guaranteed to be 1 or 2 hours ahead of UTC
    qdbd_connection.options().set_timezone("Europe/Amsterdam")
    res2 = qdbpd.query(qdbd_connection, q)

    # Now use Bangkok, which again, is guaranteed to be several hours ahead of Amsterdam
    qdbd_connection.options().set_timezone("Asia/Bangkok")
    res3 = qdbpd.query(qdbd_connection, q)

    assert len(res1["$timestamp"]) == len(res2["$timestamp"])
    assert len(res1["$timestamp"]) == len(res3["$timestamp"])

    # We just ensure that UTC < Amsterdam time, Amsterdam < Bangkok time. We don't
    # do actual comparisons
    assert (res1["$timestamp"].to_numpy() < res2["$timestamp"].to_numpy()).all()
    assert (res2["$timestamp"].to_numpy() < res3["$timestamp"].to_numpy()).all()
