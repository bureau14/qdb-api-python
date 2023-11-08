# pylint: disable=C0103,C0111,C0302,W0212

import quasardb
import quasardb.pandas as qdbpd

from quasardb import metrics
import pytest

def test_clear_metrics(qdbd_connection):
    metrics.clear()

    totals = metrics.totals()
    assert len(totals) == 0

def test_scoped_measure(qdbd_connection):

    with metrics.Measure() as measure:
        scoped = measure.get()
        assert len(scoped) == 0


def test_batch_push_metrics(qdbpd_write_fn, df_with_table, qdbd_connection):
    (_, _, df, table) = df_with_table

    with metrics.Measure() as measure:
        assert len(measure.get()) == 0
        qdbpd_write_fn(df, qdbd_connection, table)

        m = measure.get()
        assert len(m) > 0

        assert "qdb_batch_push" in m
        assert m['qdb_batch_push'] > 0

def test_query_metrics(qdbpd_write_fn, df_with_table, qdbd_connection):
    (_, _, df, table) = df_with_table
    qdbpd_write_fn(df, qdbd_connection, table)

    with metrics.Measure() as measure:
        assert len(measure.get()) == 0

        df = qdbpd.query(qdbd_connection, "select * from \"{}\"".format(table.get_name()))

        m = measure.get()
        assert len(m) > 0

        assert "qdb_query" in m
        assert m['qdb_query'] > 0

def test_qdb_connect_close_metrics(qdbd_settings):

    # Insecure connection
    with metrics.Measure() as measure:
        assert len(measure.get()) == 0

        conn = quasardb.Cluster(qdbd_settings.get("uri").get("insecure"))

        m = measure.get()
        assert len(m) == 1
        assert "qdb_connect" in m
        assert m["qdb_connect"] > 0

        conn.close()

        m = measure.get()

        assert len(m) == 2
        assert "qdb_close" in m
        assert m["qdb_close"] > 0


    # Secure connection
    with metrics.Measure() as measure:
        assert len(measure.get()) == 0

        conn = quasardb.Cluster(uri=qdbd_settings.get("uri").get("secure"),
                                user_name=qdbd_settings.get("security").get("user_name"),
                                user_private_key=qdbd_settings.get("security").get("user_private_key"),
                                cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))

        m = measure.get()
        assert len(m) == 1
        assert "qdb_connect" in m
        assert m["qdb_connect"] > 0

        conn.close()

        m = measure.get()

        assert len(m) == 2
        assert "qdb_close" in m
        assert m["qdb_close"] > 0


def test_qdb_ts_list_columns_metrics(qdbd_connection, table):

    with metrics.Measure() as measure:
        assert len(measure.get()) == 0

        table2 = qdbd_connection.table(table.get_name())

        m = measure.get()
        assert len(m) == 1

        assert "qdb_ts_list_columns" in m
        assert m["qdb_ts_list_columns"] > 0
