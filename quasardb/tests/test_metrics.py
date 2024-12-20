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

        qdbpd.query(qdbd_connection, "select * from \"{}\"".format(table.get_name()))

        m = measure.get()
        assert len(m) > 0

        assert "qdb_query" in m
        assert m['qdb_query'] > 0

def test_accumulating_metrics_within_scope(qdbpd_write_fn, df_with_table, qdbd_connection):
    (_, _, df, table) = df_with_table
    qdbpd_write_fn(df, qdbd_connection, table)

    with metrics.Measure() as measure:
        assert len(measure.get()) == 0

        # First query
        qdbpd.query(qdbd_connection, "select * from \"{}\"".format(table.get_name()))

        m1 = measure.get()
        assert m1['qdb_query'] > 0

        # Second query
        qdbpd.query(qdbd_connection, "select * from \"{}\"".format(table.get_name()))
        m2 = measure.get()

        # The crux of the test, we expect this metric to accumulate
        assert m2['qdb_query'] > m1['qdb_query']
