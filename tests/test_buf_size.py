# pylint: disable=C0103,C0111,C0302,W0212

import conftest
import quasardb
import pytest

import test_batch_inserter as batchlib


# Don't use `qdbd_connection` fixture, as it's module-scoped (for perf reasons)
# and our options modifications would persist across tests.
#
# XXX(leon): is there an easy way to tell pytest, "please use function-scope
#            for these specific tests" ?
def test_set_client_max_in_buf_1(qdbd_settings):
    with conftest.create_qdbd_connection(qdbd_settings) as conn:
        with pytest.raises(quasardb.InvalidArgumentError):
            conn.options().set_client_max_in_buf_size(1)


# Don't use `qdbd_connection` fixture, as it's module-scoped (for perf reasons)
# and our options modifications would persist across tests.
#
# XXX(leon): is there an easy way to tell pytest, "please use function-scope
#            for these specific tests" ?
def test_set_client_max_in_buf_1MB(qdbd_settings):
    with conftest.create_qdbd_connection(qdbd_settings) as conn:
        conn.options().set_client_max_in_buf_size(1024 * 1024)
        assert conn.options().get_client_max_in_buf_size() == (1024 * 1024)


def test_get_client_max_in_buf(qdbd_connection):
    assert qdbd_connection.options().get_client_max_in_buf_size() > 0


def test_get_cluster_max_in_buf(qdbd_connection):
    assert qdbd_connection.options().get_cluster_max_in_buf_size() > 0


# Don't use `qdbd_connection` fixture, as it's module-scoped (for perf reasons)
# and our options modifications would persist across tests.
#
# XXX(leon): is there an easy way to tell pytest, "please use function-scope
#            for these specific tests" ?
def test_client_query_buf_size_error(qdbd_settings, table, many_intervals):
    with conftest.create_qdbd_connection(qdbd_settings) as conn:
        # First insert some data
        inserter = conn.inserter(
            batchlib._make_inserter_info(table))

        # doubles, blobs, strings, integers, timestamps =
        # batchlib._test_with_table(
        batchlib._test_with_table(
            inserter,
            table,
            many_intervals,
            batchlib._regular_push)

        res = conn.query("select * from \"" + table.get_name() + "\"")

        assert len(res) == 10000

        conn.options().set_client_max_in_buf_size(1500)

        with pytest.raises(quasardb.InputBufferTooSmallError, match=r'consider increasing the buffer size'):
            conn.query("select * from \"" + table.get_name() + "\"")
