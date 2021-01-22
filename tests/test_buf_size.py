# pylint: disable=C0103,C0111,C0302,W0212

import quasardb
import pytest

import test_batch_inserter as batchlib


def test_set_client_max_in_buf_1(qdbd_connection):
    with pytest.raises(quasardb.Error):
        qdbd_connection.options().set_client_max_in_buf_size(1)


def test_set_client_max_in_buf_1MB(qdbd_connection):
    qdbd_connection.options().set_client_max_in_buf_size(1024 * 1024)
    assert qdbd_connection.options().get_client_max_in_buf_size() == (1024 * 1024)


def test_get_client_max_in_buf(qdbd_connection):
    assert qdbd_connection.options().get_client_max_in_buf_size() > 0


def test_get_cluster_max_in_buf(qdbd_connection):
    assert qdbd_connection.options().get_cluster_max_in_buf_size() > 0


def test_client_query_buf_size_error(qdbd_connection, table, many_intervals):
    # First insert some data
    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps =
    # batchlib._test_with_table(
    batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    res = qdbd_connection.query("select * from \"" + table.get_name() + "\"")

    assert len(res) == 10000

    qdbd_connection.options().set_client_max_in_buf_size(1500)

    with pytest.raises(quasardb.InputBufferTooSmallError, match=r'consider increasing the buffer size'):
        qdbd_connection.query("select * from \"" + table.get_name() + "\"")
