# pylint: disable=C0103,C0111,C0302,W0212

import quasardb
import pytest


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
