# pylint: disable=C0103,C0111,C0302,W0212

import quasardb
import pytest

import test_batch_inserter as batchlib


def test_get_client_max_parallelism(qdbd_connection):
    assert qdbd_connection.options().get_client_max_parallelism() > 0


def test_set_client_max_parallelism_negative(qdbd_connection):
    with pytest.raises(TypeError):
        qdbd_connection.options().set_client_max_parallelism(-1)


def test_set_client_max_parallelism_0(qdbd_connection):
    qdbd_connection.options().set_client_max_parallelism(0)
    assert qdbd_connection.options().get_client_max_parallelism() > 0


def test_set_client_max_parallelism_1(qdbd_connection):
    qdbd_connection.options().set_client_max_parallelism(1)
    assert qdbd_connection.options().get_client_max_parallelism() == 1


def test_get_client_max_parallelism_8(qdbd_connection):
    assert qdbd_connection.options().get_client_max_parallelism() == 1
