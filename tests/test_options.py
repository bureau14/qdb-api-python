# pylint: disable=C0103,C0111,C0302,W0212

import quasardb
import pytest


def test_get_client_max_parallelism(qdbd_connection):
    assert qdbd_connection.options().get_client_max_parallelism() > 0


def test_set_client_max_parallelism_negative(qdbd_settings):
    with pytest.raises(TypeError):
        quasardb.Cluster(
            uri=qdbd_settings["uri"]["insecure"], client_max_parallelism=-1
        )


def test_set_client_max_parallelism_0(qdbd_settings):
    conn = quasardb.Cluster(
        uri=qdbd_settings["uri"]["insecure"], client_max_parallelism=0
    )

    assert conn.options().get_client_max_parallelism() > 0


def test_set_client_max_parallelism_1(qdbd_settings):
    conn = quasardb.Cluster(
        uri=qdbd_settings["uri"]["insecure"], client_max_parallelism=1
    )
    assert conn.options().get_client_max_parallelism() == 1


def test_set_client_max_parallelism_8(qdbd_settings):
    conn = quasardb.Cluster(
        uri=qdbd_settings["uri"]["insecure"], client_max_parallelism=8
    )
    assert conn.options().get_client_max_parallelism() == 8


def test_get_query_max_length(qdbd_connection):
    assert qdbd_connection.options().get_query_max_length() >= 0


def test_set_query_max_length(qdbd_connection):
    qdbd_connection.options().set_query_max_length(1234)
    assert qdbd_connection.options().get_query_max_length() == 1234


def test_get_client_max_batch_load(qdbd_connection):
    assert qdbd_connection.options().get_client_max_batch_load() >= 0


def test_set_client_max_batch_load(qdbd_connection):
    # Make sure to remember the default value, as the connection may be reused
    old = qdbd_connection.options().get_client_max_batch_load()

    try:
        qdbd_connection.options().set_client_max_batch_load(1234)
        assert qdbd_connection.options().get_client_max_batch_load() == 1234
    finally:
        qdbd_connection.options().set_client_max_batch_load(old)


def test_get_connection_per_address_soft_limit(qdbd_connection):
    assert qdbd_connection.options().get_connection_per_address_soft_limit() >= 0


def test_set_connection_per_address_soft_limit(qdbd_connection):
    # Make sure to remember the default value, as the connection may be reused
    old = qdbd_connection.options().get_connection_per_address_soft_limit()

    # Just to make sure there isn't the coincidence that the default has been changed to
    # 42 and this whole test would become useless
    assert old != 42

    print("old: ", old)

    qdbd_connection.options().set_connection_per_address_soft_limit(42)
    assert qdbd_connection.options().get_connection_per_address_soft_limit() == 42
