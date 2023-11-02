# pylint: disable=C0103,C0111,C0302,W0212
import unittest
import pytest

import quasardb


def test_connect_throws_input_error__when_uri_is_invalid():
    with pytest.raises(quasardb.InvalidArgumentError):
        quasardb.Cluster(uri='invalid_uri')


def test_connect_throws_connection_error_when_no_cluster_on_given_uri():
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(uri='qdb://127.0.0.1:1')


def test_connect_throws_connection_error_when_no_cluster_public_key(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"))


def test_connect_throws_connection_error_when_no_user_name(qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))


def test_connect_throws_connection_error_when_no_user_private_key(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            user_name=qdbd_settings.get("security").get("user_name"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))


def test_connect_ok_to_secure_cluster(qdbd_settings):
    quasardb.Cluster(
        uri=qdbd_settings.get("uri").get("secure"),
        user_name=qdbd_settings.get("security").get("user_name"),
        user_private_key=qdbd_settings.get("security").get("user_private_key"),
        cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))


def test_connect_throws_connection_error_when_no_cluster_public_key_file(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            user_security_file=qdbd_settings.get("security").get("user_private_key_file"))


def test_connect_throws_connection_error_when_no_user_security_file(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_connect_throws_connection_error_when_mix_security_1(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_connect_throws_connection_error_when_mix_security_2(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            user_security_file=qdbd_settings.get("security").get("user_private_key_file"),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_connect_throws_connection_error_when_mix_security_full(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(
            uri=qdbd_settings.get("uri").get("secure"),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"),
            user_security_file=qdbd_settings.get("security").get("user_private_key_file"),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_connect_ok_to_secure_cluster_with_file(qdbd_settings):
    quasardb.Cluster(
        uri=qdbd_settings.get("uri").get("secure"),
        user_security_file=qdbd_settings.get("security").get("user_private_key_file"),
        cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_connect_with_open_to_secure_cluster(qdbd_settings):
    with quasardb.Cluster(uri=qdbd_settings.get("uri").get("insecure")) as conn:
        topology = conn.node_topology(
            qdbd_settings.get('uri').get('insecure'))

        assert len(topology) > 0

def test_connect_throws_exception_when_accessing_outside_with_open(qdbd_settings):
    with pytest.raises(quasardb.InvalidHandleError):
        with quasardb.Cluster('qdb://127.0.0.1:2836') as conn:
            pass
        output = conn.node_topology(qdbd_settings.get('uri').get('insecure'))
        print(output)
