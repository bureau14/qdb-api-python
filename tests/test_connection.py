# pylint: disable=C0103,C0111,C0302,W0212
import unittest
import pytest

import quasardb

def test_connect_throws_input_error__when_uri_is_invalid():
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(uri='invalid_uri')

def test_connect_throws_connection_error_when_no_cluster_on_given_uri():
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(uri='qdb://127.0.0.1:1')

def test_connect_throws_connection_error_when_no_cluster_public_key(qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(uri=qdbd_settings.get("uri").get("secure"),
                         user_name=qdbd_settings.get("security").get("user_name"),
                         user_private_key=qdbd_settings.get("security").get("user_private_key"))

def test_connect_throws_connection_error_when_no_user_name(qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(uri=qdbd_settings.get("uri").get("secure"),
                         user_private_key=qdbd_settings.get("security").get("user_private_key"),
                         cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))

def test_connect_throws_connection_error_when_no_user_private_key(qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Cluster(uri=qdbd_settings.get("uri").get("secure"),
                         user_name=qdbd_settings.get("security").get("user_name"),
                         cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))


def test_connect_ok_to_secure_cluster(qdbd_settings):
    quasardb.Cluster(uri=qdbd_settings.get("uri").get("secure"),
                     user_name=qdbd_settings.get("security").get("user_name"),
                     user_private_key=qdbd_settings.get("security").get("user_private_key"),
                     cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))

    # def test_connect_throws_connection_error__when_no_user_name(self):
    #     self.assertRaises(quasardb.Error,
    #                       quasardb.Cluster, uri=settings.SECURE_URI,
    #                       user_private_key=settings.SECURE_USER_PRIVATE_KEY,
    #                       cluster_public_key=settings.SECURE_CLUSTER_PUBLIC_KEY)

    # def test_connect_ok__secure_cluster(self):
    #     try:
    #         quasardb.Cluster(uri=settings.SECURE_URI,
    #                          user_name=settings.SECURE_USER_NAME,
    #                          user_private_key=settings.SECURE_USER_PRIVATE_KEY,
    #                          cluster_public_key=settings.SECURE_CLUSTER_PUBLIC_KEY)
    #     except Exception as ex:  # pylint: disable=W0703
    #         self.fail(msg='Cannot connect to secure settings.cluster: ' + str(ex))
