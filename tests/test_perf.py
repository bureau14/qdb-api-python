import pytest
import quasardb


def test_perf_disable(qdbd_connection):
    qdbd_connection.perf().disable()


def test_perf_enable(qdbd_connection):
    qdbd_connection.perf().enable()


def test_perf_enable_fail_if_performance_is_not_enabled(qdbd_settings):
    cluster = quasardb.Cluster(
        uri=qdbd_settings.get("uri").get("secure"),
        user_name=qdbd_settings.get("security").get("user_name"),
        user_private_key=qdbd_settings.get("security").get("user_private_key"),
        cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))
    with pytest.raises(quasardb.Error):
        cluster.perf().enable()


def test_perf_get__when_empty(qdbd_connection):
    profiles = qdbd_connection.perf().get()
    assert len(profiles) == 0


def test_perf_get_after_blob_put(qdbd_connection, entry_name, random_string):
    qdbd_connection.perf().enable()
    qdbd_connection.blob(entry_name).put(random_string)
    qdbd_connection.blob(entry_name).get()

    profiles = qdbd_connection.perf().get()
    assert len(profiles) == 2
    assert profiles[0][0] == "blob.put"
    assert profiles[1][0] == "common.get"


def test_perf_clear_after_blob_put(qdbd_connection, entry_name, random_string):
    qdbd_connection.perf().enable()
    qdbd_connection.blob(entry_name).put(random_string)

    profiles = qdbd_connection.perf().get()
    assert len(profiles) != 0

    qdbd_connection.perf().clear()
    profiles = qdbd_connection.perf().get()
    assert len(profiles) == 0
