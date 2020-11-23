import pytest
import quasardb


def test_perf_disable(qdbd_connection):
    qdbd_connection.perf().disable()


def test_perf_enable(qdbd_connection):
    qdbd_connection.perf().enable()

def test_perf_enable_secure_conn(qdbd_secure_connection):
    qdbd_secure_connection.perf().enable()

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
