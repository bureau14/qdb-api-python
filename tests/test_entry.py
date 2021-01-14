# pylint: disable=C0103,C0111,C0302,W0212
import quasardb


def test_get_name(qdbd_connection, entry_name):
    blob = qdbd_connection.blob(entry_name)
    assert entry_name == blob.get_name()

    integer = qdbd_connection.integer(entry_name)
    assert entry_name == integer.get_name()
