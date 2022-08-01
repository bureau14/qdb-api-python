import pytest
import quasardb


def test_node_wrong_leading_qdb_in_uri(qdbd_connection, qdbd_settings):
    with pytest.raises(quasardb.Error, match=r'The (handle|argument) is invalid'):
        # With check in connect, node() throws.
        direct_conn = qdbd_connection.node(
            qdbd_settings.get("uri").get("insecure"))
        # Without it, C API should return an error in prefix_get.
        direct_conn.prefix_get("$qdb", 10)


def test_node_empty_prefix(qdbd_direct_connection):
    res = qdbd_direct_connection.prefix_get("testazeazeaze", 10)
    assert len(res) == 0


def test_node_find_prefixes(qdbd_direct_connection):
    res = qdbd_direct_connection.prefix_get("$qdb.statistics.", 20)
    assert len(res) > 0


def test_node_direct_integer(qdbd_direct_connection, entry_name, random_integer):
    entry = qdbd_direct_connection.integer(entry_name)

    with pytest.raises(quasardb.AliasNotFoundError):
        entry.get()

    entry.put(random_integer)
    assert entry.get() == random_integer

    entry.update(random_integer + 1)
    assert entry.get() == random_integer + 1

    entry.remove()

    with pytest.raises(quasardb.AliasNotFoundError):
        entry.get()


def test_node_direct_blob(qdbd_direct_connection, entry_name, random_blob):
    entry = qdbd_direct_connection.blob(entry_name)

    with pytest.raises(quasardb.AliasNotFoundError):
        entry.get()

    entry.put(random_blob)
    assert entry.get() == random_blob

    entry.update(random_blob + random_blob)
    assert entry.get() == random_blob + random_blob

    entry.remove()

    with pytest.raises(quasardb.AliasNotFoundError):
        entry.get()
