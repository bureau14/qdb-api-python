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
    assert len(res) == 20


def test_node_blob_get(qdbd_direct_connection):
    entry = qdbd_direct_connection.blob("$qdb.statistics.node_id")
    got = entry.get()
    assert isinstance(got, bytes) and len(got) > 0


def test_node_integer_get(qdbd_direct_connection):
    entry = qdbd_direct_connection.integer("$qdb.statistics.cpu.system")
    got = entry.get()

    assert isinstance(got, int) and got > 0
