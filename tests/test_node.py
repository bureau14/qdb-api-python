import pytest
import quasardb


def test_node_wrong_leading_qdb_in_uri(qdbd_connection, qdbd_settings):
    with pytest.raises(quasardb.Error, match=r'The (handle|argument) is invalid'):
        # With check in connect, node() throws.
        direct_conn = qdbd_connection.node(
            qdbd_settings.get("uri").get("insecure"))
        # Without it, C API should return an error in prefix_get.
        direct_conn.prefix_get("$qdb", 10)


def test_node_connect_throws_connection_error_when_no_cluster_public_key(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"))


def test_node_connect_throws_connection_error_when_no_user_name(qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))


def test_node_connect_throws_connection_error_when_no_user_private_key(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            user_name=qdbd_settings.get("security").get("user_name"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))


def test_node_connect_ok_to_secure_cluster(qdbd_settings):
    quasardb.Node(
        uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
        user_name=qdbd_settings.get("security").get("user_name"),
        user_private_key=qdbd_settings.get("security").get("user_private_key"),
        cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))


def test_node_connect_throws_connection_error_when_no_cluster_public_key_file(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            user_security_file=qdbd_settings.get("security").get("user_private_key_file"))


def test_node_connect_throws_connection_error_when_no_user_security_file(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_node_connect_throws_connection_error_when_mix_security_1(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_node_connect_throws_connection_error_when_mix_security_2(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            user_security_file=qdbd_settings.get("security").get("user_private_key_file"),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_node_connect_throws_connection_error_when_mix_security_full(
        qdbd_settings):
    with pytest.raises(quasardb.Error):
        quasardb.Node(
            uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
            user_name=qdbd_settings.get("security").get("user_name"),
            user_private_key=qdbd_settings.get("security").get("user_private_key"),
            cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"),
            user_security_file=qdbd_settings.get("security").get("user_private_key_file"),
            cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


def test_node_connect_ok_to_secure_cluster_with_file(qdbd_settings):
    quasardb.Node(
        uri=qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
        user_security_file=qdbd_settings.get("security").get("user_private_key_file"),
        cluster_public_key_file=qdbd_settings.get("security").get("cluster_public_key_file"))


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
