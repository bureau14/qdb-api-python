import pytest
import quasardb

def test_node_empty_prefix(qdbd_direct_connection):
    res = qdbd_direct_connection.prefix_get("testazeazeaze", 10)
    assert len(res) == 0

def test_node_find_prefixes(qdbd_direct_connection):
    res = qdbd_direct_connection.prefix_get("$qdb.statistics.", 20)
    assert len(res) == 20

def test_node_blob_get(qdbd_direct_connection):
    entry = qdbd_direct_connection.blob("$qdb.statistics.node_id")  
    got = entry.get()
    assert type(got) is bytes and len(got) > 0

def test_node_integer_get(qdbd_direct_connection):
    entry = qdbd_direct_connection.integer("$qdb.statistics.cpu.system")
    got = entry.get()

    assert type(got) is int and got > 0 