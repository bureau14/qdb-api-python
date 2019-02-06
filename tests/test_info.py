# pylint: disable=C0103,C0111,C0302,W0212
import quasardb

def test_node_status(qdbd_connection, qdbd_settings):
    status = qdbd_connection.node_status(qdbd_settings.get('uri').get('insecure'))
    assert len(status) > 0
    assert status.get('overall') != None

def test_node_config(qdbd_connection, qdbd_settings):
    config = qdbd_connection.node_config(qdbd_settings.get('uri').get('insecure'))
    assert len(config) > 0
    assert config.get('global') != None
    assert config.get('local') != None

def test_node_topology(qdbd_connection, qdbd_settings):
    topology = qdbd_connection.node_topology(qdbd_settings.get('uri').get('insecure'))

    assert len(topology) > 0

    assert topology.get('predecessor') != None
    assert topology.get('center') != None
    assert topology.get('successor') != None