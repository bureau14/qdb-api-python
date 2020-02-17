# pylint: disable=C0103,C0111,C0302,W0212
import unittest
import pytest

import quasardb

def test_connect_with_trace():
    with pytest.raises(quasardb.Error, match=r"at qdb_connect"):
        quasardb.Cluster(uri='invalid_uri')

def test_query_with_trace(qdbd_connection, table, column_name):
    with pytest.raises(quasardb.Error, match=r"at qdb_query"):
        qdbd_connection.query("select notexists from " + table.get_name() + " in range (1990, +40y)")
