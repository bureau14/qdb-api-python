# pylint: disable=C0103,C0111,C0302,W0212
import unittest
import pytest

import quasardb


def test_connect_with_trace():
    with pytest.raises(quasardb.Error, match=r"at qdb_connect"):
        quasardb.Cluster(uri='invalid_uri')
