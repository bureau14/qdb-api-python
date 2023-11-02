# pylint: disable=C0103,C0111,C0302,W0212

import quasardb
import pytest


def test_query_max_length(qdbd_connection):
    qdbd_connection.options().set_max_query_length(1234)
    assert qdbd_connection.options().get_max_query_length() == 1234
