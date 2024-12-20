# pylint: disable=C0103,C0111,C0302,W0212

import pytest
import quasardb


def test_max_cardinality_ok(qdbd_connection):
    qdbd_connection.options().set_max_cardinality(140000)


def test_max_cardinality_throws_when_value_is_zero(qdbd_connection):
    with pytest.raises(quasardb.InvalidArgumentError):
        qdbd_connection.options().set_max_cardinality(0)


def test_max_cardinality_throws_when_value_is_negative(qdbd_connection):
    with pytest.raises(TypeError):
        qdbd_connection.options().set_max_cardinality(-143)
