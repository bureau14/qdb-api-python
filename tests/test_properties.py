# pylint: disable=C0103,C0111,C0302,W0212,W0702

import pytest
import quasardb

def test_properties_disabled_by_default(qdbd_connection, random_identifier):
    """
    Validates that properties are disabled by defalt by getting a non-existing key and
    ensuring an exception is thrown.
    """

    with pytest.raises(quasardb.Error):
        qdbd_connection.properties().get(random_identifier)

def test_properties_get_none_when_not_found(qdbd_connection, random_identifier):
    qdbd_connection.options().enable_user_properties()
    assert qdbd_connection.properties().get(random_identifier) == None


def test_properties_put(qdbd_connection, random_identifier, random_string):
    qdbd_connection.options().enable_user_properties()

    qdbd_connection.properties().put(random_identifier, random_string)
    assert qdbd_connection.properties().get(random_identifier) == random_string


def test_properties_put_twice_raises_error(qdbd_connection, random_identifier, random_string):
    qdbd_connection.options().enable_user_properties()

    qdbd_connection.properties().put(random_identifier, random_string)

    with pytest.raises(quasardb.AliasAlreadyExistsError):
        qdbd_connection.properties().put(random_identifier, random_string)


def test_properties_remove(qdbd_connection, random_identifier, random_string):
    qdbd_connection.options().enable_user_properties()

    qdbd_connection.properties().put(random_identifier, random_string)
    assert qdbd_connection.properties().get(random_identifier) == random_string

    qdbd_connection.properties().remove(random_identifier)
    assert qdbd_connection.properties().get(random_identifier) == None


def test_properties_clear(qdbd_connection, random_identifier, random_string):
    qdbd_connection.options().enable_user_properties()

    qdbd_connection.properties().put(random_identifier, random_string)
    assert qdbd_connection.properties().get(random_identifier) == random_string

    qdbd_connection.properties().clear()
    assert qdbd_connection.properties().get(random_identifier) == None
