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
    qdbd_connection.properties().get(random_identifier) == None


def test_properties_put(qdbd_connection, random_identifier, random_string):
    qdbd_connection.options().enable_user_properties()

    qdbd_connection.properties().put(random_identifier, random_string)
    qdbd_connection.properties().get(random_identifier) == random_string
