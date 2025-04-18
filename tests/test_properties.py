# pylint: disable=C0103,C0111,C0302,W0212,W0702

import pytest
import conftest
import numpy as np

import logging

import quasardb
import quasardb.pandas as qdbpd

logger = logging.getLogger("test-user-properties")


def test_properties_enabled_by_default(qdbd_connection, random_identifier):
    """
    Validates that properties are enabled by defaylt by getting a non-existing key and
    ensuring an exception is thrown.
    """
    assert qdbd_connection.properties().get(random_identifier) == None


def test_properties_get_none_when_not_found(qdbd_connection, random_identifier):
    assert qdbd_connection.properties().get(random_identifier) == None


def test_properties_put(qdbd_connection, random_identifier, random_string):
    qdbd_connection.properties().put(random_identifier, random_string)
    assert qdbd_connection.properties().get(random_identifier) == random_string


def test_properties_put_twice_raises_error(
    qdbd_connection, random_identifier, random_string
):
    qdbd_connection.properties().put(random_identifier, random_string)

    with pytest.raises(quasardb.AliasAlreadyExistsError):
        qdbd_connection.properties().put(random_identifier, random_string)


def test_properties_remove(qdbd_connection, random_identifier, random_string):
    qdbd_connection.properties().put(random_identifier, random_string)
    assert qdbd_connection.properties().get(random_identifier) == random_string

    qdbd_connection.properties().remove(random_identifier)
    assert qdbd_connection.properties().get(random_identifier) == None


def test_properties_clear(qdbd_connection, random_identifier, random_string):
    qdbd_connection.properties().put(random_identifier, random_string)
    assert qdbd_connection.properties().get(random_identifier) == random_string

    qdbd_connection.properties().clear()
    assert qdbd_connection.properties().get(random_identifier) == None


def _find_log_file():
    print
    import pathlib

    current_path = pathlib.Path(__file__).parent
    project_root = current_path.parent

    log_path = project_root.joinpath("insecure/log/0-0-0-1/qdbd.json")

    assert log_path.is_file() == True

    return log_path.resolve()


def _has_user_property_in_log_file(log_path, key, value):
    """
    Returns `True` if the user property is found in the logs
    """

    import json

    key = "client_{}".format(key)

    with open(log_path) as f:

        # Return true if there is at least one row with the correct key/value
        for line in f:
            try:
                d = json.loads(line)

                if key in d and d[key] == value:
                    logger.info("found key %s matching value %s", key, value)
                    return True

            except json.JSONDecodeError as e:
                logger.exception("Invalid JSON, skipping line")
                logger.warning("row: '%s'", line)
                pass

    return False


# Ensure only a single variation is executed:
#  - no null values / sparse data in table
#  - single data type
#  - no different ariations of row types
@conftest.override_sparsify("none")
@conftest.override_cdtypes(np.dtype("unicode"))
@conftest.override_row_count(224)
def test_properties_in_log(
    qdbpd_write_fn,
    qdbd_connection,
    df_with_table,
    table_name,
    column_name,
    random_identifier,
    random_string,
):
    """
    This test is a bit more involved, it will try to ensure that the user properties metadata is actually
    logged in the qdbd logs.

    To do this, it triggers a query with metadata attached, and polls the logs until it finds the metadata.
    This assumes the logs are written in JSON and the services are started using the start/stop services script,
    which is always the case in Teamcity. If this test fails, it may be because of this reason.
    """

    # XXX(leon): do we even need to write this at all? It doesn't appear that the test
    # even relies on this data being present?
    (_, _, df, table) = df_with_table
    qdbpd_write_fn(df, qdbd_connection, table, push_mode=quasardb.WriterPushMode.Fast)

    qdbd_connection.properties().clear()
    qdbd_connection.properties().put(random_identifier, random_string)

    # a faulty query will emit a log entry that will contain the user properties
    try:
        qdbpd.query(qdbd_connection, "PUT BLOB a 'a'")
        qdbpd.query(qdbd_connection, "PUT BLOB a 'a'")
    except:
        pass

    log_file = _find_log_file()

    # Now wait until we find the custom property somewhere in the log file

    import time

    start = time.time()
    found_properties = False

    # 1 minute timeout
    while time.time() - start < 60:
        if _has_user_property_in_log_file(log_file, random_identifier, random_string):
            logger.info("found user properties!")
            found_properties = True
            break

        logger.info("User properties not found, waiting 1 second..")
        time.sleep(1)

    assert found_properties == True
