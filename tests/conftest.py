# pylint: disable=C0103,C0111,C0302,W0212
import json
import random
import string
import pytest
import numpy as np

import quasardb


def connect(uri):
    return quasardb.Cluster(uri)

def direct_connect(conn, node_uri):
    return conn.node(node_uri)

def config():
    return {"uri":
            {"insecure": "qdb://127.0.0.1:2836",
             "secure": "qdb://127.0.0.1:2838"}}


@pytest.fixture
def qdbd_settings(scope="module"):
    user_key = {}
    cluster_key = ""

    with open('user_private.key', 'r') as user_key_file:
        user_key = json.load(user_key_file)
    with open('cluster_public.key', 'r') as cluster_key_file:
        cluster_key = cluster_key_file.read()
    return {
        "uri": {
            "insecure": "qdb://127.0.0.1:2836",
            "secure": "qdb://127.0.0.1:2838"},
        "security": {
            "user_name": user_key['username'],
            "user_private_key": user_key['secret_key'],
            "cluster_public_key": cluster_key}}


@pytest.fixture
def qdbd_connection(qdbd_settings):
    return connect(qdbd_settings.get("uri").get("insecure"))

@pytest.fixture
def qdbd_direct_connection(qdbd_settings, qdbd_connection):
    return direct_connect(qdbd_connection, qdbd_settings.get("uri").get("insecure").replace("qdb://", ""))

@pytest.fixture
def random_identifier():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(16))


@pytest.fixture
def random_string():
    return ''.join(
        random.choice(
            string.ascii_uppercase +
            string.digits) for _ in range(16))


@pytest.fixture
def column_name(random_identifier):
    return random_identifier


@pytest.fixture
def tag_name(random_identifier):
    return random_identifier


@pytest.fixture
def tag_names():
    return sorted([''.join(random.choice(string.ascii_lowercase)
                           for _ in range(16)) for _ in range(10)])


@pytest.fixture
def entry_name(random_string):
    return random_string


@pytest.fixture
def random_blob(random_string):
    return random_string.encode('UTF-8')


@pytest.fixture
def random_integer():
    return random.randint(-1000000000, 1000000000)


@pytest.fixture
def blob_entry(qdbd_connection, entry_name):
    return qdbd_connection.blob(entry_name)


@pytest.fixture
def integer_entry(qdbd_connection, entry_name):
    return qdbd_connection.integer(entry_name)


@pytest.fixture
def table(qdbd_connection, entry_name):

    ts = qdbd_connection.ts(entry_name)
    double_col = quasardb.ColumnInfo(quasardb.ColumnType.Double, "the_double")
    blob_col = quasardb.ColumnInfo(quasardb.ColumnType.Blob, "the_blob")
    string_col = quasardb.ColumnInfo(quasardb.ColumnType.String, "the_string")
    int64_col = quasardb.ColumnInfo(quasardb.ColumnType.Int64, "the_int64")
    ts_col = quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, "the_ts")

    ts.create([double_col, blob_col, string_col, int64_col, ts_col])
    return ts


@pytest.fixture
def intervals():
    start_time = np.datetime64('2017-01-01', 'ns')
    return [(start_time, start_time + np.timedelta64(1, 's'))]


@pytest.fixture
def many_intervals():
    start_time = np.datetime64('2017-01-01', 'ns')
    return np.array([(start_time + np.timedelta64(i, 's'))
                     for i in range(10000)]).astype('datetime64[ns]')
