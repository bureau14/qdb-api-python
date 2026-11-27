"""Real service tests for transport dispatch and REST authentication/lifecycle."""

import json

import httpx
import numpy as np
import pytest
import quasardb


@pytest.mark.parametrize(
    "constructor,uri",
    [
        ("Cluster", "https://[::1"),
        ("Cluster", "http://localhost:bad-port/"),
        ("RestCluster", "https://[::1"),
        ("RestCluster", "http://localhost:bad-port/"),
        ("RestCluster", "ftp://localhost"),
    ],
)
def test_invalid_uri(constructor, uri):
    with pytest.raises(quasardb.InvalidArgumentError):
        getattr(quasardb, constructor)(uri)


def _answer(conn):
    with conn.stream_query("SELECT 42 AS answer", batch_size=1) as stream:
        return list(stream)[0]["answer"].tolist()


def test_dispatch_and_concrete_construction(cluster, cluster_settings):
    settings = cluster_settings.copy()
    uri = settings.pop("uri")
    concrete = (
        quasardb.RestCluster if uri.startswith("http") else quasardb.quasardb.Cluster
    )
    assert isinstance(cluster, concrete)
    if uri.startswith("http"):
        assert not hasattr(cluster, "table")
    assert isinstance(cluster, quasardb.Cluster) == (
        concrete is quasardb.quasardb.Cluster
    )
    assert _answer(cluster) == [42]
    with quasardb.Cluster(uri, **settings) as dispatched:
        assert type(dispatched) is type(cluster)
    with concrete(uri, **settings) as conn:
        assert conn.uri() == uri
        assert _answer(conn) == [42]
    assert not conn.is_open()
    conn.close()


def test_native_subclass(qdbd_settings):
    class NamedCluster(quasardb.Cluster):
        def __init__(self, label):
            super().__init__(qdbd_settings["uri"]["insecure"])
            self.label = label

    with NamedCluster("http://label") as conn:
        assert conn.label == "http://label"


def test_native_rejects_rest_option(qdbd_settings):
    with pytest.raises(TypeError):
        quasardb.Cluster(qdbd_settings["uri"]["insecure"], verify=True)


def test_refresh_and_relogin(rest_cluster):
    conn = rest_cluster
    conn._access_token = "invalid-access-token"
    assert _answer(conn) == [42]
    assert conn._access_token != "invalid-access-token"
    conn._access_token = "invalid-access-token"
    conn._refresh_token = "invalid-refresh-token"
    assert _answer(conn) == [42]
    assert conn._refresh_token != "invalid-refresh-token"


@pytest.mark.parametrize("options", [{}, {"verify": True}])
def test_https_rejects_self_signed_certificate_by_default(options):
    with pytest.raises(httpx.ConnectError) as exc:
        quasardb.Cluster("https://127.0.0.1:40443", **options)
    assert "certificate verify failed" in str(exc.value).lower()


@pytest.mark.parametrize("constructor", ["Cluster", "RestCluster"])
def test_encryption_requires_https(constructor):
    with pytest.raises(quasardb.InvalidArgumentError, match="HTTPS"):
        getattr(quasardb, constructor)("http://127.0.0.1", enable_encryption=True)


def test_credentials_file(rest_cluster_settings, tmp_path):
    settings = rest_cluster_settings
    credentials = tmp_path / "user.json"
    credentials.write_text(
        json.dumps(
            {
                "username": settings.get("user_name", ""),
                "secret_key": settings.get("user_private_key", ""),
            }
        )
    )
    if "user_name" not in settings:
        with pytest.raises(quasardb.InvalidArgumentError):
            quasardb.RestCluster(settings["uri"], user_security_file=str(credentials))
        return
    with quasardb.RestCluster(
        settings["uri"],
        user_security_file=str(credentials),
        cluster_public_key="ignored",
        cluster_public_key_file="ignored",
        enable_encryption=settings["enable_encryption"],
        do_version_check=True,
        client_max_parallelism=3,
        verify=settings.get("verify", True),
    ) as conn:
        assert _answer(conn) == [42]


@pytest.mark.parametrize(
    "options",
    [
        {"user_name": "incomplete"},
        {"user_private_key": "incomplete"},
        {"user_name": "u", "user_private_key": "k", "user_security_file": "unused"},
        {"user_security_file": "does-not-exist"},
    ],
)
def test_invalid_credentials(options):
    with pytest.raises(quasardb.InvalidArgumentError):
        quasardb.RestCluster("http://127.0.0.1", **options)


@pytest.mark.parametrize(
    "contents", ["{", "[]", "{}", '{"username": 1, "secret_key": "k"}']
)
def test_malformed_credentials_file(tmp_path, contents):
    credentials = tmp_path / "user.json"
    credentials.write_text(contents)
    with pytest.raises(quasardb.InvalidArgumentError):
        quasardb.RestCluster("http://127.0.0.1", user_security_file=str(credentials))


def test_invalid_query(rest_cluster):
    with pytest.raises(quasardb.InvalidQueryError, match="400"):
        with rest_cluster.stream_query("THIS IS NOT SQL"):
            pass


def test_stream_query_encoding_error_on_entry(rest_cluster):
    stream = rest_cluster.stream_query("\ud800")
    with pytest.raises(UnicodeEncodeError):
        with stream:
            pytest.fail("The query cannot be encoded as UTF-8")


def test_stream_stays_exhausted(rest_cluster):
    with rest_cluster.stream_query("SELECT 42 AS answer") as batches:
        assert next(batches)["answer"].tolist() == [42]
        with pytest.raises(StopIteration):
            next(batches)
        with pytest.raises(StopIteration):
            next(batches)


def test_close_stream_before_cluster(rest_cluster):
    with rest_cluster.stream_query("SELECT 42 AS answer") as batches:
        kept = next(batches)["answer"]
    assert _answer(rest_cluster) == [42]
    rest_cluster.close()
    assert not rest_cluster._access_token
    assert not rest_cluster._credentials
    assert kept.tolist() == [42]
    stream = rest_cluster.stream_query("SELECT 1")
    with pytest.raises(quasardb.InvalidHandleError):
        with stream:
            pass


@pytest.mark.parametrize(
    "query,kind",
    [
        ("#sql SELECT CAST('-9223372036854775808' AS BIGINT) AS value", "i"),
        ("#sql SELECT CAST('nan' AS DOUBLE) AS value", "f"),
    ],
)
def test_rest_sentinels_are_values(rest_cluster, query, kind):
    # Native writes turn these values into nulls. DuckDB literals let us check
    # that the Python client preserves a valid NaN and minimum int64.
    with rest_cluster.stream_query(query) as stream:
        array = next(stream)["value"]
        assert array.dtype.kind == kind
        assert not array.mask[0]
        if kind == "i":
            assert array[0] == -(2**63)
        else:
            assert np.isnan(array[0])


def test_large_query_preserves_values(rest_cluster, native_cluster, query_table):
    """Reassemble every row in order, regardless of where the transport splits batches."""
    count = 2**16 + 7
    timestamps = np.datetime64("2020-01-01", "ns") + np.arange(count).astype(
        "timedelta64[s]"
    )
    expected = np.arange(count, dtype="int64")
    from test_query import _write_points

    _write_points(native_cluster, query_table, "the_int64", (timestamps, expected))
    query = 'SELECT the_int64 FROM "{}"'.format(query_table.get_name())
    with rest_cluster.stream_query(query) as stream:
        actual = np.ma.concatenate([batch["the_int64"] for batch in stream])
    assert not np.any(actual.mask)
    np.testing.assert_array_equal(actual, expected)
