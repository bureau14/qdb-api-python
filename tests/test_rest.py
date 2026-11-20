"""Real service tests for transport dispatch and REST authentication/lifecycle."""

import datetime
import json
import ssl

import numpy as np
import pytest
import quasardb


@pytest.mark.parametrize("constructor", ["Cluster", "RestCluster"])
@pytest.mark.parametrize(
    "uri", ["ftp://localhost", "https://[::1", "http://localhost:bad-port/"]
)
def test_invalid_uri(constructor, uri):
    with pytest.raises(quasardb.InvalidArgumentError):
        getattr(quasardb, constructor)(uri)


def _answer(conn):
    with conn.stream_query("SELECT 42 AS answer", batch_size=1) as stream:
        return list(stream)[0]["answer"].tolist()


def test_dispatch_and_concrete_construction(
    query_connection, query_connection_settings
):
    settings = query_connection_settings
    concrete = (
        quasardb.RestCluster
        if settings["uri"].startswith("http")
        else quasardb.NativeCluster
    )
    assert type(query_connection) is concrete
    assert isinstance(query_connection, quasardb.Cluster)
    assert issubclass(concrete, quasardb.Cluster)
    assert _answer(query_connection) == [42]
    with concrete(**settings) as conn:
        assert conn.uri() == settings["uri"]
        assert _answer(conn) == [42]
    assert not conn.is_open()
    conn.close()


def test_refresh_and_relogin(rest_connection):
    conn = rest_connection
    conn._access_token = "invalid-access-token"
    assert _answer(conn) == [42]
    assert conn._access_token != "invalid-access-token"
    conn._access_token = "invalid-access-token"
    conn._refresh_token = "invalid-refresh-token"
    assert _answer(conn) == [42]
    assert conn._refresh_token != "invalid-refresh-token"


@pytest.mark.parametrize("options", [{}, {"verify": True}])
def test_https_rejects_self_signed_certificate_by_default(options):
    with pytest.raises(quasardb.Error) as exc:
        quasardb.Cluster("https://127.0.0.1:40443", **options)
    assert isinstance(exc.value.__cause__, ssl.SSLCertVerificationError)


def test_credentials_file(rest_connection_settings, tmp_path):
    settings = rest_connection_settings
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
        enable_encryption=True,
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


def test_invalid_query(rest_connection):
    with pytest.raises(quasardb.InvalidQueryError, match="400"):
        with rest_connection.stream_query("THIS IS NOT SQL"):
            pass


def test_reader_stays_exhausted_until_context_exit(rest_connection):
    with rest_connection.stream_query("SELECT 42 AS answer") as reader:
        assert next(reader)["answer"].tolist() == [42]
        with pytest.raises(StopIteration):
            next(reader)
        assert reader._response is None
        with pytest.raises(StopIteration):
            next(reader)
    with pytest.raises(quasardb.UninitializedError):
        next(reader)


def test_close_active_reader(rest_connection):
    reader = rest_connection.stream_query("SELECT 42 AS answer")
    with reader:
        kept = next(reader)["answer"]
        rest_connection.close()
        assert reader._response is None
        assert reader._reader is None
        assert not rest_connection._access_token
        assert not rest_connection._credentials
        with pytest.raises(quasardb.UninitializedError):
            next(reader)
    assert kept.tolist() == [42]
    with pytest.raises(quasardb.InvalidHandleError):
        rest_connection.stream_query("SELECT 1")


unsupported = [
    ("blob", ("entry",)),
    ("compact_abort", ()),
    ("compact_full", ()),
    ("compact_progress", ()),
    ("double", ("entry",)),
    ("endpoints", ()),
    ("find", ("find(tag='tag')",)),
    ("get_memory_info", ()),
    ("integer", ("entry",)),
    ("node", ("127.0.0.1:2836",)),
    ("node_config", ("127.0.0.1:2836",)),
    ("node_status", ("127.0.0.1:2836",)),
    ("node_topology", ("127.0.0.1:2836",)),
    ("options", ()),
    ("perf", ()),
    ("pinned_writer", ()),
    ("prefix_count", ("prefix",)),
    ("prefix_get", ("prefix", 10)),
    ("properties", ()),
    ("purge_all", (datetime.timedelta(seconds=1),)),
    ("purge_cache", (datetime.timedelta(seconds=1),)),
    ("query", ("SELECT 1",)),
    ("query_continuous_full", ("SELECT 1", datetime.timedelta(seconds=1))),
    ("query_continuous_new_values", ("SELECT 1", datetime.timedelta(seconds=1))),
    ("query_numpy", ("SELECT 1",)),
    ("reader", (["table"],)),
    ("string", ("entry",)),
    ("suffix_count", ("suffix",)),
    ("suffix_get", ("suffix", 10)),
    ("table", ("table",)),
    ("tag", ("tag",)),
    ("tidy_memory", ()),
    ("timestamp", ("entry",)),
    ("trim_all", (datetime.timedelta(seconds=1), datetime.timedelta(seconds=1))),
    ("ts", ("table",)),
    ("wait_for_compaction", ()),
    ("validate_query", ("SELECT 1",)),
    (
        "split_query_range",
        (
            datetime.datetime(2020, 1, 1),
            datetime.datetime(2020, 1, 2),
            datetime.timedelta(hours=1),
        ),
    ),
    ("writer", ()),
]


def test_unsupported_methods(rest_connection):
    for method, args in unsupported:
        with pytest.raises(NotImplementedError):
            getattr(rest_connection, method)(*args)


@pytest.mark.parametrize(
    "query,kind",
    [
        ("#sql SELECT CAST('-9223372036854775808' AS BIGINT) AS value", "i"),
        ("#sql SELECT CAST('nan' AS DOUBLE) AS value", "f"),
    ],
)
def test_rest_sentinels_are_values(rest_connection, query, kind):
    # Native writes turn these values into nulls. DuckDB literals let us check
    # that the Python client preserves a valid NaN and minimum int64.
    with rest_connection.stream_query(query) as stream:
        array = next(stream)["value"]
        assert array.dtype.kind == kind
        assert not array.mask[0]
        if kind == "i":
            assert array[0] == -(2**63)
        else:
            assert np.isnan(array[0])


def test_large_query_preserves_values(query_connection, qdbd_connection, table):
    count = 2**16 + 7
    timestamps = np.datetime64("2020-01-01", "ns") + np.arange(count).astype(
        "timedelta64[s]"
    )
    expected = np.arange(count, dtype="int64")
    from test_query import _write_points

    _write_points(qdbd_connection, table, "the_int64", (timestamps, expected))
    query = 'SELECT the_int64 FROM "{}"'.format(table.get_name())
    for size in (0, 10000):
        with query_connection.stream_query(query, batch_size=size) as stream:
            batches = [batch["the_int64"] for batch in stream]
        assert all(len(array) > 0 for array in batches)
        if size and isinstance(query_connection, quasardb.NativeCluster):
            assert all(len(array) <= size for array in batches)
        actual = np.ma.concatenate(batches)
        assert not np.any(actual.mask)
        np.testing.assert_array_equal(actual, expected)


def test_native_parameter_conventions(qdbd_connection, entry_name):
    assert isinstance(qdbd_connection.table(entry_name), quasardb.Table)
    with pytest.raises(TypeError):
        qdbd_connection.table(alias=entry_name)
    with pytest.raises(TypeError):
        qdbd_connection.purge_all(timeout=datetime.timedelta(seconds=1))
    with qdbd_connection.stream_query(
        query="SELECT 42 AS answer", batch_size=1
    ) as reader:
        assert next(iter(reader))["answer"].tolist() == [42]
