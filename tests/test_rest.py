"""Real service tests for transport dispatch and REST authentication/lifecycle."""

import datetime
import gc
import inspect
import json
import subprocess
import sys

import numpy as np
import pytest
import quasardb


def test_native_inheritance_and_lifetime(qdbd_connection, monkeypatch):
    from quasardb.quasardb import Cluster as LegacyCluster

    class NativeSubclass(quasardb.NativeCluster):
        pass

    assert LegacyCluster is quasardb.NativeCluster
    assert LegacyCluster is not quasardb.Cluster
    assert type(qdbd_connection) is quasardb.NativeCluster
    assert isinstance(qdbd_connection, quasardb.Cluster)
    assert issubclass(quasardb.NativeCluster, quasardb.Cluster)
    original_init = quasardb.NativeCluster.__init__
    calls = []

    def counted_init(self, *args, **kwargs):
        calls.append(self)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(quasardb.NativeCluster, "__init__", counted_init)
    for constructor in (quasardb.Cluster, LegacyCluster, NativeSubclass):
        conn = constructor(qdbd_connection.uri())
        assert calls.pop() is conn
        assert isinstance(conn, constructor)
        with conn as session:
            assert session.query("SELECT 1")[0]
            assert session.is_open()
        assert not conn.is_open()
        assert not calls
        del conn
        gc.collect()
    assert "uri" in inspect.signature(quasardb.Cluster).parameters


def test_native_without_optional_dependency(qdbd_connection):
    # A fresh interpreter prevents pandas' own optional Arrow import from hiding
    # an eager dependency in the connection/package import path.
    program = """
import importlib.abc
import inspect
import pydoc
import sys
class BlockArrow(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "pyarrow" or fullname.startswith("pyarrow."):
            raise ModuleNotFoundError("Arrow deliberately unavailable in this test")
sys.meta_path.insert(0, BlockArrow())
import quasardb
exported = {}
exec("from quasardb import *", exported)
assert "RestCluster" not in exported
assert exported["MaskedArray"] is quasardb.quasardb.MaskedArray
assert exported["Cluster"] is quasardb.Cluster
assert quasardb.quasardb.Cluster is quasardb.NativeCluster
assert quasardb.Cluster is not quasardb.quasardb.Cluster
for name in dir(quasardb.quasardb):
    if not name.startswith("_") and name != "Cluster":
        assert exported[name] is getattr(quasardb.quasardb, name), name
assert "RestCluster" not in dict(inspect.getmembers(quasardb))
assert "quasardb" in pydoc.render_doc(quasardb)
assert "quasardb._rest" not in sys.modules
import quasardb.pool
import quasardb.numpy
import quasardb.pandas
with quasardb.Cluster(sys.argv[1]) as conn:
    assert isinstance(conn, quasardb.NativeCluster)
    assert conn.query("SELECT 1")
assert "quasardb._rest" not in sys.modules
try:
    quasardb.Cluster("http://127.0.0.1:40080")
except quasardb.PyArrowRequired as exc:
    assert isinstance(exc, ImportError)
    assert isinstance(exc.__cause__, ModuleNotFoundError)
    assert "pip install 'quasardb[rest]'" in str(exc)
else:
    raise AssertionError("REST must report the missing optional dependency")
try:
    from quasardb import RestCluster
except quasardb.PyArrowRequired as exc:
    assert isinstance(exc, ImportError)
    assert isinstance(exc.__cause__, ModuleNotFoundError)
    assert "pip install 'quasardb[rest]'" in str(exc)
else:
    raise AssertionError("Explicit REST import must report the missing dependency")
"""
    result = subprocess.run(
        [sys.executable, "-c", program, qdbd_connection.uri()],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert result.returncode == 0, result.stderr


def test_wildcard_exports_after_explicit_rest_access():
    program = """
import quasardb as qdb
import quasardb.pool
assert qdb.RestCluster is not None
from quasardb import *
assert "RestCluster" not in locals()
assert pool is qdb.pool
assert MaskedArray is qdb.quasardb.MaskedArray
"""
    result = subprocess.run(
        [sys.executable, "-c", program],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "native_uri",
    [
        "qdb://127.0.0.1:2836,[::1]:2836",
        b"qdb://127.0.0.1:2836",
        bytearray(b"qdb://127.0.0.1:2836"),
    ],
)
def test_native_dispatch_forwards_uri(monkeypatch, native_uri):
    expected_timeout = datetime.timedelta(seconds=5)

    def capture(self, uri, *, timeout):
        assert uri is native_uri
        assert timeout == expected_timeout
        raise RuntimeError("native constructor reached")

    monkeypatch.setattr(quasardb.NativeCluster, "__init__", capture)
    with pytest.raises(RuntimeError, match="native constructor reached"):
        quasardb.Cluster(uri=native_uri, timeout=expected_timeout)


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


def test_unsupported_inventory():
    supported = {"close", "is_open", "uri", "stream_query"}
    methods = {name for name in dir(quasardb.NativeCluster) if not name.startswith("_")}
    assert {method for method, _ in unsupported} == methods - supported


def test_installed_typing_files():
    from pathlib import Path

    package = Path(quasardb.__file__).parent
    for name in (
        "py.typed",
        "__init__.pyi",
        "_cluster.py",
        "_rest.py",
        "quasardb/_cluster.pyi",
    ):
        assert (package / name).is_file(), name


@pytest.mark.parametrize(
    "query,kind",
    [
        ("#sql SELECT CAST('-9223372036854775808' AS BIGINT) AS value", "i"),
        ("#sql SELECT CAST('nan' AS DOUBLE) AS value", "f"),
    ],
)
def test_rest_sentinels_are_values(rest_connection, query, kind):
    # DuckDB literals avoid native insertion's sentinel-to-null conversion.
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
