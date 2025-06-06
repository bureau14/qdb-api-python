from time import sleep
import re
import pytest
import quasardb
import quasardb.stats as qdbst

import test_batch_inserter as batchlib
import conftest

_has_stats = False


def _write_data(conn, table):
    inserter = conn.inserter(batchlib._make_inserter_info(table))

    # doubles, blobs, strings, integers, timestamps, symbols =
    # batchlib._test_with_table(
    _, _, _, _, _, _ = batchlib._test_with_table(
        inserter, table, conftest.create_many_intervals(), batchlib._regular_push
    )


def _has_stats(conn):
    xs = qdbst.by_node(conn)

    # As we always use a secure connection, we will wait for by-uid statistics to appear.
    #
    # We also don't expect a cluster to be running, so just ensure we have exactly 1 node.
    ks = list(xs.keys())
    assert len(ks) == 1

    node_id = ks[0]
    node_stats = xs[node_id]

    assert "by_uid" in node_stats
    uid_stats = node_stats["by_uid"]

    # The actual check happens here: we expect at least 1 per-uid statistic
    return len(uid_stats.keys()) > 0


def _ensure_stats(conn, table):
    # This function is merely here to generate some activity on the cluster,
    # so that we're guaranteed to have some statistics

    max_polls = 10
    n = 0

    while _has_stats(conn) is False:
        _write_data(conn, table)
        sleep(1)

        n = n + 1
        assert n <= max_polls


# A fairly conservative set of user-specific stats we always expect after doing our
# _write_data() activity generating.
#
# We can add keys here if we want to better test against regressions, but for now it's
# fairly conservative.
_expected_user_stats = [
    "requests.total_count",
    "requests.successes_count",
    "requests.in_bytes",
    "requests.out_bytes",
    "perf.ts.table_insert.deserialization.total_ns",
    "perf.ts.table_insert.processing.total_ns",
]

# Same, but cumulative stats.
_expected_cumulative_stats = [
    "requests.total_count",
    "requests.successes_count",
    "requests.in_bytes",
    "requests.out_bytes",
    "perf.ts.buffered_table_insert.deserialization.total_ns",
    "perf.ts.buffered_table_insert.processing.total_ns",
]


def _validate_stats_dict(xs):
    """
    Validates a dict with keys / stat tuples association.
    """
    for k, x in xs.items():
        print("k: ", k, ", x: ", x)

        # Each statistic is a dict of type/unit/value
        assert isinstance(x, dict)
        assert "value" in x
        assert "type" in x
        assert "unit" in x

        # Everything that's not a NONE unit (i.e. not a label) should be an int
        if x["unit"] != qdbst.Unit.NONE:

            assert isinstance(x["value"], int)


def _validate_node_stats(stats):
    assert "by_uid" in stats
    assert "cumulative" in stats

    # Test user stats
    for uid, xs in stats["by_uid"].items():
        assert isinstance(uid, int)

        for expected in _expected_user_stats:
            assert expected in xs

        _validate_stats_dict(xs)

    # Test cumulative stats
    xs = stats["cumulative"]
    for expected in _expected_cumulative_stats:
        assert expected in xs

        _validate_stats_dict(xs)


def test_stats_by_node(qdbd_secure_connection, secure_table):
    _ensure_stats(qdbd_secure_connection, secure_table)

    xs = qdbst.by_node(qdbd_secure_connection)

    assert len(xs) == 1

    for _, stats in xs.items():
        _validate_node_stats(stats)


def test_stats_of_node(qdbd_settings, qdbd_secure_connection, secure_table):
    # First seed the table
    _ensure_stats(qdbd_secure_connection, secure_table)

    # Now establish direct connection
    conn = quasardb.Node(
        qdbd_settings.get("uri").get("secure").replace("qdb://", ""),
        user_name=qdbd_settings.get("security").get("user_name"),
        user_private_key=qdbd_settings.get("security").get("user_private_key"),
        cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"),
        enable_encryption=qdbd_settings.get("security").get("enable_encryption"),
    )

    qdbst.of_node(conn)


def test_stats_regex():
    # This is mostly to test a regression, where user-stats for users with multi-digit ids
    # were not picked up.

    user_stats = [
        "$qdb.statistics.foo.uid_1",
        "$qdb.statistics.foo.uid_21",
        "$qdb.statistics.foo.uid_321",
        "$qdb.statistics.foo.bar.uid_321",
    ]
    total_stats = [
        "$qdb.statistics.foo",
        "$qdb.statistics.foo.bar",
        "$qdb.statistics.foo.bar1" "$qdb.statistics.u.i.d.1" "$qdb.statistics.foo.uid1",
    ]

    for s in user_stats:
        assert qdbst.is_user_stat(s) is True
        assert qdbst.is_cumulative_stat(s) is False

    for s in total_stats:
        assert qdbst.is_user_stat(s) is False
        assert qdbst.is_cumulative_stat(s) is True


def test_stat_type_is_deprecated():
    with pytest.deprecated_call():
        qdbst.stat_type("foobar")
