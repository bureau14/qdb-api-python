from time import sleep
import re
import pytest
import quasardb
import quasardb.stats as qdbst
import pprint

import test_batch_inserter as batchlib
import conftest

_has_stats = False

def _write_data(conn, table):
    inserter = conn.inserter(
        batchlib._make_inserter_info(table))

    doubles, blobs, strings, integers, timestamps = batchlib._test_with_table(
        inserter,
        table,
        conftest.create_many_intervals(),
        batchlib._regular_push)

def _ensure_stats(conn, table):
    global _has_stats

    # This function is merely here to generate some activity on the cluster,
    # so that we're guaranteed to have some statistics
    if _has_stats is False:
        _write_data(conn,table)
        # Statistics refresh interval is 5 seconds.
        sleep(6)
        _has_stats = True

# A fairly conservative set of user-specific stats we always expect after doing our
# _write_data() activity generating.
#
# We can add keys here if we want to better test against regressions, but for now it's
# fairly conservative.
_expected_user_stats = ['requests.total_count',
                        'requests.successes_count',
                        'requests.bytes_in',
                        'requests.bytes_out',
                        'perf.ts.table_insert.deserialization.total_ns',
                        'perf.ts.table_insert.processing.total_ns']

# Same, but cumulative stats.
_expected_cumulative_stats = ['requests.total_count',
                              'requests.successes_count',
                              'requests.bytes_in',
                              'requests.bytes_out',
                              'perf.ts.buffered_table_insert.deserialization.total_ns',
                              'perf.ts.buffered_table_insert.processing.total_ns',
                              'partitions_count']


def test_stats_by_node (qdbd_secure_connection,
                        secure_table):
    assert(len(_expected_cumulative_stats) > len(_expected_user_stats))
    _ensure_stats(qdbd_secure_connection, secure_table)
    xs = qdbst.by_node(qdbd_secure_connection)

    pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(xs['127.0.0.1:2838']['cumulative'])

    assert len(xs) == 1

    for _,stats in xs.items():
        assert 'by_uid' in stats
        assert 'cumulative' in stats

        # Test user stats
        for uid,xs in stats['by_uid'].items():
            assert isinstance(uid, int)

            for expected in _expected_user_stats:
                assert expected in xs

            for k,v in xs.items():
                # As far as I know, per-user statistics should *always* be integers
                assert isinstance(v, int)

        # Test cumulative stats
        xs = stats['cumulative']
        for expected in _expected_cumulative_stats:
            assert expected in xs
