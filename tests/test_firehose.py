# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import numpy as np
import quasardb
import quasardb.firehose as qdbfh
import test_batch_inserter as batchlib
import time
import itertools
import multiprocessing

def _ensure_timeout(f, timeout=3):
    p = multiprocessing.Process(target=f)
    p.start()

    p.join(timeout)

    if not p.is_alive():
        return False

    p.terminate()
    p.join()
    return True


def test_subscribe_single_table(qdbd_connection, table, many_intervals):
    # Our test case for streaming data is as follows:
    # 1. We first initialize a subscription
    # 2. *After* this, we insert new data
    # 3. We ensure we actually retrieved that data, and exactly that data (nothing more)
    # 4. Insert new data
    # 5. Repeat step 3 again

    inserter = qdbd_connection.inserter(
        batchlib._make_inserter_info(table))

    xs = table.subscribe(qdbd_connection)

    doubles, blobs, strings, integers, timestamps = batchlib._test_with_table(
        inserter,
        table,
        many_intervals,
        batchlib._regular_push)

    time.sleep(4)

    n = len(doubles)


    # After insertion, we expect exactly `n` items in the database, and not a
    # single element more.
    offset = 0
    while offset < n:
        row = next(xs)
        assert row['the_double'] == doubles[offset]
        assert row['the_blob'] == blobs[offset]
        assert row['the_string'] == strings[offset]
        assert row['the_int64'] == integers[offset]
        assert row['the_ts'] == timestamps[offset]
        offset = offset + 1

    def next_row():
        return next(xs)

    # Ensure that acquiring the next row times out after 3 seconds, which ensures
    # that we have reached 'the end'.
    assert _ensure_timeout(next_row, timeout=3) == True

    # Now, we insert additional data, which should wake up our subscription.
    # What we're going to do is just insert the exact same data, but this time
    # with different intervals
    many_intervals_ = list()
    for x in many_intervals:
        many_intervals_.append(x + np.timedelta64(365, 'D'))

    doubles, blobs, strings, integers, timestamps = batchlib._test_with_table(
        inserter,
        table,
        many_intervals_,
        batchlib._regular_push)

    # Note that we only reset `offset`; since we just inserted exactly double
    # the data, `n` is still 10000 which is perfect!
    offset = 0
    while offset < n:
        row = next(xs)
        assert row['the_double'] == doubles[offset]
        assert row['the_blob'] == blobs[offset]
        assert row['the_string'] == strings[offset]
        assert row['the_int64'] == integers[offset]
        assert row['the_ts'] == timestamps[offset]
        offset = offset + 1


    # Ensure that acquiring the next row times out after 3 seconds, which ensures
    # that we have reached 'the end'.
    assert _ensure_timeout(next_row, timeout=3) == True
