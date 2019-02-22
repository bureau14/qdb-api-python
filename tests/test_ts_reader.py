# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
import test_ts_batch as batchlib

def test_reader_returns_correct_results(qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    offset = 0
    for row in table.reader():
        assert row[0] == doubles[offset]
        assert row[1] == blobs[offset]
        assert row[2] == integers[offset]
        assert row[3] == timestamps[offset]

        offset = offset + 1

def test_reader_iterator_returns_reference(qdbd_connection, table, many_intervals):
    # For performance reasons, our iterators are merely references to the
    # underlying local_table position. A side effect is that if the iterator moves
    # forward, all references will move forward as well.
    #
    # This is actually undesired, and this test is here to detect regressions in
    # behavior.
    batch_inserter = qdbd_connection.ts_batch(batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    rows = []
    for row in table.reader():
        print("appending row: " + str(row[0]))
        rows.append(row)

    for row in rows:
        print("row = " + str(row[0]))
