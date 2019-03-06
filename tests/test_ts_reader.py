# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
from functools import reduce
import test_ts_batch as batchlib
import numpy as np


def test_reader_can_return_no_rows(qdbd_connection, table, many_intervals):
    assert 0 == reduce(lambda x, y: x + 1, table.reader(), 0)


def test_reader_returns_correct_results(
        qdbd_connection, table, many_intervals):
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    offset = 0
    for row in table.reader():
        assert row[1] == doubles[offset]
        assert row[2] == blobs[offset]
        assert row[3] == integers[offset]
        assert row[4] == timestamps[offset]

        offset = offset + 1

def test_reader_iterator_returns_reference(
        qdbd_connection, table, many_intervals, capsys):
    # For performance reasons, our iterators are merely references to the
    # underlying local_table position. A side effect is that if the iterator moves
    # forward, all references will move forward as well.
    #
    # This is actually undesired, and this test is here to detect regressions in
    # behavior.
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    rows = []
    for row in table.reader():
        rows.append(row)

    for row in rows:
        # Timestamp is copied by value
        assert isinstance(row[0], np.datetime64)

        assert row[1] == None
        assert row[2] == None
        assert row[3] == None
        assert row[4] == None

def test_reader_can_copy_rows(qdbd_connection, table, many_intervals):
    # As a mitigation to the local table reference issue tested above,
    # we provide the ability to copy rows.

    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    rows = []
    for row in table.reader():
        copied = row.copy()
        rows.append(copied)

    offset = 0
    for row in rows:
        assert row[1] == doubles[offset]
        assert row[2] == blobs[offset]
        assert row[3] == integers[offset]
        assert row[4] == timestamps[offset]

        offset = offset + 1


def test_reader_can_select_columns(qdbd_connection, table, many_intervals):
    # Verifies that we can select a subset of the total available columns.
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    offset = 0
    for row in table.reader(columns=['the_int64', 'the_double']):
        assert row[1] == integers[offset]
        assert row[2] == doubles[offset]

        with pytest.raises(IndexError):
            print(str(row[3]))

        offset = offset + 1

def test_reader_can_request_ranges(qdbd_connection, table, many_intervals):
    # Verifies that we can select ranges
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    first_range = (many_intervals[0], many_intervals[1])
    second_range = (many_intervals[1], many_intervals[2])
    assert 1 == reduce(
        lambda x,
        y: x + 1,
        table.reader(
            ranges=[first_range]),
        0)
    assert 2 == reduce(
        lambda x,
        y: x + 1,
        table.reader(
            ranges=[
                first_range,
                second_range]),
        0)


def test_reader_can_read_dicts(qdbd_connection, table, many_intervals):
    # Verifies that we can select a subset of the total available columns.
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    offset = 0
    for row in table.reader(dict=True):
        assert isinstance(row['$timestamp'], np.datetime64)
        assert row['the_double'] == doubles[offset]
        assert row['the_blob'] == blobs[offset]
        assert row['the_int64'] == integers[offset]
        assert row['the_ts'] == timestamps[offset]

        with pytest.raises(KeyError):
            print(str(row['nothere']))

        offset = offset + 1

def test_reader_can_copy_dict_rows(qdbd_connection, table, many_intervals):
    # Just like our regular row reader, our dict-based ready also needs to
    # copy the rows.
    batch_inserter = qdbd_connection.ts_batch(
        batchlib._make_ts_batch_info(table))

    doubles, blobs, integers, timestamps = batchlib._test_with_table(
        batch_inserter,
        table,
        many_intervals,
        batchlib._row_insertion_method,
        batchlib._regular_push)

    offset = 0
    rows = []

    for row in table.reader(dict=True):
        copied = row.copy()
        rows.append(copied)

    for row in rows:
        assert row['the_double'] == doubles[offset]
        assert row['the_blob'] == blobs[offset]
        assert row['the_int64'] == integers[offset]
        assert row['the_ts'] == timestamps[offset]

        with pytest.raises(KeyError):
            print(str(row['nothere']))

        offset = offset + 1
