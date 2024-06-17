# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
from functools import reduce
import test_batch_inserter as batchlib
import numpy as np


def test_can_open_reader(qdbd_connection, table):
    tables = [table]

    with qdbd_connection.reader(tables) as reader:
        rows = list(reader)
        assert len(rows) == 0


def test_reader_has_infinite_batch_size_by_default(qdbd_connection, table):
    tables = [table]

    with qdbd_connection.reader(tables) as reader:
        assert reader.get_batch_size() == 0


def test_cannot_provide_batch_size_as_regular_arg(qdbd_connection, table):
    tables = [table]

    with pytest.raises(TypeError):
        with qdbd_connection.reader(tables, 128) as reader:
            assert reader.get_batch_size() == 128


def test_can_set_batch_size_as_kwarg(qdbd_connection, table):
    tables = [table]

    with qdbd_connection.reader(tables, batch_size=128) as reader:
        assert reader.get_batch_size() == 128


def test_reader_can_iterate_rows(qdbpd_write_fn, df_with_table, qdbd_connection, many_intervals, row_count):
    (ctype, dtype, df, table) = df_with_table

    assert len(df.index) == row_count

    column_names = list(column.name for column in table.list_columns())

    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False, dtype=dtype)

    tables = [table]

    with qdbd_connection.reader(tables) as reader:
        seen = False

        for row in reader:
            # When we're not providing a "batch size", we expect everything in 1 large batch, and since
            # we only have 1 table, we should really just have only a single iteration.
            assert seen == False
            seen = True

            assert len(row['$timestamp']) == row_count

            for column_name in column_names:
                assert len(row[column_name]) == row_count


def test_reader_can_iterate_batches(qdbpd_write_fn, df_with_table, qdbd_connection, many_intervals, row_count):
    (ctype, dtype, df, table) = df_with_table

    assert len(df.index) == row_count

    # We expect the row_count to be divisble by 2, otherwise the test below doesn't work anymore
    assert row_count % 2 == 0
    batch_size = int(row_count / 2)


    column_names = list(column.name for column in table.list_columns())

    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False, dtype=dtype)

    tables = [table]

    with qdbd_connection.reader(tables, batch_size=batch_size) as reader:
        seen = 0

        for row in reader:
            # We expect exactly 2 batches for a single table
            assert seen < 2
            seen += 1

            assert len(row['$timestamp']) == batch_size

            for column_name in column_names:
                assert len(row[column_name]) == batch_size
