# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
import numpy as np


def test_can_open_reader(qdbd_connection, table):
    table_names = [table.get_name()]

    with qdbd_connection.reader(table_names) as reader:
        rows = list(reader)
        assert len(rows) == 0


def test_cannot_iterate_reader_without_open(qdbd_connection, table):
    table_names = [table.get_name()]
    reader = qdbd_connection.reader(table_names)

    with pytest.raises(quasardb.UninitializedError):
        _ = list(reader)


def test_reader_has_infinite_batch_size_by_default(qdbd_connection, table):
    table_names = [table.get_name()]

    with qdbd_connection.reader(table_names) as reader:
        assert reader.get_batch_size() == 0


def test_cannot_provide_batch_size_as_regular_arg(qdbd_connection, table):
    table_names = [table.get_name()]

    with pytest.raises(TypeError):
        with qdbd_connection.reader(table_names, 128) as reader:
            assert reader.get_batch_size() == 128


def test_can_set_batch_size_as_kwarg(qdbd_connection, table):
    table_names = [table.get_name()]

    with qdbd_connection.reader(table_names, batch_size=128) as reader:
        assert reader.get_batch_size() == 128


def test_reader_returns_dicts(qdbpd_write_fn, df_with_table, qdbd_connection):
    (ctype, dtype, df, table) = df_with_table

    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False, dtype=dtype)

    table_names = [table.get_name()]

    with qdbd_connection.reader(table_names) as reader:
        for row in reader:
            # These should be pure dicts
            assert isinstance(row, dict)

            # Ensure each key is a numpy array-like
            for k, v in row.items():

                if k == '$timestamp':
                    # this is the timestamp index, should always be a regular
                    # numpy array
                    assert isinstance(v, np.ndarray)
                else:
                    assert isinstance(v, np.ma.core.MaskedArray)


def test_reader_can_iterate_rows(
        qdbpd_write_fn,
        df_with_table,
        qdbd_connection,
        row_count):
    (ctype, dtype, df, table) = df_with_table

    assert len(df.index) == row_count

    column_names = list(column.name for column in table.list_columns())

    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False, dtype=dtype)

    table_names = [table.get_name()]

    with qdbd_connection.reader(table_names) as reader:
        seen = False

        for row in reader:
            # When we're not providing a "batch size", we expect everything in
            # 1 large batch, and since we only have 1 table, we should really
            # just have only a single iteration.
            assert seen is False
            seen = True

            assert len(row['$timestamp']) == row_count

            for column_name in column_names:
                assert len(row[column_name]) == row_count


def test_reader_can_iterate_batches(
        qdbpd_write_fn,
        df_with_table,
        qdbd_connection,
        row_count):
    (ctype, dtype, df, table) = df_with_table

    assert len(df.index) == row_count

    # We expect the row_count to be divisble by 2, otherwise the test below
    # doesn't work anymore
    assert row_count % 2 == 0
    batch_size = int(row_count / 2)
    column_names = list(column.name for column in table.list_columns())

    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False, dtype=dtype)

    table_names = [table.get_name()]

    with qdbd_connection.reader(table_names, batch_size=batch_size) as reader:
        seen = 0

        for row in reader:
            # We expect exactly 2 batches for a single table
            assert seen < 2
            seen += 1

            assert len(row['$timestamp']) == batch_size

            for column_name in column_names:
                assert len(row[column_name]) == batch_size
