# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
import numpy as np
import pandas as pd


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

                if k == "$timestamp":
                    # this is the timestamp index, should always be a regular
                    # numpy array
                    assert isinstance(v, np.ndarray)
                else:
                    assert isinstance(v, np.ma.core.MaskedArray)


def test_reader_can_iterate_rows(
    qdbpd_write_fn, df_with_table, qdbd_connection, row_count
):
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

            assert len(row["$timestamp"]) == row_count

            for column_name in column_names:
                assert len(row[column_name]) == row_count


def test_reader_can_iterate_batches(
    qdbpd_write_fn, df_with_table, qdbd_connection, row_count
):
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

            assert len(row["$timestamp"]) == batch_size

            for column_name in column_names:
                assert len(row[column_name]) == batch_size


# ---------------------------------------------------------------------------
# Arrow-based reader tests
# ---------------------------------------------------------------------------


def _read_all_arrow_batches_to_df(reader):
    """
    Helper: reads arrow batches one by one, concatenates them,
    and returns a pandas DataFrame indexed by $timestamp.
    """
    pa = pytest.importorskip("pyarrow")

    tables = []

    # Consume batches one by one (do not call list() on the iterator up front)
    for batch_reader in reader.arrow_batch_reader():
        table = batch_reader.read_all()
        assert isinstance(table, pa.Table)
        # We expect non-empty batches here, otherwise something is off
        assert table.num_rows > 0
        tables.append(table)

    combined = pa.concat_tables(tables)

    df = combined.to_pandas()
    assert "$timestamp" in df.columns
    assert "$table" in df.columns

    df = df.set_index("$timestamp")
    df.index = df.index.astype("datetime64[ns]")

    return df


def test_arrow_reader_batches(
    qdbpd_write_fn, df_with_table, qdbd_connection, reader_batch_size
):
    (ctype, dtype, df, table) = df_with_table

    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False, dtype=dtype)

    table_names = [table.get_name()]

    with qdbd_connection.reader(table_names, batch_size=reader_batch_size) as reader:
        result_df = _read_all_arrow_batches_to_df(reader)

    # Build expected dataframe: original df + $table column
    expected_df = df.copy()
    # $table does not exist initially, we must add it explicitly
    expected_df["$table"] = table_names[0]
    expected_df["$timestamp"] = expected_df.index.astype("datetime64[ns]")
    expected_df = expected_df.set_index("$timestamp")

    # Sort and compare, allow different column order
    pd.testing.assert_frame_equal(
        expected_df.sort_index(),
        result_df.sort_index(),
        check_like=True,
        check_dtype=False,
    )


def test_arrow_reader_respects_batch_size(
    qdbpd_write_fn, df_with_table, qdbd_connection, row_count
):
    """
    Similar to test_reader_can_iterate_batches but using the Arrow API.

    Ensures:
      - total row count matches the original DataFrame;
      - multiple batches are produced when batch_size < total rows.
    """
    pa = pytest.importorskip("pyarrow")

    (ctype, dtype, df, table) = df_with_table
    assert len(df.index) == row_count
    assert row_count % 2 == 0

    batch_size = row_count // 2
    qdbpd_write_fn(df, qdbd_connection, table, infer_types=False, dtype=dtype)

    table_names = [table.get_name()]

    total_rows = 0
    batch_count = 0

    with qdbd_connection.reader(table_names, batch_size=batch_size) as reader:
        for batch_reader in reader.arrow_batch_reader():
            batch_count += 1

            table_batch = batch_reader.read_all()
            assert isinstance(table_batch, pa.Table)

            df_batch = table_batch.to_pandas()
            assert "$timestamp" in df_batch.columns
            assert "$table" in df_batch.columns

            # All rows in the batch must belong to known tables
            assert set(df_batch["$table"].unique()).issubset(set(table_names))

            total_rows += len(df_batch)

    # Total number of rows must match original DataFrame
    assert total_rows == len(df.index)

    # With this batch size we expect at least two Arrow batches
    assert batch_count >= 2
