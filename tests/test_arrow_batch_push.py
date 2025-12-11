import numpy as np
import pandas as pd
import pytest

import quasardb
import quasardb.pandas as qdbpd


def _arrow_reader(timestamps, values):
    pa = pytest.importorskip("pyarrow")

    ts_array = pa.array(timestamps.astype("datetime64[ns]"), type=pa.timestamp("ns"))
    value_array = pa.array(values, type=pa.float64())
    batch = pa.record_batch([ts_array, value_array], names=["$timestamp", "value"])
    return pa.RecordBatchReader.from_batches(batch.schema, [batch])


def _create_arrow_table(connection, entry_name):
    table_name = entry_name + "_arrow"
    table = connection.table(table_name)

    column = quasardb.ColumnInfo(quasardb.ColumnType.Double, "value")
    table.create([column])

    return table


@pytest.mark.usefixtures("qdbd_connection")
def test_batch_push_arrow_with_options(qdbd_connection, entry_name):
    pa = pytest.importorskip("pyarrow")

    table = _create_arrow_table(qdbd_connection, entry_name)

    timestamps = np.array(
        [
            np.datetime64("2024-01-01T00:00:00", "ns"),
            np.datetime64("2024-01-01T00:00:01", "ns"),
        ],
        dtype="datetime64[ns]",
    )
    values = pa.array([1.5, 2.5], type=pa.float64())
    ts_array = pa.array(timestamps.astype("datetime64[ns]"), type=pa.timestamp("ns"))

    batch = pa.record_batch([ts_array, values], names=["$timestamp", "value"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    qdbd_connection.batch_push_arrow(
        table.get_name(),
        reader,
        deduplicate=False,
        deduplication_mode="drop",
        push_mode=quasardb.WriterPushMode.Transactional,
        write_through=False,
    )

    results = table.double_get_ranges(
        "value", [(timestamps[0], timestamps[-1] + np.timedelta64(1, "s"))]
    )

    np.testing.assert_array_equal(results[0], timestamps)
    np.testing.assert_allclose(results[1], np.array([1.5, 2.5]))


@pytest.mark.parametrize(
    "deduplication_mode, expected_values",
    [
        ("drop", np.array([1.5, 2.5])),
        ("upsert", np.array([10.5, 11.5])),
    ],
)
def test_batch_push_arrow_deduplicate_modes(
    qdbd_connection, entry_name, deduplication_mode, expected_values
):
    pytest.importorskip("pyarrow")

    table = _create_arrow_table(qdbd_connection, entry_name)

    timestamps = np.array(
        [
            np.datetime64("2024-01-01T00:00:00", "ns"),
            np.datetime64("2024-01-01T00:00:01", "ns"),
        ],
        dtype="datetime64[ns]",
    )

    initial_reader = _arrow_reader(timestamps, [1.5, 2.5])
    duplicate_reader = _arrow_reader(timestamps, [10.5, 11.5])

    qdbd_connection.batch_push_arrow(
        table.get_name(),
        initial_reader,
        write_through=True,
    )
    qdbd_connection.batch_push_arrow(
        table.get_name(),
        duplicate_reader,
        deduplicate=["$timestamp"],
        deduplication_mode=deduplication_mode,
        write_through=True,
    )

    results = table.double_get_ranges(
        "value", [(timestamps[0], timestamps[-1] + np.timedelta64(1, "s"))]
    )

    np.testing.assert_array_equal(results[0], timestamps)
    np.testing.assert_allclose(results[1], expected_values)


def test_batch_push_arrow_invalid_deduplication_mode(qdbd_connection, entry_name):
    pytest.importorskip("pyarrow")

    table = _create_arrow_table(qdbd_connection, entry_name)

    timestamps = np.array(
        [
            np.datetime64("2024-01-01T00:00:00", "ns"),
            np.datetime64("2024-01-01T00:00:01", "ns"),
        ],
        dtype="datetime64[ns]",
    )

    reader = _arrow_reader(timestamps, [1.5, 2.5])

    with pytest.raises(quasardb.InvalidArgumentError):
        qdbd_connection.batch_push_arrow(
            table.get_name(),
            reader,
            deduplicate=True,
            deduplication_mode="invalid",
            write_through=False,
        )


def test_arrow_push_roundtrip_with_pandas(df_with_table, qdbd_connection):
    pa = pytest.importorskip("pyarrow")

    (_, _, df, table) = df_with_table

    qdbpd.write_dataframe(
        df, qdbd_connection, table, infer_types=False, arrow_push=True
    )

    batches = []
    with qdbd_connection.reader([table.get_name()]) as reader:
        for batch_reader in reader.arrow_batch_reader():
            batches.append(batch_reader.read_all())

    combined = pa.concat_tables(batches)
    result_df = combined.to_pandas()

    assert "$timestamp" in result_df.columns

    result_df = result_df.set_index("$timestamp")
    result_df.index = result_df.index.astype("datetime64[ns]")

    # Build expected dataframe: original df + $table column
    expected_df = df.copy()
    # $table does not exist initially, we must add it explicitly
    expected_df["$table"] = table.get_name()
    expected_df["$timestamp"] = expected_df.index.astype("datetime64[ns]")
    expected_df = expected_df.set_index("$timestamp")

    pd.testing.assert_frame_equal(
        expected_df.sort_index(), result_df.sort_index(), check_like=True, check_dtype=False
    )
