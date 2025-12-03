import numpy as np
import pytest

import quasardb


@pytest.mark.usefixtures("qdbd_connection")
def test_batch_push_arrow_with_options(qdbd_connection, entry_name):
    pa = pytest.importorskip("pyarrow")

    table_name = entry_name + "_arrow"
    table = qdbd_connection.table(table_name)

    column = quasardb.ColumnInfo(quasardb.ColumnType.Double, "value")
    table.create([column])

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
        table_name,
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
