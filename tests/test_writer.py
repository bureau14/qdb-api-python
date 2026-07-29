# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange, int as long  # pylint: disable=W0622
from functools import reduce  # pylint: disable=W0622
import datetime
import test_table as tslib
from time import sleep

import pytest
import quasardb
import numpy as np
import quasardb.numpy as qdbnp


def _make_local_creation_table(qdbd_connection, table_name):
    columns = [
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, "value"),
        quasardb.ColumnInfo(
            quasardb.ColumnType.Symbol, "symbol", "writer_creation_symbols"
        ),
    ]
    shard_size = datetime.timedelta(days=1)
    ttl = datetime.timedelta(days=7)

    schema = quasardb.TableSchema(columns, shard_size, ttl)
    table = qdbd_connection.table_from_schema(table_name, schema)
    return table, columns, shard_size, ttl


def _generate_data(count, start=np.datetime64("2017-01-01", "ns")):
    integers = np.random.randint(-100, 100, count)
    timestamps = tslib._generate_dates(start + np.timedelta64("1", "D"), count)

    return (integers, timestamps)


def test_local_creation_table_normalizes_explicit_timestamp(
    qdbd_connection, entry_name
):
    columns = [
        quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, "$timestamp"),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, "value"),
    ]
    table = qdbd_connection.table_from_schema(
        entry_name,
        quasardb.TableSchema(
            columns,
            datetime.timedelta(days=1),
            datetime.timedelta(0),
        ),
    )

    local_columns = table.list_columns()
    assert len(local_columns) == 1
    assert local_columns[0].name == "value"
    assert local_columns[0].type == quasardb.ColumnType.Int64

    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    qdbnp.write_arrays(
        {"$timestamp": np.array([timestamp]), "value": np.array([42], dtype="int64")},
        qdbd_connection,
        table,
        infer_types=False,
        creation_mode=quasardb.WriterCreationMode.CreateTables,
    )

    created_columns = qdbd_connection.table(entry_name).list_columns()
    assert len(created_columns) == 1
    assert created_columns[0].name == "value"
    assert created_columns[0].type == quasardb.ColumnType.Int64


@pytest.mark.parametrize(
    "columns",
    [
        [
            quasardb.ColumnInfo(quasardb.ColumnType.Double, "$timestamp"),
            quasardb.ColumnInfo(quasardb.ColumnType.Int64, "value"),
        ],
        [
            quasardb.ColumnInfo(quasardb.ColumnType.Int64, "value"),
            quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, "$timestamp"),
        ],
    ],
    ids=["wrong-type", "wrong-position"],
)
def test_local_creation_table_rejects_invalid_timestamp(
    qdbd_connection, entry_name, columns
):
    with pytest.raises(quasardb.InvalidArgumentError):
        qdbd_connection.table_from_schema(
            entry_name,
            quasardb.TableSchema(
                columns,
                datetime.timedelta(days=1),
                datetime.timedelta(0),
            ),
        )


@pytest.mark.parametrize(
    "creation_mode",
    [None, quasardb.WriterCreationMode.DontCreate],
    ids=["default", "dont-create"],
)
def test_missing_table_is_not_created(qdbd_connection, entry_name, creation_mode):
    table, _, _, _ = _make_local_creation_table(qdbd_connection, entry_name)
    writer = qdbd_connection.writer()

    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    writer.set_int64(0, 42)
    writer.set_string(1, "forty-two")

    push_options = {}
    if creation_mode is not None:
        push_options["creation_mode"] = creation_mode

    with pytest.raises(quasardb.AliasNotFoundError):
        writer.push(**push_options)

    with pytest.raises(quasardb.AliasNotFoundError):
        qdbd_connection.table(entry_name).list_columns()


def test_create_tables_mode_creates_missing_table(qdbd_connection, entry_name):
    table, expected_columns, shard_size, ttl = _make_local_creation_table(
        qdbd_connection, entry_name
    )
    writer = qdbd_connection.writer()
    timestamp = np.datetime64("now", "ns")

    writer.start_row(table, timestamp)
    writer.set_int64(0, 42)
    writer.set_string(1, "forty-two")
    writer.push(creation_mode=quasardb.WriterCreationMode.CreateTables)

    created_table = qdbd_connection.table(entry_name)
    actual_columns = created_table.list_columns()

    assert len(actual_columns) == len(expected_columns)
    for actual, expected in zip(actual_columns, expected_columns):
        assert actual.name == expected.name
        assert actual.type == expected.type
        assert actual.symtable == expected.symtable

    assert created_table.get_shard_size() == shard_size
    assert created_table.get_ttl() == ttl

    rows = qdbd_connection.query(
        'SELECT "$timestamp","value","symbol" FROM "{}"'.format(entry_name)
    )
    assert len(rows) == 1
    assert rows[0]["$timestamp"] == timestamp
    assert rows[0]["value"] == 42
    assert rows[0]["symbol"] == "forty-two"


def test_create_tables_mode_uses_existing_table(qdbd_connection, table):
    local_table = qdbd_connection.table_from_schema(
        table.get_name(),
        quasardb.TableSchema(
            table.list_columns(),
            table.get_shard_size(),
            table.get_ttl(),
        ),
    )
    writer = qdbd_connection.writer()
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")

    writer.start_row(local_table, timestamp)
    writer.set_int64(3, 42)
    writer.push(creation_mode=quasardb.WriterCreationMode.CreateTables)

    rows = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )
    assert len(rows) == 1
    assert rows[0]["$timestamp"] == timestamp
    assert rows[0]["the_int64"] == 42


def test_create_tables_mode_rejects_mixed_batch(
    qdbd_connection, table, random_identifier
):
    existing_table = qdbd_connection.table(table.get_name())
    missing_table, _, _, _ = _make_local_creation_table(
        qdbd_connection, random_identifier
    )
    timestamp = np.datetime64("now", "ns")
    index = np.array([timestamp], dtype="datetime64[ns]")

    with pytest.raises(quasardb.InvalidArgumentError, match="local schema"):
        qdbnp.write_arrays(
            [
                (
                    existing_table,
                    {
                        "$timestamp": index,
                        "the_int64": np.array([42], dtype="int64"),
                    },
                ),
                (
                    missing_table,
                    {
                        "$timestamp": index,
                        "value": np.array([7], dtype="int64"),
                        "symbol": np.array(["seven"], dtype="U"),
                    },
                ),
            ],
            qdbd_connection,
            infer_types=False,
            creation_mode=quasardb.WriterCreationMode.CreateTables,
        )

    assert qdbd_connection.table(random_identifier).exists() is False


def test_create_tables_mode_rejects_server_backed_table(qdbd_connection, table):
    timestamp = np.datetime64("now", "ns")
    index = np.array([timestamp], dtype="datetime64[ns]")

    # CreateTables accepts only tables built from an explicit local schema.
    with pytest.raises(quasardb.InvalidArgumentError, match="local schema"):
        qdbnp.write_arrays(
            [
                (
                    table,
                    {
                        "$timestamp": index,
                        "the_int64": np.array([42], dtype="int64"),
                    },
                ),
            ],
            qdbd_connection,
            infer_types=False,
            creation_mode=quasardb.WriterCreationMode.CreateTables,
        )

    assert table.exists() is True


def test_incorrect_type_double(qdbd_connection, table):
    writer = qdbd_connection.writer()
    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    for idx in range(6):
        if idx == 0:
            continue
        with pytest.raises(quasardb.IncompatibleTypeError):
            writer.set_double(idx, 1.1)


def test_successful_type_double(qdbd_connection, table):
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    value = 1.1

    writer = qdbd_connection.writer()
    writer.start_row(table, timestamp)
    writer.set_double(0, value)
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_double" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 1
    assert res[0]["$timestamp"] == timestamp
    assert res[0]["the_double"] == value


def test_incorrect_type_blob(qdbd_connection, table):
    writer = qdbd_connection.writer()
    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    with pytest.raises(quasardb.IncompatibleTypeError):
        writer.set_int64(1, 1234)


def test_successful_type_blob(qdbd_connection, table):
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    value = b"aaa"

    writer = qdbd_connection.writer()
    writer.start_row(table, timestamp)
    writer.set_blob(1, value)
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_blob" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 1
    assert res[0]["$timestamp"] == timestamp
    assert res[0]["the_blob"] == value


def test_incorrect_type_string(qdbd_connection, table):
    writer = qdbd_connection.writer()
    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    with pytest.raises(quasardb.IncompatibleTypeError):
        writer.set_int64(2, 1234)


def test_successful_type_string(qdbd_connection, table):
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    value = "aaa"

    writer = qdbd_connection.writer()
    writer.start_row(table, timestamp)
    writer.set_string(2, value)
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_string" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 1
    assert res[0]["$timestamp"] == timestamp
    assert res[0]["the_string"] == value


def test_incorrect_type_int64(qdbd_connection, table):
    writer = qdbd_connection.writer()
    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    for idx in range(6):
        if idx == 3:
            continue
        with pytest.raises(quasardb.IncompatibleTypeError):
            writer.set_int64(idx, 1)


def test_successful_type_int64(qdbd_connection, table):
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    value = 1

    writer = qdbd_connection.writer()
    writer.start_row(table, timestamp)
    writer.set_int64(3, value)
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 1
    assert res[0]["$timestamp"] == timestamp
    assert res[0]["the_int64"] == value


def test_incorrect_type_timestamp(qdbd_connection, table):
    writer = qdbd_connection.writer()
    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    for idx in range(6):
        if idx == 4:
            continue
        with pytest.raises(quasardb.IncompatibleTypeError):
            writer.set_timestamp(idx, np.datetime64("2020-01-01T00:00:00", "ns"))


def test_successful_type_timestamp(qdbd_connection, table):
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    value = np.datetime64("2020-01-01T00:00:00", "ns")

    writer = qdbd_connection.writer()
    writer.start_row(table, timestamp)
    writer.set_timestamp(4, value)
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_ts" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 1
    assert res[0]["$timestamp"] == timestamp
    assert res[0]["the_ts"] == value


def test_incorrect_type_symbol(qdbd_connection, table):
    writer = qdbd_connection.writer()
    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    with pytest.raises(quasardb.IncompatibleTypeError):
        writer.set_int64(5, 1234)


def test_successful_type_symbol(qdbd_connection, table):
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    value = "aaa"

    writer = qdbd_connection.writer()
    writer.start_row(table, timestamp)
    writer.set_string(5, value)
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_symbol" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 1
    assert res[0]["$timestamp"] == timestamp
    assert res[0]["the_symbol"] == value


def test_insert_data_ordered_single_shard(qdbd_connection, table):
    writer = qdbd_connection.writer()

    timestamps = [
        np.datetime64("2020-01-01T00:00:00", "ns"),
        np.datetime64("2020-01-01T00:00:01", "ns"),
    ]
    values = [0, 1]

    writer.start_row(table, timestamps[0])
    writer.set_int64(3, values[0])
    writer.start_row(table, timestamps[1])
    writer.set_int64(3, values[1])

    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row["$timestamp"] == timestamps[idx]
        assert row["the_int64"] == values[idx]


def test_insert_data_ordered_multiple_shards(qdbd_connection, table):
    writer = qdbd_connection.writer()

    timestamps = [np.datetime64("2020-01-01", "ns"), np.datetime64("2020-01-02", "ns")]
    values = [1, 2]

    writer.start_row(table, timestamps[0])
    writer.set_int64(3, values[0])
    writer.start_row(table, timestamps[1])
    writer.set_int64(3, values[1])

    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row["$timestamp"] == timestamps[idx]
        assert row["the_int64"] == values[idx]


def test_insert_data_unordered_single_shard(qdbd_connection, table):
    writer = qdbd_connection.writer()

    timestamps = [
        np.datetime64("2020-01-01T00:00:01", "ns"),
        np.datetime64("2020-01-01T00:00:00", "ns"),
    ]
    values = [1, 0]

    writer.start_row(table, timestamps[0])
    writer.set_int64(3, values[0])
    writer.start_row(table, timestamps[1])
    writer.set_int64(3, values[1])

    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )

    assert res[0]["$timestamp"] == timestamps[1]
    assert res[0]["the_int64"] == values[1]
    assert res[1]["$timestamp"] == timestamps[0]
    assert res[1]["the_int64"] == values[0]


def test_insert_data_unordered_multiple_shards(qdbd_connection, table):
    writer = qdbd_connection.writer()

    timestamps = [np.datetime64("2020-01-02", "ns"), np.datetime64("2020-01-01", "ns")]
    values = [2, 1]

    writer.start_row(table, timestamps[0])
    writer.set_int64(3, values[0])
    writer.start_row(table, timestamps[1])
    writer.set_int64(3, values[1])

    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )

    assert res[0]["$timestamp"] == timestamps[1]
    assert res[0]["the_int64"] == values[1]
    assert res[1]["$timestamp"] == timestamps[0]
    assert res[1]["the_int64"] == values[0]


def test_insert_data_ordered_single_shard_two_push(qdbd_connection, table):
    writer = qdbd_connection.writer()

    timestamps = [
        np.datetime64("2020-01-01T00:00:00", "ns"),
        np.datetime64("2020-01-01T00:00:01", "ns"),
    ]
    values = [0, 1]

    writer.start_row(table, timestamps[0])
    writer.set_int64(3, values[0])
    writer.push()

    writer.start_row(table, timestamps[1])
    writer.set_int64(3, values[1])
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row["$timestamp"] == timestamps[idx]
        assert row["the_int64"] == values[idx]


def test_insert_data_ordered_multiple_shards_two_push(qdbd_connection, table):
    writer = qdbd_connection.writer()

    timestamps = [np.datetime64("2020-01-01", "ns"), np.datetime64("2020-01-02", "ns")]
    values = [1, 2]

    writer.start_row(table, timestamps[0])
    writer.set_int64(3, values[0])
    writer.push()

    writer.start_row(table, timestamps[1])
    writer.set_int64(3, values[1])
    writer.push()

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_int64" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 2
    for idx, row in enumerate(res):
        assert row["$timestamp"] == timestamps[idx]
        assert row["the_int64"] == values[idx]


def test_write_through_flag(qdbd_connection, table):
    timestamp = np.datetime64("2020-01-01T00:00:00", "ns")
    value = "aaa"

    writer = qdbd_connection.writer()
    writer.start_row(table, timestamp)
    writer.set_string(2, value)
    writer.push(write_through=True)

    res = qdbd_connection.query(
        'SELECT "$timestamp","the_string" FROM "{}"'.format(table.get_name())
    )
    assert len(res) == 1
    assert res[0]["$timestamp"] == timestamp
    assert res[0]["the_string"] == value


def test_write_through_flag_throws_when_incorrect(qdbd_connection, table):
    writer = qdbd_connection.writer()
    writer.start_row(table, np.datetime64("2020-01-01T00:00:00", "ns"))
    with pytest.raises(quasardb.InvalidArgumentError):
        writer.push(write_through="wrong!")


# generative tests


def _row_insertion_method(
    writer, table, dates, doubles, blobs, strings, integers, timestamps, symbols
):
    for i in range(len(dates)):
        writer.start_row(table, dates[i])
        writer.set_double(0, doubles[i])
        writer.set_blob(1, blobs[i])
        writer.set_string(2, strings[i])
        writer.set_int64(3, integers[i])
        writer.set_timestamp(4, timestamps[i])
        writer.set_string(5, symbols[i])


def _regular_push(writer):
    writer.push()


def _async_push(writer):
    writer.push_async()
    # Wait for push_async to complete
    # Ideally we could be able to get the proper flush interval
    sleep(15)


def _fast_push(writer):
    writer.push_fast()


def _generate_data(count, start=np.datetime64("2017-01-01", "ns")):
    doubles = np.random.uniform(-100.0, 100.0, count)
    integers = np.random.randint(-100, 100, count)
    blobs = np.array(
        list(np.random.bytes(np.random.randint(8, 16)) for i in range(count)), "O"
    )
    strings = np.array([("content_" + str(item)) for item in range(count)])
    timestamps = tslib._generate_dates(start + np.timedelta64("1", "D"), count)
    symbols = np.array([("sym_" + str(item)) for item in range(count)])

    return (doubles, integers, blobs, strings, timestamps, symbols)


def _set_batch_writer_data(writer, table, intervals, data, start=0):
    (doubles, integers, blobs, strings, timestamps, symbols) = data

    for i in range(start, len(intervals)):
        writer.start_row(table, intervals[i])
        writer.set_double(0, doubles[i])
        writer.set_blob(1, blobs[i])
        writer.set_string(2, strings[i])
        writer.set_int64(3, integers[i])
        writer.set_timestamp(4, timestamps[i])
        writer.set_string(5, symbols[i])


def _read_column(conn, table, column, *, ranges=None):
    idx, xs = qdbnp.read_arrays(
        conn,
        [table],
        column_names=[column],
        ranges=ranges,
    )
    return idx, xs[column]


def _read_all_columns(conn, table, *, ranges=None):
    column_names = [
        tslib._double_col_name(table),
        tslib._blob_col_name(table),
        tslib._string_col_name(table),
        tslib._int64_col_name(table),
        tslib._ts_col_name(table),
        tslib._symbol_col_name(table),
    ]
    return qdbnp.read_arrays(conn, [table], column_names=column_names, ranges=ranges)


def _assert_results(conn, table, intervals, data):
    (doubles, integers, blobs, strings, timestamps, symbols) = data

    whole_range = (intervals[0], intervals[-1:][0] + np.timedelta64(2, "s"))
    idx, xs = _read_all_columns(conn, table, ranges=[whole_range])

    np.testing.assert_array_equal(idx, intervals)
    np.testing.assert_array_equal(xs[tslib._double_col_name(table)], doubles)
    np.testing.assert_array_equal(xs[tslib._blob_col_name(table)], blobs)
    np.testing.assert_array_equal(xs[tslib._string_col_name(table)], strings)
    np.testing.assert_array_equal(xs[tslib._int64_col_name(table)], integers)
    np.testing.assert_array_equal(xs[tslib._ts_col_name(table)], timestamps)
    np.testing.assert_array_equal(xs[tslib._symbol_col_name(table)], symbols)
