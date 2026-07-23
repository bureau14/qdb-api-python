import numpy as np
import numpy.ma as ma

import quasardb.numpy as qdbnp


def _generate_data(count, start=np.datetime64("2017-01-01", "ns")):
    doubles = np.random.uniform(-100.0, 100.0, count)
    integers = np.random.randint(-100, 100, count)
    blobs = np.array(
        list(np.random.bytes(np.random.randint(8, 16)) for _ in range(count)), "O"
    )
    strings = np.array([("content_" + str(item)) for item in range(count)])
    timestamps = np.array(
        [(start + np.timedelta64(1, "D") + np.timedelta64(i, "s")) for i in range(count)]
    ).astype("datetime64[ns]")
    symbols = np.array([("symbol_" + str(item)) for item in range(count)])

    return doubles, integers, blobs, strings, timestamps, symbols


def _table_data_by_column(table, data):
    doubles, integers, blobs, strings, timestamps, symbols = data
    column_data = {
        "the_double": doubles,
        "the_blob": blobs,
        "the_string": strings,
        "the_int64": integers,
        "the_ts": timestamps,
        "the_symbol": symbols,
    }

    return {
        column.name: column_data[column.name]
        for column in table.list_columns()
        if column.name in column_data
    }


def _read_all_columns(conn, table, *, ranges=None):
    column_names = [column.name for column in table.list_columns()]
    return qdbnp.read_arrays(conn, [table], column_names=column_names, ranges=ranges)


def _assert_results(conn, table, intervals, data):
    doubles, integers, blobs, strings, timestamps, symbols = data

    whole_range = (intervals[0], intervals[-1:][0] + np.timedelta64(2, "s"))
    idx, xs = _read_all_columns(conn, table, ranges=[whole_range])

    np.testing.assert_array_equal(idx, intervals)
    np.testing.assert_array_equal(xs["the_double"], doubles)
    np.testing.assert_array_equal(xs["the_blob"], blobs)
    np.testing.assert_array_equal(xs["the_string"], strings)
    np.testing.assert_array_equal(xs["the_int64"], integers)
    np.testing.assert_array_equal(xs["the_ts"], timestamps)
    np.testing.assert_array_equal(xs["the_symbol"], symbols)


def _test_with_table(conn, table, intervals, data=None, push_mode=None):
    if data is None:
        data = _generate_data(len(intervals))

    whole_range = (intervals[0], intervals[-1:][0] + np.timedelta64(2, "s"))
    idx, xs = _read_all_columns(conn, table, ranges=[whole_range])
    assert len(idx) == 0
    for values in xs.values():
        assert len(values) == 0

    kwargs = {}
    if push_mode is not None:
        kwargs["push_mode"] = push_mode

    qdbnp.write_arrays(
        _table_data_by_column(table, data),
        conn,
        table,
        index=intervals,
        infer_types=False,
        **kwargs
    )

    _assert_results(conn, table, intervals, data)

    doubles, integers, blobs, strings, timestamps, symbols = data
    return doubles, blobs, strings, integers, timestamps, symbols


def assert_ma_equal(lhs, rhs):
    """
    A bit hacky way to compare two masked arrays for equality, as the default
    numpy way of doing so is a bit meh (first converts to np.array, and *then*
    compares, which defeats the point of using ma -> ergo meh est).
    """
    assert ma.isMA(lhs)
    assert ma.isMA(rhs)

    assert ma.count_masked(lhs) == ma.count_masked(rhs)

    lhs_ = lhs.torecords()
    rhs_ = rhs.torecords()

    for (lval, lmask), (rval, rmask) in zip(lhs_, rhs_):
        assert lmask == rmask

        if not lmask:
            assert lval == rval


def assert_arrays_equal(lhs, rhs):
    """
    Accepts two arrays, potentially masked or not, and compares them.
    """
    if not ma.isMA(lhs):
        lhs = qdbnp.ensure_ma(lhs)

    if not ma.isMA(rhs):
        rhs = qdbnp.ensure_ma(rhs)

    assert_ma_equal(lhs, rhs)


def assert_indexed_arrays_equal(lhs, rhs):
    """
    Accepts two "array results", which is two tuples of a timestamp and a
    data array.

    lhs is typically the "generated" / "input" data, rhs is typically the data
    returned by qdb.
    """
    (lhs_idx, lhs_data) = lhs
    (rhs_idx, rhs_data) = rhs

    # The index (timestamps) is not allowed to be null, so should never be
    # a masked array anyway.
    assert not ma.isMA(lhs_idx)
    assert not ma.isMA(rhs_idx)
    np.testing.assert_array_equal(lhs_idx, rhs_idx)

    assert_arrays_equal(lhs_data, rhs_data)
