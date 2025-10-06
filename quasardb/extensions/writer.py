import copy
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import numpy.ma as ma

import quasardb

__all__: List[Any] = []


def _ensure_ctype(self: Any, idx: int, ctype: quasardb.ColumnType) -> None:
    assert "table" in self._legacy_state
    infos = self._legacy_state["table"].list_columns()
    cinfo = infos[idx]

    ctype_data = copy.copy(ctype)
    ctype_column = copy.copy(cinfo.type)

    if ctype_data == quasardb.ColumnType.Symbol:
        ctype_data = quasardb.ColumnType.String

    if ctype_column == quasardb.ColumnType.Symbol:
        ctype_column = quasardb.ColumnType.String

    if not ctype_data == ctype_column:
        raise quasardb.IncompatibleTypeError()


def _legacy_next_row(self: Any, table: Any) -> Dict[str, Any]:
    if "pending" not in self._legacy_state:
        self._legacy_state["pending"] = []

    if "table" not in self._legacy_state:
        self._legacy_state["table"] = table

    self._legacy_state["pending"].append({"by_index": {}})

    # Return reference to the row inside the buffer
    return self._legacy_state["pending"][-1]


def _legacy_current_row(self: Any) -> Dict[str, Any]:
    return self._legacy_state["pending"][-1]


def _legacy_start_row(self: Any, table: Any, x: np.datetime64) -> None:
    row = _legacy_next_row(self, table)
    assert "$timestamp" not in row
    row["$timestamp"] = x


def _legacy_set_double(self: Any, idx: int, x: float) -> None:
    _ensure_ctype(self, idx, quasardb.ColumnType.Double)
    assert isinstance(x, float)
    assert idx not in _legacy_current_row(self)["by_index"]
    _legacy_current_row(self)["by_index"][idx] = x


def _legacy_set_int64(self: Any, idx: int, x: int) -> None:
    _ensure_ctype(self, idx, quasardb.ColumnType.Int64)
    assert isinstance(x, int)
    assert idx not in _legacy_current_row(self)["by_index"]
    _legacy_current_row(self)["by_index"][idx] = x


def _legacy_set_timestamp(self: Any, idx: int, x: np.datetime64) -> None:
    _ensure_ctype(self, idx, quasardb.ColumnType.Timestamp)
    assert idx not in _legacy_current_row(self)["by_index"]
    _legacy_current_row(self)["by_index"][idx] = x


def _legacy_set_string(self: Any, idx: int, x: str) -> None:
    _ensure_ctype(self, idx, quasardb.ColumnType.String)
    assert isinstance(x, str)
    assert idx not in _legacy_current_row(self)["by_index"]

    _legacy_current_row(self)["by_index"][idx] = x


def _legacy_set_blob(self: Any, idx: int, x: bytes) -> None:
    _ensure_ctype(self, idx, quasardb.ColumnType.Blob)
    assert isinstance(x, bytes)
    assert idx not in _legacy_current_row(self)["by_index"]

    _legacy_current_row(self)["by_index"][idx] = x


def _legacy_push(self: Any) -> Optional[quasardb.WriterData]:
    if "pending" not in self._legacy_state:
        # Extremely likely default case, no "old" rows
        return None

    assert "table" in self._legacy_state
    table = self._legacy_state["table"]

    # Some useful constants
    dtype_by_ctype = {
        quasardb.ColumnType.Double: np.dtype("float64"),
        quasardb.ColumnType.Int64: np.dtype("int64"),
        quasardb.ColumnType.Timestamp: np.dtype("datetime64[ns]"),
        quasardb.ColumnType.String: np.dtype("unicode"),
        quasardb.ColumnType.Symbol: np.dtype("unicode"),
        quasardb.ColumnType.Blob: np.dtype("bytes"),
    }

    ctype_by_idx = {}
    cinfos = table.list_columns()
    for i in range(len(cinfos)):
        ctype_by_idx[i] = cinfos[i].type

    all_idx = set(ctype_by_idx.keys())

    # Prepare data structure
    pivoted: Dict[str, Any] = {"$timestamp": [], "by_index": {}}
    for i in all_idx:
        pivoted["by_index"][i] = []

    # Do the actual pivot
    for row in self._legacy_state["pending"]:
        assert "$timestamp" in row
        assert "by_index" in row

        pivoted["$timestamp"].append(row["$timestamp"])

        for idx in pivoted["by_index"].keys():
            val = row["by_index"].get(idx, None)
            pivoted["by_index"][idx].append(val)

    # Validation / verification, not strictly necessary. Effectively
    # ensures that we have the exact same amount of values for every
    # column
    for xs in pivoted["by_index"].values():
        assert len(xs) == len(pivoted["$timestamp"])

    column_data = []

    for idx, xs in pivoted["by_index"].items():
        ctype = ctype_by_idx[idx]
        dtype = dtype_by_ctype[ctype]

        # None-mask works, because everything inside the list are just regular ojbects

        mask = [x is None for x in xs]

        if all(mask):
            xs_ = ma.masked_all(len(xs), dtype=dtype)
        else:
            xs_ = ma.masked_array(data=np.array(xs, dtype), mask=mask)

        assert len(xs_) == len(pivoted["$timestamp"])

        column_data.append(xs_)

    push_data = quasardb.WriterData()
    index = np.array(pivoted["$timestamp"], np.dtype("datetime64[ns]"))

    push_data.append(table, index, column_data)

    self._legacy_state = {}
    return push_data


def _wrap_fn(
    old_fn: Callable[..., Any], replace_fn: Callable[..., Optional[quasardb.WriterData]]
) -> Callable[..., Any]:

    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        data = replace_fn(self)
        if data:
            return old_fn(self, data, *args, **kwargs)
        else:
            return old_fn(self, *args, **kwargs)

    return wrapped


def extend_writer(x: Any) -> None:
    """
    Extends the writer with the "old", batch inserter API. This is purely
    a backwards compatibility layer, and we want to avoid having to maintain that
    in C++ with few benefits.
    """

    x.start_row = _legacy_start_row
    x.set_double = _legacy_set_double
    x.set_int64 = _legacy_set_int64
    x.set_string = _legacy_set_string
    x.set_blob = _legacy_set_blob
    x.set_timestamp = _legacy_set_timestamp

    x.push = _wrap_fn(x.push, _legacy_push)
    x.push_fast = _wrap_fn(x.push_fast, _legacy_push)
    x.push_async = _wrap_fn(x.push_async, _legacy_push)
    x.push_truncate = _wrap_fn(x.push_truncate, _legacy_push)
