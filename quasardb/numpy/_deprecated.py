from __future__ import annotations

import warnings
from typing import Any, Optional, Tuple

from quasardb.quasardb import Table
from quasardb.typing import DType, MaskedArrayAny, NDArrayTime


def _warn_deprecated_api(name: str, replacement: str, stacklevel: int = 2) -> None:
    warnings.warn(
        f"{name} is deprecated and will be removed in a future version. "
        f"Use {replacement} instead.",
        DeprecationWarning,
        stacklevel=stacklevel,
    )


def read_array(
    table: Optional[Table] = None, column: Optional[str] = None, ranges: Any = None
) -> Tuple[NDArrayTime, MaskedArrayAny]:
    """
    Deprecated compatibility wrapper around read_arrays().

    Reads a single column and returns its shared timestamp index together with
    the column data as a masked array.
    """
    from . import read_arrays

    _warn_deprecated_api(
        "qdbnp.read_array()",
        "qdbnp.read_arrays(..., columns=[column], ...)",
    )

    if table is None:
        raise RuntimeError("A table is required.")

    if column is None:
        raise RuntimeError("A column is required.")

    idx, data = read_arrays(table=table, columns=[column], ranges=ranges)
    return idx, data[column]


def write_array(
    data: Any = None,
    index: Optional[NDArrayTime] = None,
    table: Optional[Table] = None,
    column: Optional[str] = None,
    dtype: Optional[DType] = None,
    infer_types: bool = True,
) -> None:
    """
    Deprecated compatibility wrapper around write_arrays().

    Write a Numpy array to a single column.

    Parameters:
    -----------

    data: np.array
      Numpy array with a dtype that is compatible with the column's type.

    index: np.array
      Numpy array with a datetime64[ns] dtype that will be used as the
      $timestamp axis for the data to be stored.

    dtype: optional np.dtype
      If provided, ensures the data array is converted to this dtype before
      insertion.

    infer_types: optional bool
      If true, when necessary will attempt to convert the data and index array
      to the best type for the column. For example, if you provide float64 data
      while the column's type is int64, it will automatically convert the data.

      Defaults to True. For production use cases where you want to avoid implicit
      conversions, we recommend always setting this to False.

    """
    from . import write_arrays

    _warn_deprecated_api("qdbnp.write_array()", "qdbnp.write_arrays(...)")

    if table is None:
        raise RuntimeError("A table is required.")

    if column is None:
        raise RuntimeError("A column is required.")

    if data is None:
        raise RuntimeError("A data numpy array is required.")

    if index is None:
        raise RuntimeError("An index numpy timestamp array is required.")

    write_arrays(
        {column: data},
        None,
        table,
        dtype={column: dtype},
        index=index,
        infer_types=infer_types,
        writer=table.writer(),
    )


read_array.__module__ = "quasardb.numpy"
write_array.__module__ = "quasardb.numpy"
