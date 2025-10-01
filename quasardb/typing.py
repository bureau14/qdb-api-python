from __future__ import annotations

from typing import Any, Iterable

import numpy as np

# Numpy
DType = np.dtype[Any]
NDArrayAny = np.ndarray[Any, np.dtype[Any]]
NDArrayTime = np.ndarray[Any, np.dtype[np.datetime64]]
MaskedArrayAny = np.ma.MaskedArray[Any, Any]

# Qdb expressions
Range = tuple[np.datetime64, np.datetime64]
RangeSet = Iterable[Range]
