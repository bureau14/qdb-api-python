from __future__ import annotations

from typing import Iterable, Tuple

import numpy as np

# Numpy

# # Modern typing (numpy >= 1.22.0, python >= 3.9)
# DType = np.dtype[Any]
# NDArrayAny = np.ndarray[Any, np.dtype[Any]]
# NDArrayTime = np.ndarray[Any, np.dtype[np.datetime64]]
# MaskedArrayAny = np.ma.MaskedArray[Any, Any]

# Legacy fallback (numpy ~ 1.20.3, python 3.7)
DType = np.dtype
NDArrayAny = np.ndarray
NDArrayTime = np.ndarray
MaskedArrayAny = np.ma.MaskedArray

# Qdb expressions
Range = Tuple[np.datetime64, np.datetime64]
RangeSet = Iterable[Range]
