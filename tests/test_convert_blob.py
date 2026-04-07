from quasardb import test_convert as m

import numpy as np
import numpy.ma as ma
import quasardb

from utils import assert_indexed_arrays_equal


def _blob_recode(values):
    idx = np.array(
        [
            np.datetime64("2017-01-01T00:00:00", "ns"),
            np.datetime64("2017-01-01T00:00:01", "ns"),
            np.datetime64("2017-01-01T00:00:02", "ns"),
            np.datetime64("2017-01-01T00:00:03", "ns"),
        ]
    )
    return m.test_array_recode(quasardb.ColumnType.Blob, np.dtype("O"), (idx, values))


def test_blob_object_array_recode_roundtrip_bytes():
    values = np.array([b"alpha", b"beta", b"\x00gamma", b"delta"], dtype=np.object_)
    result = _blob_recode(values)
    assert_indexed_arrays_equal((result[0], values), result)


def test_blob_object_array_recode_roundtrip_none_values():
    values = np.array([b"alpha", None, b"gamma", None], dtype=np.object_)
    result = _blob_recode(values)
    assert_indexed_arrays_equal((result[0], values), result)


def test_blob_object_array_recode_roundtrip_masked_values():
    values = ma.masked_array(
        data=np.array([b"alpha", b"beta", b"gamma", b"delta"], dtype=np.object_),
        mask=[False, True, False, True],
    )
    result = _blob_recode(values)
    assert_indexed_arrays_equal((result[0], values), result)


def test_blob_object_array_recode_roundtrip_empty_bytes():
    values = np.array([b"", b"beta", b"", b"delta"], dtype=np.object_)
    result = _blob_recode(values)
    assert_indexed_arrays_equal((result[0], values), result)
