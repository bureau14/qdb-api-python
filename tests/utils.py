import numpy as np
import numpy.ma as ma

import quasardb.numpy as qdbnp


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
