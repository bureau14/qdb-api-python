
import pytest
import quasardb
from quasardb import RetryOptions

###
# XXX: If this import fails, ensure that you built the quasardb python API with
#      export QDB_TESTS_ENABLED=ON.
from quasardb import MockFailureOptions

def test_default_retry_thrice():
    x = RetryOptions()

    assert x.retries_left == 3
    assert x.has_next() == True

def test_permutations():
    x1 = RetryOptions()

    x2 = x1.next()
    x3 = x2.next()
    x4 = x3.next()

    assert x1.retries_left == 3
    assert x1.has_next() == True

    assert x2.retries_left == 2
    assert x2.has_next() == True

    assert x3.retries_left == 1
    assert x3.has_next() == True

    assert x4.retries_left == 0
    assert x4.has_next() == False

def test_retries_out_of_bounds():
    x = RetryOptions(0)

    assert x.retries_left == 0
    assert x.has_next() == False

    with pytest.raises(quasardb.OutOfBoundsError):
        x.next()

def test_mock_failures_disabled_by_default():
    x = MockFailureOptions()

    assert x.has_next() == False
    assert x.failures_left == 0

def test_mock_failures_permutations():
    x1 = MockFailureOptions(2)

    x2 = x1.next()
    x3 = x2.next()

    assert x1.has_next() == True
    assert x1.failures_left == 2

    assert x2.has_next() == True
    assert x2.failures_left == 1

    assert x3.has_next() == False
    assert x3.failures_left == 0
