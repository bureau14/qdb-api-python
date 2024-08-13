###
#
# XXX: If this import fails, ensure that you built the quasardb python API with
#      export QDB_TESTS_ENABLED=ON. E.g.
#
#      ```
#      export QDB_TESTS_ENABLED=ON
#      python3 setup.py test --addopts "-s tests/test_retry_options.py"
#      ```
#
#      This is because the retry options are built-in with failure mocking
#      ability when compiled with tests enabled.

import pytest
import quasardb
from quasardb import RetryOptions

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
    x = RetryOptions()

    assert x.has_mock_failure() == False
    assert x.mock_failures_left == 0
