import re
import pytest
from quasardb.pool import Pool, with_pool

def test_can_create_pool():
    # Basic test whether whether we can create a new pool

    with Pool(uri='qdb://127.0.0.1:2836') as pool:
        pass

def test_can_acquire_connection():
    # Basic test whether whether we can acquire a new connection

    with Pool(uri='qdb://127.0.0.1:2836') as pool:
        with pool.connect() as conn:
            pass

def test_use_acquired_connection(entry_name):
    # Ensures the connection returned by `pool.connect()` is actually a usable
    # QuasarDB connection. It's relevant because we extend the connection class
    # for our Pool.

    with Pool(uri='qdb://127.0.0.1:2836') as pool:
        with pool.connect() as conn:
            conn.integer(entry_name).put(1234)
            assert conn.integer(entry_name).get() == 1234


@with_pool
def _test_default_decorator_impl(pool):
    return True

def test_default_decorator():
    assert _test_default_decorator_impl() == True

@with_pool(arg='foo')
def _test_kwargs_decorator_impl(entry_name, foo):
    return True

def test_kwarg_decorator(entry_name):
    assert _test_kwargs_decorator_impl(entry_name) == True
