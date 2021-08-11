import re
import pytest
import quasardb
import quasardb.pool as pool

def test_can_create_pool():
    # Basic test whether whether we can create a new pool

    with pool.SingletonPool(uri='qdb://127.0.0.1:2836') as p:
        pass

def test_can_acquire_connection():
    # Basic test whether whether we can acquire a new connection

    with pool.SingletonPool(uri='qdb://127.0.0.1:2836') as p:
        with p.connect() as conn:
            pass

def test_releases_connection_in_with_block():
    # Tests whether pool.release() is invoked with a connection after a `with` block,
    # rather than actually closing a connection

    the_conn = None

    with pool.SingletonPool(uri='qdb://127.0.0.1:2836') as p:
        with p.connect() as conn:
            the_conn = conn
            assert the_conn.is_open() is True

        assert the_conn.is_open() is True


def test_cleans_connections_after_pool_close():
    # Verifies whether a pool cleans up all connections after it is closed

    the_conn = None

    with pool.SingletonPool(uri='qdb://127.0.0.1:2836') as p:
        with p.connect() as conn:
            the_conn = conn
            assert the_conn.is_open() is True

        assert the_conn.is_open() is True

    assert the_conn.is_open() is False



def test_use_acquired_connection(entry_name):
    # Ensures the connection returned by `pool.connect()` is actually a usable
    # QuasarDB connection. It's relevant because we extend the connection class
    # for our SingletonPool.

    with pool.SingletonPool(uri='qdb://127.0.0.1:2836') as p:
        with p.connect() as conn:
            conn.integer(entry_name).put(1234)
            assert conn.integer(entry_name).get() == 1234

def test_uninitialized_singleton():
    # Tests whether an error is thrown if we use the singleton without initialization
    with pytest.raises(AssertionError, match=r"Global connection pool uninitialized"):
        pool.instance()

def test_singleton():
    pool.initialize(uri='qdb://127.0.0.1:2836')
    assert isinstance(pool.instance(), pool.SingletonPool)

def test_singleton_checks_type():
    # Tests whether an error is thrown if we initialize the pool with an incorrect
    # arg.
    with pytest.raises(TypeError):
        pool.initialize('Foo')

@pool.with_conn()
def _test_default_decorator_impl(conn):
    # Default decorator just inserts the pool as the first arg
    return conn

def test_default_decorator():
    assert isinstance(_test_default_decorator_impl(), pool.SessionWrapper)

@pool.with_conn(arg='foo')
def _test_kwargs_decorator_impl(entry_name, foo):
    # We can use arg= to specify a specific kwarg that should be used
    return foo

def test_kwarg_decorator(entry_name):
    assert isinstance(_test_kwargs_decorator_impl(entry_name), pool.SessionWrapper)

@pool.with_conn(arg=1)
def _test_positional_args_decorator_impl(entry_name, conn, another_entry_name):
    # We expect the pool to be injected at exactly position 1
    return conn

def test_positional_args_decorator(entry_name):
    assert isinstance(_test_positional_args_decorator_impl(entry_name, entry_name), pool.SessionWrapper)
