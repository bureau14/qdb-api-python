import re
import pytest
import quasardb

def test_can_create_pool():
    # Basic test whether whether we can create a new pool

    with quasardb.Pool(uri='qdb://127.0.0.1:2836') as pool:
        pass

def test_can_acquire_connection():
    # Basic test whether whether we can acquire a new connection

    with quasardb.Pool(uri='qdb://127.0.0.1:2836', size=16) as pool:
        with pool.connect() as conn:
            pass
