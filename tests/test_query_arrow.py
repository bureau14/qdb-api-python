# # pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb
import numpy as np

import test_table as tslib

def test_returns_alias_not_found_when_ts_doesnt_exist(qdbd_connection):
    with pytest.raises(quasardb.AliasNotFoundError):
        qdbd_connection.query_arrow(
            'select * from ' +
            'this_ts_doesnt_exist' +
            ' in range(2017, +10d)')

def test_returns_empty_result(qdbd_connection, table):
    res = qdbd_connection.query_arrow(
        "select * from \"" +
        table.get_name() +
        "\" in range(2016-01-01 , 2016-12-12)")
    # We have 8 null columns
    assert len(res) == 8
    for x in res:
        assert x.is_null()