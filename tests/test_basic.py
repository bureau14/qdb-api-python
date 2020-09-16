# pylint: disable=C0103,C0111,C0302,W0212
import datetime

import pytest
import quasardb


def test_build():
    build = quasardb.build()
    assert len(build) > 0


def test_version():
    build = quasardb.version()
    assert len(build) > 0

def test_can_purge_all(qdbd_secure_connection):
    qdbd_secure_connection.purge_all(datetime.timedelta(minutes=1))
