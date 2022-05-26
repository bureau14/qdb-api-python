# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import quasardb


def test_set_timeout_1_day(qdbd_connection):
    qdbd_connection.options().set_timeout(datetime.timedelta(days=1))


def test_set_timeout_1_second(qdbd_connection):
    qdbd_connection.options().set_timeout(datetime.timedelta(seconds=1))


def test_set_timeout_throws__when_timeout_is_1_microsecond(qdbd_connection):
    with pytest.raises(quasardb.InvalidArgumentError):
        qdbd_connection.options().set_timeout(datetime.timedelta(microseconds=1))
