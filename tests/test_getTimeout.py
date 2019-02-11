# pylint: disable=C0103,C0111,C0302,W0212
import datetime


def test_get_timeout(qdbd_connection):
    qdbd_connection.options().set_timeout(datetime.timedelta(
        seconds=1))
    duration = qdbd_connection.options().get_timeout()

    assert duration == datetime.timedelta(seconds=1)
