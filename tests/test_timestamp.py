# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
import pytest
import quasardb
import datetime

def test_put(timestamp_entry, random_timestamp):
    timestamp_entry.put(random_timestamp)


def test_get(timestamp_entry, random_timestamp):
    timestamp_entry.put(random_timestamp)
    got = timestamp_entry.get()

    assert random_timestamp == got


def test_update(timestamp_entry, random_timestamp):
    timestamp_entry.put(random_timestamp)
    got = timestamp_entry.get()

    assert random_timestamp == got


    new_timestamp = random_timestamp + datetime.timedelta(days=42)

    timestamp_entry.update(new_timestamp)
    got = timestamp_entry.get()

    assert new_timestamp == got
