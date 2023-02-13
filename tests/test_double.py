# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
import pytest
import quasardb


def test_put(double_entry, random_double):
    double_entry.put(random_double)


def test_get(double_entry, random_double):
    double_entry.put(random_double)
    got = double_entry.get()

    assert random_double == got


def test_update(double_entry, random_double):
    double_entry.put(random_double)
    got = double_entry.get()

    assert random_double == got

    new_double = 42
    double_entry.update(new_double)
    got = double_entry.get()

    assert 42 == got


def test_add(double_entry, random_double):
    double_entry.put(random_double)
    got = double_entry.get()

    assert random_double == got

    double_entry.add(42)
    got = double_entry.get()

    assert (random_double + 42) == got
