# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
import pytest
import quasardb

def test_put(integer_entry, random_integer):
    integer_entry.put(random_integer)

def test_get(integer_entry, random_integer):
    integer_entry.put(random_integer)
    got = integer_entry.get()

    assert random_integer == got

def test_update(integer_entry, random_integer):
    integer_entry.put(random_integer)
    got = integer_entry.get()

    assert random_integer == got

    new_integer = 42
    integer_entry.update(new_integer)
    got = integer_entry.get()

    assert 42 == got

def test_add(integer_entry, random_integer):
    integer_entry.put(random_integer)
    got = integer_entry.get()

    assert random_integer == got

    integer_entry.add(42)
    got = integer_entry.get()

    assert (random_integer + 42) == got
