# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
import pytest
import quasardb


def test_put(string_entry, random_string):
    string_entry.put(random_string)


def test_put_throws_exception_when_called_twice(string_entry, random_string):
    string_entry.put(random_string)
    with pytest.raises(quasardb.AliasAlreadyExistsError):
        string_entry.put(random_string)


def test_get(string_entry, random_string):
    string_entry.put(random_string)

    got = string_entry.get()

    assert random_string == got


def test_remove(string_entry, random_string):
    string_entry.put(random_string)
    string_entry.remove()

    with pytest.raises(quasardb.AliasNotFoundError):
        string_entry.get()


def test_remove_throws_exception_when_called_twice(string_entry, random_string):
    string_entry.put(random_string)
    string_entry.remove()

    with pytest.raises(quasardb.AliasNotFoundError):
        string_entry.remove()




def test_update(string_entry, random_string):
    string_entry.update(random_string)
    got = string_entry.get()
    assert random_string == got

    new_entry_content = "It's the new style"
    string_entry.update(new_entry_content)
    got = string_entry.get()
    assert got == new_entry_content

    string_entry.remove()

    new_entry_content = "I'm never in training, my voice is not straining"
    string_entry.update(new_entry_content)
    got = string_entry.get()

    assert got == new_entry_content


def test_get_and_update(string_entry, random_string):
    string_entry.put(random_string)
    got = string_entry.get()
    assert random_string == got

    entry_new_content = "new stuff"
    string_entry.update(entry_new_content)
    got = string_entry.get()

    assert entry_new_content == got

    got = string_entry.get()
    assert entry_new_content == got
    string_entry.remove()


def test_get_and_remove(string_entry, random_string):
    string_entry.put(random_string)

    got = string_entry.get_and_remove()
    assert random_string == got

    with pytest.raises(quasardb.AliasNotFoundError):
        string_entry.get()


def test_remove_if(string_entry, random_string):
    string_entry.put(random_string)
    got = string_entry.get()

    assert random_string == got

    with pytest.raises(quasardb.Error):
        string_entry.remove_if(random_string + "a")

    got = string_entry.get()
    assert random_string == got

    string_entry.remove_if(random_string)

    with pytest.raises(quasardb.AliasNotFoundError):
        string_entry.get()


def test_compare_and_swap(string_entry, random_string):

    string_entry.put(random_string)
    got = string_entry.get()

    assert random_string == got

    entry_new_content = "new stuff"
    got = string_entry.compare_and_swap(entry_new_content, entry_new_content)

    assert random_string == got

    got = string_entry.compare_and_swap(entry_new_content, random_string)
    assert len(got) == 0

    got = string_entry.get()
    assert entry_new_content == got

    string_entry.remove()
