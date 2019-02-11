# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
import pytest
import quasardb


def test_put(blob_entry, random_string):
    blob_entry.put(random_string)


def test_put_throws_exception_when_called_twice(blob_entry, random_blob):
    blob_entry.put(random_blob)
    with pytest.raises(quasardb.Error):
        blob_entry.put(random_blob)


def test_get(blob_entry, random_blob):
    blob_entry.put(random_blob)

    got = blob_entry.get()

    assert random_blob == got


def test_remove(blob_entry, random_blob):
    blob_entry.put(random_blob)
    blob_entry.remove()

    with pytest.raises(quasardb.Error):
        blob_entry.get()


def test_remove_throws_exception_when_called_twice(blob_entry, random_blob):
    blob_entry.put(random_blob)
    blob_entry.remove()

    with pytest.raises(quasardb.Error):
        blob_entry.remove()


def test_update(blob_entry, random_blob):
    blob_entry.update(random_blob)
    got = blob_entry.get()
    assert random_blob == got

    new_entry_content = b"It's the new style"
    blob_entry.update(new_entry_content)
    got = blob_entry.get()
    assert got == new_entry_content

    blob_entry.remove()

    new_entry_content = b"I'm never in training, my voice is not straining"
    blob_entry.update(new_entry_content)
    got = blob_entry.get()

    assert got == new_entry_content


def test_get_and_update(blob_entry, random_blob):
    blob_entry.put(random_blob)
    got = blob_entry.get()
    assert random_blob == got

    entry_new_content = b"new stuff"
    blob_entry.update(entry_new_content)
    got = blob_entry.get()

    assert entry_new_content == got

    got = blob_entry.get()
    assert entry_new_content == got
    blob_entry.remove()


def test_get_and_remove(blob_entry, random_blob):
    blob_entry.put(random_blob)

    got = blob_entry.get_and_remove()
    assert random_blob == got

    with pytest.raises(quasardb.Error):
        blob_entry.get()


def test_remove_if(blob_entry, random_blob):
    blob_entry.put(random_blob)
    got = blob_entry.get()

    assert random_blob == got

    with pytest.raises(quasardb.Error):
        blob_entry.remove_if(random_blob + b'a')

    got = blob_entry.get()
    assert random_blob == got

    blob_entry.remove_if(random_blob)

    with pytest.raises(quasardb.Error):
        blob_entry.get()


def test_compare_and_swap(blob_entry, random_blob):

    blob_entry.put(random_blob)
    got = blob_entry.get()

    assert random_blob == got

    entry_new_content = b"new stuff"
    got = blob_entry.compare_and_swap(entry_new_content, entry_new_content)

    assert random_blob == got

    got = blob_entry.compare_and_swap(entry_new_content, random_blob)
    assert len(got) == 0

    got = blob_entry.get()
    assert entry_new_content == got

    blob_entry.remove()
