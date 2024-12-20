# pylint: disable=C0103,C0111,C0302,W0212
import pytest
import quasardb


def test_types_tag(qdbd_connection, blob_entry, random_blob, tag_name):
    blob_entry.put(random_blob)
    blob_entry.attach_tag(tag_name)

    res = qdbd_connection.find("find(tag='" + tag_name + "')").run()

    assert len(res) == 1

    res = qdbd_connection.find(
        "find(tag='" + tag_name + "' AND type=blob)").run()

    assert len(res) == 1

    res = qdbd_connection.find(
        "find(tag='" + tag_name + "' AND type=integer)").run()

    assert len(res) == 0


def test_types_tags(qdbd_connection, blob_entry, random_blob, tag_names):
    blob_entry.put(random_blob)
    blob_entry.attach_tags(tag_names)

    res = qdbd_connection.find("find(tag='" + tag_names[0] + "')").run()
    assert len(res) == 1

    res = qdbd_connection.find("find(tag='" + tag_names[1] + "')").run()
    assert len(res) == 1

    res = qdbd_connection.find(
        "find(tag='" +
        tag_names[0] +
        "' AND tag='" +
        tag_names[1] +
        "')").run()

    assert len(res) == 1
