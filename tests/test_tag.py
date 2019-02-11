# pylint: disable=C0103,C0111,C0302,W0212


def test_get_entries(blob_entry, random_blob, tag_name):
    blob_entry.put(random_blob)

    tags = blob_entry.get_tags()
    assert len(tags) == 0

    assert blob_entry.has_tag(tag_name) == False
    assert blob_entry.attach_tag(tag_name)
    assert blob_entry.attach_tag(tag_name) == False

    tags = blob_entry.get_tags()
    assert len(tags) == 1
    assert tags[0] == tag_name

    assert blob_entry.has_tag(tag_name)
    assert blob_entry.detach_tag(tag_name)
    assert blob_entry.detach_tag(tag_name) == False

    tags = blob_entry.get_tags()
    assert len(tags) == 0


def test_tag_multiple(blob_entry, random_blob, tag_names):
    blob_entry.put(random_blob)

    tags = blob_entry.get_tags()
    assert len(tags) == 0

    blob_entry.attach_tags(tag_names)

    assert sorted(blob_entry.get_tags()) == tag_names

    blob_entry.detach_tags(tag_names)

    tags = blob_entry.get_tags()
    assert len(tags) == 0
