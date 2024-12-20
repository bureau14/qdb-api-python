# pylint: disable=C0103,C0111,C0302,W0212


def test_empty_suffix(qdbd_connection):

    res = qdbd_connection.suffix_get("testazeazeaze", 10)
    assert len(res) == 0

    res = qdbd_connection.suffix_count("testazeazeaze")
    assert res == 0


def test_find_one(qdbd_connection, entry_name, random_string):
    dat_suffix = random_string

    entry_name = entry_name + dat_suffix
    entry_content = "content"

    b = qdbd_connection.blob(entry_name)
    b.put(entry_content)

    res = qdbd_connection.suffix_get(dat_suffix, 10)
    assert len(res) == 1
    assert res[0] == entry_name

    assert qdbd_connection.suffix_count(dat_suffix) == 1
