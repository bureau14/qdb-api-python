# pylint: disable=C0103,C0111,C0302,W0212

def test_empty_prefix(qdbd_connection):

    res = qdbd_connection.prefix_get("testazeazeaze", 10)
    assert len(res) == 0

    res = qdbd_connection.prefix_count("testazeazeaze")
    assert res == 0


def test_find_one(qdbd_connection, entry_name, random_string):
    dat_prefix = random_string

    entry_name = dat_prefix + entry_name
    entry_content = "content"

    b = qdbd_connection.blob(entry_name)
    b.put(entry_content)

    res = qdbd_connection.prefix_get(dat_prefix, 10)
    assert len(res) == 1
    assert res[0] == entry_name

    assert qdbd_connection.prefix_count(dat_prefix) == 1
