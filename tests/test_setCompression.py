# pylint: disable=C0103,C0111,C0302,W0212,W0702

import pytest
import quasardb


def test_compression_none(qdbd_connection):
    qdbd_connection.options().set_compression(
        quasardb.Options.Compression.Disabled)


def test_compression_fast(qdbd_connection):
    qdbd_connection.options().set_compression(quasardb.Options.Compression.Fast)


def test_compression_invalid(qdbd_connection):
    with pytest.raises(TypeError):
        qdbd_connection.options().set_compression(123)
