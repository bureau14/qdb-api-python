# pylint: disable=C0103,C0111,C0302,W0212
import datetime

import pytest
import quasardb


def test_can_compact_full_and_abort(qdbd_connection):
    try:
        qdbd_connection.compact_full()
    finally:
        qdbd_connection.compact_abort()
        qdbd_connection.wait_for_compaction()

def test_can_get_compaction_progress(qdbd_connection):
    try:
        qdbd_connection.compact_full()

        progress = qdbd_connection.compact_progress()
        assert progress > 0 and progress <= 100

    finally:
        qdbd_connection.compact_abort()
        qdbd_connection.wait_for_compaction()
