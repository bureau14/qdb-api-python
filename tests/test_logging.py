import re
import pytest
import quasardb
import logging
import time
import numpy as np


def test_native_logging_output(blob_entry, random_string, caplog):
    caplog.set_level(logging.DEBUG)

    # Do something that generates native logs
    blob_entry.put(random_string)

    # Wait for QuasarDB to flush the logs
    time.sleep(6)

    # Do something that will flush the logs
    blob_entry.remove()

    # Gather all module names that have generated logs, we expect some stuff from at
    # least quasardb.native which we want to test here
    messages = list(lr.message for lr in caplog.records)
    modules = list(lr.name for lr in caplog.records)

    print("messages: ", messages)
    print("modules: ", modules)

    assert 'quasardb.native' in modules
    assert 'requested clients broker service run' in messages


def test_invalid_utf8_logs_qdb3361(qdbd_connection, caplog):
    caplog.set_level(logging.DEBUG)

    # Generate invalid UTF-8 binary string which we'll use as table name
    tablename = b'foo\x80abc'

    # Verify this does, in fact, raise an error in case when it's interpreted
    # as UTF-8.
    with pytest.raises(UnicodeDecodeError):
        tablename.decode("utf-8")

    # Now, use this tablename to do some batch insertion
    table = qdbd_connection.table(tablename)
    try:
        table.remove()
    except quasardb.Error:
        # AliasNotFound
        pass

    col = quasardb.ColumnInfo(quasardb.ColumnType.Double, "the_double")
    table.create([col])

    batchcol = [quasardb.BatchColumnInfo(tablename, "the_double", 10)]
    inserter = qdbd_connection.inserter(batchcol)

    inserter.start_row(np.datetime64('2020-01-01', 'ns'))
    inserter.set_double(0, 1.234)

    inserter.push()
    time.sleep(4)

    # This call right here triggers the 'bad' logs to be flushed into the python
    # userspace.
    table.remove()

    # Look at the captured log messages to verify our 'broken' utf-8 code is, in fact,
    # in the logs.
    seen = False
    for lr in caplog.records:
        seen = seen or '\x80' in lr.message

    assert seen
