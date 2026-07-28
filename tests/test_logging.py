import re
import pytest
import quasardb
import logging
import time
import numpy as np
import quasardb.numpy as qdbnp


@pytest.mark.skip(reason="Works, but flaky")
def test_native_logging_output(blob_entry, random_blob, caplog):
    caplog.set_level(logging.DEBUG)

    def found(xs):
        messages = [lr.message for lr in xs]
        modules = [lr.name for lr in xs]
        return (
            "quasardb.native" in modules
            and "requested clients broker service run" in messages
        )

    max_retries = 30
    for i in range(max_retries):
        # Do something that will flush the logs
        try:
            assert blob_entry.get() == random_blob
            blob_entry.remove()
        except quasardb.Error:
            blob_entry.put(random_blob)

        if found(caplog.records):
            break

        time.sleep(1)

    assert found(caplog.records)


@pytest.mark.skip(reason="TODO: Should only be enabled for CMAKE_BUILD_TYPE=Debug")
def test_invalid_utf8_logs_qdb3361(qdbd_connection, caplog):
    caplog.set_level(logging.DEBUG)

    # Generate invalid UTF-8 binary string which we'll use as table name
    tablename = b"foo\x80abc"

    # Verify this does, in fact, raise an error in case when it's interpreted
    # as UTF-8.
    with pytest.raises(UnicodeDecodeError):
        tablename.decode("utf-8")

    # Now, use this tablename to do some batch insertion
    table = qdbd_connection.table(tablename)
    try:
        table.remove()
    except quasardb.AliasNotFoundError:
        # AliasNotFound
        pass

    col = quasardb.ColumnInfo(quasardb.ColumnType.Double, "the_double")
    table.create([col])

    qdbnp.write_arrays(
        {"the_double": np.array([1.234])},
        qdbd_connection,
        table,
        index=np.array([np.datetime64("2020-01-01", "ns")]),
        infer_types=False,
    )
    time.sleep(4)

    # This call right here triggers the 'bad' logs to be flushed into the python
    # userspace.
    table.remove()

    # Look at the captured log messages to verify our 'broken' utf-8 code is, in fact,
    # in the logs.
    seen = False
    for lr in caplog.records:
        seen = seen or "\x80" in lr.message

    assert seen
