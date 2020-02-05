import re
import pytest
import quasardb
import logging
import time

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
