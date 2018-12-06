
import re
import unittest
from settings import quasardb

__INVALID_URI__ = "_qdb_://invaliduri"

class QuasardbCluster(unittest.TestCase):
    def test_capi_version_check():
        # This will throw everytime
        # The version check is the most critical step so it is done first.
        # We thus just have to check that the error we get is related to
        # this heck to figure how it went.
        versionCheckPassed = False
        msg = ""
        try:
            c = quasardb.Cluster(__INVALID_URI__)
        except quasardb.Error as e:
            msg = e.what()
            versionCheckPassed = not "QuasarDB C API version" in msg
        self.assertTrue(versionCheckPassed, message=msg)
