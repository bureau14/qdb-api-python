
import re
import unittest
from settings import quasardb

class QuasardbCluster(unittest.TestCase):
    def test_capi_version_check():
        # This will throw everytime
        # The version check is the most critical step so it is done first.
        # We thus just have to check that the error we get is related to
        # this check to figure how it went.
        self.assertRaisesRegexp(quasardb.Error, "^((?!C API version).)*$",
            quasardb.Cluster, "_qdb_://invaliduri")
