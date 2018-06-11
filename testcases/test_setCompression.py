# pylint: disable=C0103,C0111,C0302,W0212
from functools import reduce  # pylint: disable=W0622
import datetime
import os
import subprocess
import sys
import time
import unittest
import calendar
import pytz


for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings


class QuasardbClusterSetCompression(unittest.TestCase):
    def test_compression_none(self):
        try:
            settings.cluster.set_compression(quasardb.Compression.none)
        except:
            self.fail(
                msg='cluster.set_compression should not have raised an exception')

    def test_compression_fast(self):
        try:
            settings.cluster.set_compression(quasardb.Compression.fast)
        except:
            self.fail(
                msg='cluster.set_compression should not have raised an exception')

    def test_compression_invalid(self):
        self.assertRaises(quasardb.InputError,
                          settings.cluster.set_compression, 123)


if __name__ == '__main__':
    if settings.get_lock_status() == False:
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(
            __file__)[0], '..', 'build', 'test', 'test-reports')
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
            output=test_report_directory), exit=False)
        settings.terminate()
