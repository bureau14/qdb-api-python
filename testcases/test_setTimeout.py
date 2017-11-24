# pylint: disable=C0103,C0111,C0302,W0212

from builtins import range as xrange, int as long  # pylint: disable=W0622
from functools import reduce  # pylint: disable=W0622
import datetime
import os
import subprocess
import sys
import time
import unittest
import calendar
import pytz
import re

for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings


class QuasardbClusterSetTimeout(unittest.TestCase):
    def test_set_timeout_1_day(self):
        try:
            settings.cluster.set_timeout(datetime.timedelta(days=1))
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.set_timeout should not have raised an exception')

    def test_set_timeout_1_second(self):
        try:
            settings.cluster.set_timeout(datetime.timedelta(seconds=1))
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.set_timeout should not have raised an exception')

    def test_set_timeout_throws__when_timeout_is_1_microsecond(self):
        # timeout must be in milliseconds
        self.assertRaises(quasardb.InputError,
                          settings.cluster.set_timeout, datetime.timedelta(microseconds=1))

    def test_cluster_with_timeout_throws__when_timeout_is_less_than_1_millisecond(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri=settings.INSECURE_URI,
                          timeout=datetime.timedelta(microseconds=1))
if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()