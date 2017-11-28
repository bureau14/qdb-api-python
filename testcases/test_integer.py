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


for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings

class QuasardbInteger(unittest.TestCase):
    
    def __init__(self, methodName="runTest"):
        super(QuasardbInteger, self).__init__(methodName)
        self.entry_value = 0

    def setUp(self):
        entry_name = settings.entry_gen.next()
        self.i = settings.cluster.integer(entry_name)
        self.entry_value += 1

    def test_put_get_and_remove(self):
        self.i.put(self.entry_value)

        self.assertRaises(quasardb.OperationError,
                          self.i.put, self.entry_value)

        got = self.i.get()
        self.assertEqual(self.entry_value, got)
        self.i.remove()
        self.assertRaises(quasardb.OperationError, self.i.get)
        self.assertRaises(quasardb.OperationError, self.i.remove)

    def test_update(self):
        self.i.update(self.entry_value)
        got = self.i.get()
        self.assertEqual(self.entry_value, got)
        self.entry_value = 2
        self.i.update(self.entry_value)
        got = self.i.get()
        self.assertEqual(self.entry_value, got)
        self.i.remove()

    def test_add(self):
        self.i.put(self.entry_value)

        got = self.i.get()
        self.assertEqual(self.entry_value, got)

        entry_increment = 10

        self.i.add(entry_increment)

        got = self.i.get()
        self.assertEqual(self.entry_value + entry_increment, got)

        entry_decrement = -100

        self.i.add(entry_decrement)

        got = self.i.get()
        self.assertEqual(self.entry_value + entry_increment + entry_decrement,
                         got)

        self.i.remove()
if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()
