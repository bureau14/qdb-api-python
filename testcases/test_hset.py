# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
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


class QuasardbHSet(unittest.TestCase):
    def setUp(self):
        self.entry_content = "content"
        entry_name = settings.entry_gen.next()
        self.hset = settings.cluster.hset(entry_name)

    def test_contains_throws__when_does_not_exist(self):
        self.assertRaises(quasardb.OperationError,
                          self.hset.contains, self.entry_content)

    def test_insert_does_not_throw__when_does_not_exist(self):
        try:
            self.hset.insert(self.entry_content)
        except:  # pylint: disable=W0702
            self.fail(msg='hset.insert should not have raised an exception')

    def test_erase_throws__when_does_not_exist(self):
        self.assertRaises(quasardb.OperationError,
                          self.hset.erase, self.entry_content)

    def test_contains_returns_false__after_erase(self):
        self.hset.insert(self.entry_content)
        self.hset.erase(self.entry_content)

        self.assertFalse(self.hset.contains(self.entry_content))

    def test_erase_throws__when_called_twice(self):
        self.hset.insert(self.entry_content)
        self.hset.erase(self.entry_content)

        self.assertRaises(quasardb.OperationError,
                          self.hset.erase, self.entry_content)

    def test_insert_throws__when_called_twice(self):
        self.hset.insert(self.entry_content)

        self.assertRaises(quasardb.OperationError,
                          self.hset.insert, self.entry_content)

    def test_contains_returns_true__after_insert(self):
        self.hset.insert(self.entry_content)

        self.assertTrue(self.hset.contains(self.entry_content))

    def test_insert_multiple(self):
        for i in xrange(10):
            self.hset.insert(str(i))

        for i in xrange(10):
            self.assertTrue(self.hset.contains(str(i)))

        for i in xrange(10):
            self.hset.erase(str(i))

        # insert again
        for i in xrange(10):
            self.hset.insert(str(i))

    def test_insert_erase_contains(self):
        self.hset.insert(self.entry_content)

        self.assertRaises(quasardb.OperationError,
                          self.hset.insert, self.entry_content)

        self.assertTrue(self.hset.contains(self.entry_content))

        self.hset.erase(self.entry_content)

        self.assertFalse(self.hset.contains(self.entry_content))

        self.hset.insert(self.entry_content)

        self.assertTrue(self.hset.contains(self.entry_content))

        self.hset.erase(self.entry_content)

        self.assertRaises(quasardb.OperationError,
                          self.hset.erase, self.entry_content)

if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()
