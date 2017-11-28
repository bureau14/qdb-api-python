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

class QuasardbDeque(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(QuasardbDeque, self).__init__(methodName)
        self.entry_content_front = "front"
        self.entry_content_back = "back"

    def setUp(self):
        entry_name = settings.entry_gen.next()
        self.q = settings.cluster.deque(entry_name)

    def test_empty_queue(self):
        self.assertRaises(quasardb.OperationError, self.q.pop_back)
        self.assertRaises(quasardb.OperationError, self.q.pop_front)
        self.assertRaises(quasardb.OperationError, self.q.front)
        self.assertRaises(quasardb.OperationError, self.q.back)
        self.assertRaises(quasardb.OperationError, self.q.size)

    def test_push_front_single_element(self):
        self.q.push_front(self.entry_content_front)

        self.assertEqual(1, self.q.size())

        got = self.q.back()
        self.assertEqual(self.entry_content_front, got)

        got = self.q.front()
        self.assertEqual(self.entry_content_front, got)

    def test_push_back_single_element(self):
        self.q.push_back(self.entry_content_back)

        self.assertEqual(1, self.q.size())

        got = self.q.back()
        self.assertEqual(self.entry_content_back, got)

        got = self.q.front()
        self.assertEqual(self.entry_content_back, got)

    def test_sequence(self):
        """
        A series of test to make sure back and front operations are properly wired
        """
        entry_content_canary = "canary"

        self.q.push_back(self.entry_content_back)
        self.assertEqual(1, self.q.size())
        self.q.push_front(self.entry_content_front)
        self.assertEqual(2, self.q.size())

        got = self.q.back()
        self.assertEqual(self.entry_content_back, got)
        got = self.q.front()
        self.assertEqual(self.entry_content_front, got)

        self.q.push_back(entry_content_canary)
        self.assertEqual(3, self.q.size())

        got = self.q.back()
        self.assertEqual(entry_content_canary, got)
        got = self.q.front()
        self.assertEqual(self.entry_content_front, got)

        got = self.q.pop_back()
        self.assertEqual(entry_content_canary, got)
        self.assertEqual(2, self.q.size())

        got = self.q.back()
        self.assertEqual(self.entry_content_back, got)
        got = self.q.front()
        self.assertEqual(self.entry_content_front, got)

        self.q.push_front(entry_content_canary)
        self.assertEqual(3, self.q.size())

        got = self.q.back()
        self.assertEqual(self.entry_content_back, got)
        got = self.q.front()
        self.assertEqual(entry_content_canary, got)

        got = self.q.pop_front()
        self.assertEqual(entry_content_canary, got)
        self.assertEqual(2, self.q.size())

        got = self.q.back()
        self.assertEqual(self.entry_content_back, got)
        got = self.q.front()
        self.assertEqual(self.entry_content_front, got)

        got = self.q.pop_back()
        self.assertEqual(self.entry_content_back, got)
        self.assertEqual(1, self.q.size())

        got = self.q.back()
        self.assertEqual(self.entry_content_front, got)
        got = self.q.front()
        self.assertEqual(self.entry_content_front, got)

        got = self.q.pop_back()
        self.assertEqual(self.entry_content_front, got)
        self.assertEqual(0, self.q.size())

        self.assertRaises(quasardb.OperationError, self.q.pop_back)
        self.assertRaises(quasardb.OperationError, self.q.pop_front)
        self.assertRaises(quasardb.OperationError, self.q.front)
        self.assertRaises(quasardb.OperationError, self.q.back)

if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' , 'build' , 'test' , 'test-reports')
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()
