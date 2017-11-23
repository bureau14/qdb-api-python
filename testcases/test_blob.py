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

for root, dirnames, filenames in os.walk(os.path.join(re.search(".*qdb-api-python", os.getcwd()).group(), 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
from settings import entry_gen, cluster
# class UniqueEntryNameGenerator(object):  # pylint: disable=R0903
#
#     def __init__(self):
#         self.__prefix = "entry_"
#         self.__counter = 0
#
#     def __iter__(self):
#         return self
#
#     def next(self):
#         self.__counter += 1
#         return self.__prefix + str(self.__counter)
#
# def __cleanupProcess(process):
#     process.terminate()
#     process.wait()
#
#
# def setUpModule():
#
#     global INSECURE_URI  # pylint: disable=W0601
#     global SECURE_URI  # pylint: disable=W0601
#
#     global cluster  # pylint: disable=W0601
#     global __CLUSTERD  # pylint: disable=W0601
#     global __SECURE_CLUSTERD  # pylint: disable=W0601
#
#     global entry_gen  # pylint: disable=W0601
#
#     entry_gen = UniqueEntryNameGenerator()
#
#     root_directory = re.search(".*qdb-api-python", os.getcwd()).group()
#     qdb_directory = root_directory + "\\qdb\\bin\\"
#
#     __current_port = 3000
#     insecure_endpoint = '127.0.0.1:' + str(__current_port)
#     __current_port += 1
#     secure_endpoint = '127.0.0.1:' + str(__current_port)
#     __current_port += 1
#
#     INSECURE_URI = ""
#     SECURE_URI = ""
#
#     # don't save anything to disk
#     common_parameters = '--transient'
#
#
#
#     __CLUSTERD = subprocess.Popen(
#         [os.path.join(qdb_directory , 'qdbd'),
#          common_parameters, '--address=' + insecure_endpoint, '--security=false'])
#     if __CLUSTERD.pid == 0:
#         raise Exception("daemon", "cannot run insecure daemon")
#
#     # startup may take a couple of seconds, temporize to make sure the
#     # connection will be accepted
#
#     __SECURE_CLUSTERD = subprocess.Popen(
#         [os.path.join(qdb_directory , 'qdbd'),
#          common_parameters, '--address=' + secure_endpoint,
#          '--cluster-private-file=' +
#          os.path.join(root_directory  , 'cluster-secret-key.txt'),
#          '--user-list=' + os.path.join(root_directory , 'users.txt')])
#     if __SECURE_CLUSTERD.pid == 0:
#         __cleanupProcess(__CLUSTERD)
#         raise Exception("daemon", "cannot run secure daemon")
#
#     time.sleep(2)
#     __CLUSTERD.poll()
#     if __CLUSTERD.returncode != None:
#         __cleanupProcess(__SECURE_CLUSTERD)
#         raise Exception("daemon", "error while running insecure daemon (returned {})"
#                         .format(__CLUSTERD.returncode))
#
#     __SECURE_CLUSTERD.poll()
#     if __SECURE_CLUSTERD.returncode != None:
#         __cleanupProcess(__CLUSTERD)
#         raise Exception("daemon", "error while running secure daemon (returned {})"
#                         .format(__SECURE_CLUSTERD.returncode))
#
#     INSECURE_URI = 'qdb://' + insecure_endpoint
#     SECURE_URI = 'qdb://' + secure_endpoint
#
#     try:
#         cluster = quasardb.Cluster(INSECURE_URI)
#     except (BaseException, quasardb.Error):
#         __cleanupProcess(__CLUSTERD)
#         __cleanupProcess(__SECURE_CLUSTERD)
#         raise
#
#
# def tearDownModule():
#     __cleanupProcess(__CLUSTERD)
#     __cleanupProcess(__SECURE_CLUSTERD)

class QuasardbBlob(unittest.TestCase):
    global entry_gen, cluster
    def setUp(self):
        self.entry_name = entry_gen.next()
        self.entry_content = "content"

        self.b = cluster.blob(self.entry_name)

    def test_trim_all_at_begin(self):
        try:
            cluster.trim_all(datetime.timedelta(minutes=1))
        except quasardb.Error:
            self.fail('cluster.trim_all raised an unexpected exception')

    def test_put(self):
        self.b.put(self.entry_content)

    def test_put_throws_exception__when_called_twice(self):
        self.b.put(self.entry_content)

        self.assertRaises(quasardb.OperationError,
                          self.b.put, self.entry_content)

    def test_get(self):
        self.b.put(self.entry_content)

        got = self.b.get()

        self.assertEqual(self.entry_content, got)

    def test_remove(self):
        self.b.put(self.entry_content)

        self.b.remove()

        self.assertRaises(quasardb.OperationError, self.b.get)

    def test_put_after_remove(self):
        self.b.put(self.entry_content)

        self.b.remove()

        try:
            self.b.put(self.entry_content)
        except:  # pylint: disable=W0702
            self.fail(msg='blob.put should not have raised an exception')

    def test_remove_throws_exception__when_called_twice(self):
        self.b.put(self.entry_content)
        self.b.remove()

        self.assertRaises(quasardb.OperationError, self.b.remove)

    def test_update(self):
        self.b.update(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        new_entry_content = "it's a new style"
        self.b.update(new_entry_content)
        got = self.b.get()
        self.assertEqual(new_entry_content, got)
        self.b.remove()

        new_entry_content = ''.join('%c' % x for x in xrange(256))
        self.assertEqual(len(new_entry_content), 256)

        self.b.update(new_entry_content)
        got = self.b.get()
        self.assertEqual(new_entry_content, got)

    def test_get_and_update(self):
        self.b.put(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)

        entry_new_content = "new stuff"
        got = self.b.get_and_update(entry_new_content)
        self.assertEqual(self.entry_content, got)
        got = self.b.get()
        self.assertEqual(entry_new_content, got)

        self.b.remove()

    def test_get_and_remove(self):
        self.b.put(self.entry_content)

        got = self.b.get_and_remove()
        self.assertEqual(self.entry_content, got)
        self.assertRaises(quasardb.OperationError, self.b.get)

    def test_remove_if(self):
        self.b.put(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        self.assertRaises(quasardb.OperationError,
                          self.b.remove_if, self.entry_content + 'a')
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        self.b.remove_if(self.entry_content)
        self.assertRaises(quasardb.OperationError, self.b.get)

    def test_compare_and_swap(self):
        self.b.put(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        entry_new_content = "new stuff"
        got = self.b.compare_and_swap(entry_new_content, entry_new_content)
        self.assertEqual(self.entry_content, got)
        # unchanged because unmatched
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        got = self.b.compare_and_swap(entry_new_content, self.entry_content)
        self.assertEqual(None, got)
        # changed because matched
        got = self.b.get()
        self.assertEqual(entry_new_content, got)

        self.b.remove()

# if __name__ == '__main__':
#     test_report_directory = re.search(".*qdb-api-python", os.getcwd()).group() + "\\build\\test-reports\\"
#     import xmlrunner
#     unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
#         output=test_report_directory + 'test-reports'))()