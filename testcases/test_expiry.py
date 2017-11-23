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

class UniqueEntryNameGenerator(object):  # pylint: disable=R0903

    def __init__(self):
        self.__prefix = "entry_"
        self.__counter = 0

    def __iter__(self):
        return self

    def next(self):
        self.__counter += 1
        return self.__prefix + str(self.__counter)

def __cleanupProcess(process):
    process.terminate()
    process.wait()


def setUpModule():

    global INSECURE_URI  # pylint: disable=W0601
    global SECURE_URI  # pylint: disable=W0601

    global cluster  # pylint: disable=W0601
    global __CLUSTERD  # pylint: disable=W0601
    global __SECURE_CLUSTERD  # pylint: disable=W0601

    global entry_gen  # pylint: disable=W0601

    entry_gen = UniqueEntryNameGenerator()
    root_directory = re.search(".*qdb-api-python", os.getcwd()).group()
    qdb_directory = root_directory + "\\qdb\\bin\\"

    __current_port = 3000
    insecure_endpoint = '127.0.0.1:' + str(__current_port)
    __current_port += 1
    secure_endpoint = '127.0.0.1:' + str(__current_port)
    __current_port += 1

    INSECURE_URI = ""
    SECURE_URI = ""

    # don't save anything to disk
    common_parameters = '--transient'



    __CLUSTERD = subprocess.Popen(
        [os.path.join(qdb_directory , 'qdbd'),
         common_parameters, '--address=' + insecure_endpoint, '--security=false'])
    if __CLUSTERD.pid == 0:
        raise Exception("daemon", "cannot run insecure daemon")

    # startup may take a couple of seconds, temporize to make sure the
    # connection will be accepted

    __SECURE_CLUSTERD = subprocess.Popen(
        [os.path.join(qdb_directory , 'qdbd'),
         common_parameters, '--address=' + secure_endpoint,
         '--cluster-private-file=' +
         os.path.join(root_directory  , 'cluster-secret-key.txt'),
         '--user-list=' + os.path.join(root_directory , 'users.txt')])
    if __SECURE_CLUSTERD.pid == 0:
        __cleanupProcess(__CLUSTERD)
        raise Exception("daemon", "cannot run secure daemon")

    time.sleep(2)
    __CLUSTERD.poll()
    if __CLUSTERD.returncode != None:
        __cleanupProcess(__SECURE_CLUSTERD)
        raise Exception("daemon", "error while running insecure daemon (returned {})"
                        .format(__CLUSTERD.returncode))

    __SECURE_CLUSTERD.poll()
    if __SECURE_CLUSTERD.returncode != None:
        __cleanupProcess(__CLUSTERD)
        raise Exception("daemon", "error while running secure daemon (returned {})"
                        .format(__SECURE_CLUSTERD.returncode))

    INSECURE_URI = 'qdb://' + insecure_endpoint
    SECURE_URI = 'qdb://' + secure_endpoint

    try:
        cluster = quasardb.Cluster(INSECURE_URI)
    except (BaseException, quasardb.Error):
        __cleanupProcess(__CLUSTERD)
        __cleanupProcess(__SECURE_CLUSTERD)
        raise


def tearDownModule():
    __cleanupProcess(__CLUSTERD)
    __cleanupProcess(__SECURE_CLUSTERD)


def _make_expiry_time(td):
    # expires in one minute
    now = datetime.datetime.now(quasardb.tz)
    # get rid of the microsecond for the testcases
    return now + td - datetime.timedelta(microseconds=now.microsecond)

class QuasardbExpiry(unittest.TestCase):
    global entry_gen, cluster
    def test_expires_at(self):
        """
        Test for expiry.
        We want to make sure, in particular, that the conversion from Python datetime is right.
        """

        # add one entry
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        # entry does not exist yet
        self.assertRaises(quasardb.OperationError, b.get_expiry_time)

        b.put(entry_content)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, datetime.datetime.fromtimestamp(0, quasardb.tz))

        future_exp = _make_expiry_time(datetime.timedelta(minutes=1))
        b.expires_at(future_exp)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

    def test_expires_from_now(self):
        # add one entry
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, datetime.datetime.fromtimestamp(0, quasardb.tz))

        b.expires_at(None)

        # expires in one minute from now
        future_exp = 60
        future_exp_ms = future_exp * 1000
        b.expires_from_now(future_exp_ms)

        # We use a wide 10s interval for the check, because we have no idea at which speed
        # these testcases may run in debug. This will be enough however to check that
        # the interval has properly been converted and the time zone is
        # correct.
        future_exp_lower_bound = datetime.datetime.now(
            quasardb.tz) + datetime.timedelta(seconds=future_exp - 10)
        future_exp_higher_bound = future_exp_lower_bound + \
            datetime.timedelta(seconds=future_exp + 10)

        exp = b.get_expiry_time()
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertIsInstance(exp, datetime.datetime)
        self.assertLess(future_exp_lower_bound, exp)
        self.assertLess(exp, future_exp_higher_bound)

    def test_methods(self):
        """
        Test that methods that accept an expiry date properly forward the value
        """
        entry_name = entry_gen.next()
        entry_content = "content"

        future_exp = _make_expiry_time(datetime.timedelta(minutes=1))

        b = cluster.blob(entry_name)
        b.put(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = _make_expiry_time(datetime.timedelta(minutes=2))

        b.update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = _make_expiry_time(datetime.timedelta(minutes=3))

        b.get_and_update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = _make_expiry_time(datetime.timedelta(minutes=4))

        b.compare_and_swap(entry_content, entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        b.remove()

    def test_trim_all_at_end(self):
        try:
            cluster.trim_all(datetime.timedelta(minutes=1))
        except:  # pylint: disable=W0702
            self.fail('cluster.trim_all raised an unexpected exception')

if __name__ == '__main__':

    test_report_directory = re.search(".*qdb-api-python", os.getcwd()).group() + "\\build\\test-reports\\"
    import xmlrunner
    unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory + 'test-reports'))()