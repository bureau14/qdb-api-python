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

class QuasardbInfo(unittest.TestCase):
    global cluster, INSECURE_URI
    def test_node_status(self):
        status = cluster.node_status(INSECURE_URI)
        self.assertGreater(len(status), 0)
        self.assertIsNotNone(status.get('overall'))

    def test_node_config(self):
        config = cluster.node_config(INSECURE_URI)
        self.assertGreater(len(config), 0)
        self.assertIsNotNone(config.get('global'))
        self.assertIsNotNone(config.get('local'))

    def test_node_topology(self):
        topology = cluster.node_topology(INSECURE_URI)
        self.assertGreater(len(topology), 0)
        self.assertIsNotNone(topology.get('predecessor'))
        self.assertIsNotNone(topology.get('center'))
        self.assertIsNotNone(topology.get('successor'))

if __name__ == '__main__':
    test_report_directory = re.search(".*qdb-api-python", os.getcwd()).group() + "\\build\\test-reports\\"

    import xmlrunner
    unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory + 'test-reports'))()