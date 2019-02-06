# pylint: disable=C0103,C0111,C0302,W0212
import os
import sys
import subprocess
import time
import pytest
import random
import glob
import string

import quasardb

global locked
locked = False

def set_lock():
    global locked
    locked = True

def release_lock():
    global locked
    locked = False

def get_lock_status():
    return locked

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
    process.kill()
    process.wait()

def connect(uri):
    return quasardb.Cluster(uri)

def config():
    return {"uri":
            {"insecure": "qdb://127.0.0.1:28360",
             "secure": "qdb://127.0.0.1:28361"}}

entry_gen = UniqueEntryNameGenerator()

@pytest.fixture
def qdbd_connection(scope="module"):
    return connect("qdb://127.0.0.1:28360")


@pytest.fixture
def random_string():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

@pytest.fixture
def entry_name(random_string):
    return random_string

@pytest.fixture
def random_blob(random_string):
    return random_string.encode('UTF-8')

@pytest.fixture
def blob_entry(qdbd_connection, entry_name):
    return qdbd_connection.blob(entry_name)

def setUpModule():
    global INSECURE_URI  # pylint: disable=W0601
    global SECURE_URI  # pylint: disable=W0601

    global SECURE_USER_NAME, SECURE_USER_PRIVATE_KEY, SECURE_CLUSTER_PUBLIC_KEY
    global cluster  # pylint: disable=W0601
    global __CLUSTERD  # pylint: disable=W0601
    global __SECURE_CLUSTERD  # pylint: disable=W0601

    global entry_gen  # pylint: disable=W0601

    global root_directory
    global qdb_directory

    SECURE_USER_NAME = 'qdb-api-python'
    SECURE_USER_PRIVATE_KEY = 'SoHHpH26NtZvfq5pqm/8BXKbVIkf+yYiVZ5fQbq1nbcI='
    SECURE_CLUSTER_PUBLIC_KEY = 'Pb+d1o3HuFtxEb5uTl9peU89ze9BZTK9f8KdKr4k7zGA='

    entry_gen = UniqueEntryNameGenerator()
    test_data_directory = os.path.join(os.path.split(__file__)[0], 'data')
    root_directory = os.path.join(os.path.split(__file__)[0], '..')
    qdb_directory = os.path.join(root_directory, 'qdb', 'bin')
    __current_port = 3000
    __current_port += 1
    secure_endpoint = '127.0.0.1:' + str(__current_port)
    __current_port += 1

    INSECURE_URI = ""
    SECURE_URI = ""

    # don't save anything to disk
    common_parameters = '--storage-engine=transient'
    FNULL = open(os.devnull, 'w')

    __CLUSTERD = subprocess.Popen(
        [os.path.join(qdb_directory, 'qdbd'),
         common_parameters,
         '--address=' + insecure_endpoint,
         '--security=false'],
        stdout=FNULL, stderr=FNULL)
    if __CLUSTERD.pid == 0:
        raise Exception("daemon", "cannot run insecure daemon")

    # startup may take a couple of seconds, temporize to make sure the
    # connection will be accepted

    __SECURE_CLUSTERD = subprocess.Popen(
        [os.path.join(qdb_directory, 'qdbd'),
         common_parameters, '--address=' + secure_endpoint,
         '--security=true',
         '--cluster-private-file=' +
         os.path.join(test_data_directory, 'cluster-secret-key.txt'),
         '--user-list=' + os.path.join(test_data_directory, 'users.txt')], stdout=FNULL, stderr=FNULL)
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
    except:
        __cleanupProcess(__CLUSTERD)
        __cleanupProcess(__SECURE_CLUSTERD)
        raise

def tearDownModule():
    __cleanupProcess(__CLUSTERD)
    __cleanupProcess(__SECURE_CLUSTERD)

def init():
    set_lock()
    setUpModule()

def terminate():
    tearDownModule()
    release_lock()
