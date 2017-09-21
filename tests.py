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

HAS_NUMPY = True
try:
    import numpy
except ImportError:
    HAS_NUMPY = False

for root, dirnames, filenames in os.walk(os.path.join('..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401

# generate an unique entry name for the tests
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


SECURE_USER_NAME = 'qdb-api-python'
SECURE_USER_PRIVATE_KEY = 'SoHHpH26NtZvfq5pqm/8BXKbVIkf+yYiVZ5fQbq1nbcI='
SECURE_CLUSTER_PUBLIC_KEY = 'Pb+d1o3HuFtxEb5uTl9peU89ze9BZTK9f8KdKr4k7zGA='


def setUpModule():
    global INSECURE_URI  # pylint: disable=W0601
    global SECURE_URI  # pylint: disable=W0601

    global cluster  # pylint: disable=W0601
    global __CLUSTERD  # pylint: disable=W0601
    global __SECURE_CLUSTERD  # pylint: disable=W0601

    global entry_gen  # pylint: disable=W0601

    entry_gen = UniqueEntryNameGenerator()

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
        [os.path.join('.', 'qdbd'),
         common_parameters, '--address=' + insecure_endpoint, '--security=false'])
    if __CLUSTERD.pid == 0:
        raise Exception("daemon", "cannot run insecure daemon")

    # startup may take a couple of seconds, temporize to make sure the
    # connection will be accepted

    __SECURE_CLUSTERD = subprocess.Popen(
        [os.path.join(os.getcwd(), 'qdbd'),
         common_parameters, '--address=' + secure_endpoint,
         '--cluster-private-file=' +
         os.path.join(os.getcwd(), '..', '..', 'cluster-secret-key.txt'),
         '--user-list=' + os.path.join(os.getcwd(), '..', '..', 'users.txt')])
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


class QuasardbTest(unittest.TestCase):
    pass


class QuasardbConnection(QuasardbTest):

    def test_connect_throws_input_error__when_uri_is_invalid(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri='invalid_uri')

    def test_connect_throws_connection_error__when_no_cluster_on_given_uri(self):
        self.assertRaises(quasardb.ConnectionError,
                          quasardb.Cluster, uri='qdb://127.0.0.1:1')

    def test_connect_throws_connection_error__when_no_cluster_public_key(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri=SECURE_URI,
                          user_name=SECURE_USER_NAME,
                          user_private_key=SECURE_USER_PRIVATE_KEY)

    def test_connect_throws_connection_error__when_no_user_private_key(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri=SECURE_URI,
                          user_name=SECURE_USER_NAME,
                          cluster_public_key=SECURE_CLUSTER_PUBLIC_KEY)

    def test_connect_throws_connection_error__when_no_user_name(self):
        self.assertRaises(quasardb.RemoteSystemError,
                          quasardb.Cluster, uri=SECURE_URI,
                          user_private_key=SECURE_USER_PRIVATE_KEY,
                          cluster_public_key=SECURE_CLUSTER_PUBLIC_KEY)

    def test_connect_ok__secure_cluster(self):
        try:
            quasardb.Cluster(uri=SECURE_URI,
                             user_name=SECURE_USER_NAME,
                             user_private_key=SECURE_USER_PRIVATE_KEY,
                             cluster_public_key=SECURE_CLUSTER_PUBLIC_KEY)
        except Exception as ex:  # pylint: disable=W0703
            self.fail(msg='Cannot connect to secure cluster: ' + str(ex))


class QuasardbBasic(QuasardbTest):

    def test_build(self):
        build = quasardb.build()
        self.assertGreater(len(build), 0)

    def test_version(self):
        version = quasardb.version()
        self.assertGreater(len(version), 0)

    def test_purge_all_throws_exception__when_disabled_by_default(self):
        self.assertRaises(quasardb.OperationError,
                          cluster.purge_all, datetime.timedelta(minutes=1))

class QuasardbTimeUtils(QuasardbTest):

    def time_zone_nightmare(self):

        moscow_tz = pytz.timezone('Europe/Moscow') 

        record_dates = [
                        datetime(1971, 1, 1, 0, 0, 0, 0, moscow_tz),
                        datetime(1971, 1, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(1971, 1, 1, 0, 0, 0, 0),
                        datetime(2011, 1, 1, 0, 0, 0, 0, moscow_tz),
                        datetime(2012, 1, 1, 0, 0, 0, 0, moscow_tz),
                        datetime(2012, 1, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2012, 1, 1, 0, 0, 0, 0),
                        datetime(2013, 1, 1, 0, 0, 0, 0, moscow_tz),
                        datetime(2014, 1, 1, 0, 0, 0, 0, moscow_tz),
                        datetime(2014, 1, 1, 0, 0, 0, 0),
                        datetime(2015, 1, 1, 0, 0, 0, 0, moscow_tz),
                        datetime(2016, 1, 1, 0, 0, 0, 0, moscow_tz)]

        for r in record_dates:
            self.assertEqual(datetime.datetime.fromtimestamp(_time_to_unix_timestamp(r)), r.astimezone(pytz.UTC))

    def test_duration_converter(self):
        '''
        Test conversion of time durations.
        '''

        self.assertEqual(
            24 * 3600 * 1000, quasardb._duration_to_timeout_ms(datetime.timedelta(days=1)))
        self.assertEqual(
            3600 * 1000, quasardb._duration_to_timeout_ms(datetime.timedelta(hours=1)))
        self.assertEqual(
            60 * 1000, quasardb._duration_to_timeout_ms(datetime.timedelta(minutes=1)))
        self.assertEqual(1000, quasardb._duration_to_timeout_ms(
            datetime.timedelta(seconds=1)))
        self.assertEqual(1, quasardb._duration_to_timeout_ms(
            datetime.timedelta(milliseconds=1)))
        self.assertEqual(0, quasardb._duration_to_timeout_ms(
            datetime.timedelta(microseconds=1)))

    def test_ts_convert(self):
        orig_couple = [(datetime.datetime.now(quasardb.tz),
                        datetime.datetime.now(quasardb.tz))]
        converted_couple = quasardb._convert_time_couples_to_qdb_filtered_range_t_vector(
            orig_couple)
        self.assertEqual(len(converted_couple), 1)
        self.assertEqual(orig_couple[0], quasardb._convert_qdb_filtered_range_t_to_time_couple(
            converted_couple[0]))


class QuasardbClusterSetTimeout(QuasardbTest):

    def test_set_timeout_1_day(self):
        try:
            cluster.set_timeout(datetime.timedelta(days=1))
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.set_timeout should not have raised an exception')

    def test_set_timeout_1_second(self):
        try:
            cluster.set_timeout(datetime.timedelta(seconds=1))
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.set_timeout should not have raised an exception')

    def test_set_timeout_throws__when_timeout_is_1_microsecond(self):
        # timeout must be in milliseconds
        self.assertRaises(quasardb.InputError,
                          cluster.set_timeout, datetime.timedelta(microseconds=1))

    def test_cluster_with_timeout_throws__when_timeout_is_less_than_1_millisecond(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri=INSECURE_URI,
                          timeout=datetime.timedelta(microseconds=1))


class QuasardbClusterSetMaxCardinality(QuasardbTest):

    def test_max_cardinality_ok(self):
        try:
            cluster.set_max_cardinality(140000)
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.set_max_cardinality should not have raised an exception')

    def test_max_cardinality_throws_when_value_is_zero(self):
        self.assertRaises(quasardb.InputError,
                          cluster.set_max_cardinality, 0)

    def test_max_cardinality_throws_when_value_is_negative(self):
        self.assertRaises(quasardb.InputError,
                          cluster.set_max_cardinality, -143)


class QuasardbClusterSetCompression(QuasardbTest):

    def test_compression_none(self):
        try:
            cluster.set_compression(quasardb.Compression.none)
        except:
            self.fail(msg='cluster.set_compression should not have raised an exception')

    def test_compression_fast(self):
        try:
            cluster.set_compression(quasardb.Compression.fast)
        except:
            self.fail(msg='cluster.set_compression should not have raised an exception')

    def test_compression_invalid(self):
        self.assertRaises(quasardb.InputError, cluster.set_compression, 123)

class QuasardbBlob(QuasardbTest):
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


class QuasardbScan(QuasardbTest):

    def test_blob_scan_nothing(self):
        res = cluster.blob_scan("ScanWillNotFind", 1000)
        self.assertEqual(0, len(res))

    def test_blob_scan_match(self):
        entry_name = entry_gen.next()
        entry_content = "ScanWillFind"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        res = cluster.blob_scan("ScanWill", 1000)

        self.assertEqual(1, len(res))
        self.assertEqual(entry_name, res[0])

        b.remove()

    def test_blob_scan_match_many(self):
        entry_content = "ScanWillFind"

        blobs = []

        for _ in xrange(10):
            entry_name = entry_gen.next()
            b = cluster.blob(entry_name)
            b.put(entry_content)
            blobs.append(b)

        res = cluster.blob_scan("ScanWill", 5)

        self.assertEqual(5, len(res))

        for b in blobs:
            b.remove()

    def test_blob_scan_regex_nothing(self):
        res = cluster.blob_scan_regex("ScanRegexW.ll.*", 1000)
        self.assertEqual(0, len(res))

    def test_blob_scan_regex_match(self):
        entry_name = entry_gen.next()
        entry_content = "ScanRegexWillFind"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        res = cluster.blob_scan_regex("ScanRegexW.ll.*", 1000)

        self.assertEqual(1, len(res))
        self.assertEqual(entry_name, res[0])

        b.remove()

    def test_blob_scan_regex_match_many(self):
        entry_content = "ScanRegexWillFind"

        blobs = []

        for _ in xrange(10):
            entry_name = entry_gen.next()
            b = cluster.blob(entry_name)
            b.put(entry_content)
            blobs.append(b)

        res = cluster.blob_scan_regex("ScanRegexW.ll.*", 5)

        self.assertEqual(5, len(res))

        for b in blobs:
            b.remove()


class QuasardbInteger(QuasardbTest):

    def __init__(self, methodName="runTest"):
        super(QuasardbInteger, self).__init__(methodName)
        self.entry_value = 0

    def setUp(self):
        entry_name = entry_gen.next()
        self.i = cluster.integer(entry_name)
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


class QuasardbDeque(QuasardbTest):

    def __init__(self, methodName="runTest"):
        super(QuasardbDeque, self).__init__(methodName)
        self.entry_content_front = "front"
        self.entry_content_back = "back"

    def setUp(self):
        entry_name = entry_gen.next()
        self.q = cluster.deque(entry_name)

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


class QuasardbHSet(QuasardbTest):

    def setUp(self):
        self.entry_content = "content"
        entry_name = entry_gen.next()
        self.hset = cluster.hset(entry_name)

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


class QuasardbInfo(QuasardbTest):
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


class QuasardbTag(QuasardbTest):

    def test_get_entries(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        tag_name = entry_gen.next()

        b = cluster.blob(entry_name)

        b.put(entry_content)

        tags = b.get_tags()
        self.assertEqual(0, len(tags))

        self.assertFalse(b.has_tag(tag_name))

        self.assertTrue(b.attach_tag(tag_name))
        self.assertFalse(b.attach_tag(tag_name))

        tags = b.get_tags()
        self.assertEqual(1, len(tags))
        self.assertEqual(tags[0], tag_name)

        self.assertTrue(b.has_tag(tag_name))

        self.assertTrue(b.detach_tag(tag_name))
        self.assertFalse(b.detach_tag(tag_name))

        tags = b.get_tags()
        self.assertEqual(0, len(tags))

    def test_tag_sequence(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        tag_name = entry_gen.next()

        b = cluster.blob(entry_name)
        b.put(entry_content)

        t = cluster.tag(tag_name)

        entries = t.get_entries()
        self.assertEqual(0, len(entries))

        self.assertTrue(b.attach_tag(tag_name))

        tags = t.get_entries()
        self.assertEqual(1, len(tags))
        self.assertEqual(tags[0], entry_name)

        self.assertEqual(1, t.count())

        self.assertTrue(b.detach_tag(tag_name))
        entries = t.get_entries()
        self.assertEqual(0, len(entries))

def _generate_double_ts(start_time, start_val, count):
    result = []

    step = datetime.timedelta(microseconds=1)

    for _ in xrange(count):
        result.append((start_time, start_val))
        start_time += step
        start_val += 1.0

    return result

def _generate_blob_ts(start_time, count):
    result = []

    step = datetime.timedelta(microseconds=1)

    for _ in xrange(count):
        result.append((start_time, "content_" + str(start_time)))
        start_time += step

    return result

class QuasardbTimeSeries(QuasardbTest):

    def setUp(self):
        self.entry_name = entry_gen.next()
        self.my_ts = cluster.ts(self.entry_name)

        self.start_time = datetime.datetime.now(quasardb.tz)
        self.test_intervals = [(self.start_time,
                                self.start_time + datetime.timedelta(microseconds=1))]

    def _create_ts(self):
        cols = self.my_ts.create([
            quasardb.TimeSeries.DoubleColumnInfo(entry_gen.next()),
            quasardb.TimeSeries.BlobColumnInfo(entry_gen.next())
        ])
        self.assertEqual(2, len(cols))

        return (cols[0], cols[1])

class QuasardbTimeSeriesNonExisting(QuasardbTimeSeries):

    def test_aggregate_throws_when_timeseries_does_not_exist(self):
        double_col = self.my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo("blah"))

        agg = quasardb.TimeSeries.DoubleAggregations()
        agg.append(quasardb.TimeSeries.Aggregation.sum, self.test_intervals[0])

        self.assertRaises(quasardb.OperationError,
                          double_col.aggregate,
                          agg)

    def test_columns_info_throws_when_timeseries_does_not_exist(self):
        self.assertRaises(quasardb.OperationError, self.my_ts.columns_info)

    def test_columns_throws_when_timeseries_does_not_exist(self):
        self.assertRaises(quasardb.OperationError, self.my_ts.columns)

    def test_insert_throws_when_timeseries_does_not_exist(self):
        double_col = self.my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo("blah"))

        self.assertRaises(quasardb.OperationError,
                          double_col.insert, [(self.start_time, 1.0)])

    def test_get_ranges_throws_when_timeseries_does_not_exist(self):
        double_col = self.my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo("blah"))

        self.assertRaises(quasardb.OperationError,
                          double_col.get_ranges,
                          self.test_intervals)

    def test_erase_ranges_throw_when_timeseries_does_not_exist(self):
        double_col = self.my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo("blah"))

        self.assertRaises(quasardb.OperationError,
                          double_col.erase_ranges,
                          self.test_intervals)

class QuasardbTimeSeriesExisting(QuasardbTimeSeries):

    def setUp(self):
        super(QuasardbTimeSeriesExisting, self).setUp()
        (self.double_col, self.blob_col) = self._create_ts()

    def __check_double_ts(self, results, start_time, start_val, count):
        self.assertEqual(count, len(results))

        step = datetime.timedelta(microseconds=1)

        for i in xrange(count):
            self.assertEqual(results[i][0], start_time)
            self.assertEqual(results[i][1], start_val)
            start_time += step
            start_val += 1.0

    def __check_blob_ts(self, results, start_time, count):
        self.assertEqual(count, len(results))

        step = datetime.timedelta(microseconds=1)

        for i in xrange(0, count):
            self.assertEqual(results[i][0], start_time)
            self.assertEqual(results[i][1], "content_" + str(start_time))
            start_time += step

    def test_blob_aggregations_default_values(self):
        agg = quasardb.TimeSeries.BlobAggregations()
        agg.append(quasardb.TimeSeries.Aggregation.first,
                   self.test_intervals[0])

        self.assertEqual(agg[0].type, quasardb.TimeSeries.Aggregation.first)
        self.assertEqual(agg[0].count, 0)
        self.assertEqual(agg[0].content, None)
        self.assertEqual(agg[0].content_length, long(0))
        self.assertEqual(agg[0].filtered_range, self.test_intervals[0])

    def test_double_aggregations_default_values(self):
        agg = quasardb.TimeSeries.DoubleAggregations()
        agg.append(quasardb.TimeSeries.Aggregation.sum,
                   self.test_intervals[0])

        self.assertEqual(agg[0].type, quasardb.TimeSeries.Aggregation.sum)
        self.assertEqual(agg[0].count, long(0))
        self.assertEqual(agg[0].value, 0.0)
        self.assertEqual(agg[0].filtered_range, self.test_intervals[0])

    def test_creation_multiple(self):
        col_list = self.my_ts.columns_info()
        self.assertEqual(2, len(col_list))

        self.assertEqual(col_list[0].name, self.double_col.name())
        self.assertEqual(col_list[0].type,
                         quasardb.TimeSeries.ColumnType.double)
        self.assertEqual(col_list[1].name, self.blob_col.name())
        self.assertEqual(col_list[1].type, quasardb.TimeSeries.ColumnType.blob)

        # invalid columinfo
        self.assertRaises(
            quasardb.InputError, self.my_ts.column,
            quasardb.TimeSeries.ColumnInfo(self.double_col.name(),
                                           quasardb.TimeSeries.ColumnType.uninitialized))

        # cannot double create
        self.assertRaises(quasardb.OperationError, self.my_ts.create,
                          [quasardb.TimeSeries.DoubleColumnInfo(entry_gen.next())])

    def test_double_get_ranges__when_timeseries_is_empty(self):
        results = self.double_col.get_ranges(self.test_intervals)
        self.assertEqual(0, len(results))

    def test_double_erase_ranges__when_timeseries_is_empty(self):
        erased_count = self.double_col.erase_ranges(self.test_intervals)
        self.assertEqual(0, erased_count)

    def test_double_get_ranges(self):
        inserted_double_data = _generate_double_ts(self.start_time, 1.0, 1000)
        self.double_col.insert(inserted_double_data)

        results = self.double_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.__check_double_ts(results, self.start_time, 1.0, 10)

        results = self.double_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10)),
             (self.start_time + datetime.timedelta(microseconds=10),
              self.start_time + datetime.timedelta(microseconds=20))])

        self.__check_double_ts(results, self.start_time, 1.0, 20)

        # empty result
        out_of_time = self.start_time + datetime.timedelta(hours=10)
        results = self.double_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        # error: column doesn't exist
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo("lolilol"))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_double_data)

        # error: column of wrong type
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo(self.blob_col.name()))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_double_data)

    def test_double_erase_ranges(self):
        inserted_double_data = _generate_double_ts(self.start_time, 1.0, 1000)
        self.double_col.insert(inserted_double_data)

        results = self.double_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        erased_count = self.double_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, len(results))

        erased_count = self.double_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, 0)

        results = self.double_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(len(results), 0)

    def test_blob_get_ranges__when_timeseries_is_empty(self):
        results = self.blob_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

    def test_blob_erase_ranges__when_timeseries_is_empty(self):
        erased_count = self.blob_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, erased_count)

    def test_blob_get_ranges(self):
        inserted_blob_data = _generate_blob_ts(self.start_time, 20)
        self.blob_col.insert(inserted_blob_data)

        results = self.blob_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.__check_blob_ts(results, self.start_time, 10)

        results = self.blob_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10)),
             (self.start_time + datetime.timedelta(microseconds=10),
              self.start_time + datetime.timedelta(microseconds=20))])

        self.__check_blob_ts(results, self.start_time, 20)

        # empty result
        out_of_time = self.start_time + datetime.timedelta(hours=10)
        results = self.blob_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        # error: column doesn't exist
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.BlobColumnInfo("lolilol"))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_blob_data)

        # error: column of wrong type
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.BlobColumnInfo(self.double_col.name()))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_blob_data)


    def test_blob_erase_ranges(self):
        inserted_blob_data = _generate_blob_ts(self.start_time, 20)
        self.blob_col.insert(inserted_blob_data)

        results = self.blob_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        erased_count = self.blob_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, len(results))

        erased_count = self.blob_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, 0)

        results = self.blob_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(len(results), 0)

class QuasardbTimeSeriesExistingWithBlobs(QuasardbTimeSeries):

    def setUp(self):
        super(QuasardbTimeSeriesExistingWithBlobs, self).setUp()
        (self.double_col, self.blob_col) = self._create_ts()

        self.inserted_blob_data = _generate_blob_ts(self.start_time, 5)
        self.inserted_blob_col = [x[1] for x in self.inserted_blob_data]

        self.blob_col.insert(self.inserted_blob_data)

        self.test_intervals = [
            (self.start_time, self.start_time + datetime.timedelta(days=1))]

    def test_blob_get_ranges_out_of_time(self):
        out_of_time = self.start_time + datetime.timedelta(hours=10)
        results = self.blob_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

    def _test_aggregation_of_blobs(self, agg_type, expected, expected_count):
        agg = quasardb.TimeSeries.BlobAggregations()
        agg.append(agg_type, self.test_intervals[0])

        agg_res = self.blob_col.aggregate(agg)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].filtered_range, self.test_intervals[0])
        timestamp = expected[0]
        if timestamp is None:
            timestamp = datetime.datetime.fromtimestamp(0, quasardb.tz)
        self.assertEqual(agg_res[0].timestamp, timestamp)
        self.assertEqual(agg_res[0].count, expected_count)
        expected_length = long(0)
        if expected[1] is not None:
            expected_length = len(expected[1])
        self.assertEqual(agg_res[0].content_length, expected_length)
        self.assertEqual(agg_res[0].content, expected[1])

    def test_blob_aggregation_count(self):
        self._test_aggregation_of_blobs(quasardb.TimeSeries.Aggregation.count,
                                        (None, None), len(self.inserted_blob_data))

    def test_blob_aggregation_first(self):
        self._test_aggregation_of_blobs(quasardb.TimeSeries.Aggregation.first,
                                        self.inserted_blob_data[0], 1)

    def test_blob_aggregation_last(self):
        self._test_aggregation_of_blobs(quasardb.TimeSeries.Aggregation.last,
                                        self.inserted_blob_data[-1], 1)


class QuasardbTimeSeriesExistingWithDoubles(QuasardbTimeSeries):

    def setUp(self):
        super(QuasardbTimeSeriesExistingWithDoubles, self).setUp()
        (self.double_col, self.blob_col) = self._create_ts()

        self.inserted_double_data = _generate_double_ts(
            self.start_time, 1.0, 1000)
        self.inserted_double_col = [x[1] for x in self.inserted_double_data]

        self.double_col.insert(self.inserted_double_data)

        self.test_intervals = [
            (self.start_time, self.start_time + datetime.timedelta(days=1))]

    def test_double_get_ranges_out_of_time(self):
        out_of_time = self.start_time + datetime.timedelta(hours=10)
        results = self.double_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

    def _test_aggregation_of_doubles(self, agg_type, expected, expected_count):
        agg = quasardb.TimeSeries.DoubleAggregations()
        agg.append(agg_type, self.test_intervals[0])

        agg_res = self.double_col.aggregate(agg)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].filtered_range, self.test_intervals[0])
        timestamp = expected[0]
        if timestamp is None:
            timestamp = datetime.datetime.fromtimestamp(0, quasardb.tz)
        self.assertEqual(agg_res[0].timestamp, timestamp)
        self.assertEqual(agg_res[0].count, expected_count)
        self.assertEqual(agg_res[0].value, expected[1])

    def test_double_aggregation_first(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.first, self.inserted_double_data[0], 1)

    def test_double_aggregation_last(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.last, self.inserted_double_data[-1], 1)

    def test_double_aggregation_min(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.min, min(self.inserted_double_data), 1)

    def test_double_aggregation_max(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.max, max(self.inserted_double_data), 1)

    def test_double_aggregation_abs_min(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.abs_min, min(self.inserted_double_data), 1)

    def test_double_aggregation_abs_max(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.abs_max, max(self.inserted_double_data), 1)

    def test_double_aggregation_spread(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.spread,
            (None,
             max(self.inserted_double_data)[1] - min(self.inserted_double_data)[1]),
            1)

    def test_double_aggregation_count(self):
        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.count,
                                          (None, len(self.inserted_double_data)),
                                          len(self.inserted_double_data))

    def test_double_aggregation_arithmetic_mean(self):
        computed_sum = reduce(lambda x, y: x + y,
                              self.inserted_double_col, 0.0)

        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.arithmetic_mean,
                                          (None, computed_sum /
                                           len(self.inserted_double_data)),
                                          len(self.inserted_double_data))

    def test_double_aggregation_sum(self):
        computed_sum = reduce(lambda x, y: x + y,
                              self.inserted_double_col, 0.0)

        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sum,
                                          (None, computed_sum), len(self.inserted_double_data))

    def test_double_aggregation_sum_of_squares(self):
        computed_sum = reduce(lambda x, y: x + y * y,
                              self.inserted_double_col, 0.0)

        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sum_of_squares,
                                          (None, computed_sum), len(self.inserted_double_data))

    @unittest.skipUnless(HAS_NUMPY, 'requires numpy package')
    def test_double_aggregation_population_variance(self):
        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.population_variance,
                                          (None, numpy.var(
                                              self.inserted_double_col)),
                                          len(self.inserted_double_data))

    @unittest.skipUnless(HAS_NUMPY, 'requires numpy package')
    def test_double_aggregation_population_stddev(self):
        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.population_stddev,
                                          (None, numpy.std(
                                              self.inserted_double_col)),
                                          len(self.inserted_double_data))

    @unittest.skipUnless(HAS_NUMPY, 'requires numpy package')
    def test_double_aggregation_sample_variance(self):
        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sample_variance,
                                          (None,
                                           numpy.var(self.inserted_double_col, ddof=1)),
                                          len(self.inserted_double_data))

    @unittest.skipUnless(HAS_NUMPY, 'requires numpy package')
    def test_double_aggregation_sample_stddev(self):
        self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sample_stddev,
                                          (None,
                                           numpy.std(self.inserted_double_col, ddof=1)),
                                          len(self.inserted_double_data))


class QuasardbQuery(QuasardbTest):

    def test_types(self):

        my_tag = "my_tag" + entry_gen.next()

        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)
        b.attach_tag(my_tag)

        res = cluster.query("tag='" + my_tag + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = cluster.query("tag='" + my_tag + "' AND type=blob")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = cluster.query("tag='" + my_tag + "' AND type=integer")

        self.assertEqual(len(res), 0)

    def test_two_tags(self):

        tag1 = "tag1" + entry_gen.next()
        tag2 = "tag2" + entry_gen.next()

        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)
        b.attach_tag(tag1)
        b.attach_tag(tag2)

        res = cluster.query("tag='" + tag1 + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = cluster.query("tag='" + tag2 + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = cluster.query("tag='" + tag1 + "' AND tag='" + tag2 + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)


class QuasardbPrefix(QuasardbTest):

    def test_empty_prefix(self):

        res = cluster.prefix_get("testazeazeaze", 10)
        self.assertEqual(len(res), 0)

        self.assertEqual(0, cluster.prefix_count("testazeazeaze"))

    def test_find_one(self):
        dat_prefix = "my_dark_prefix"
        entry_name = dat_prefix + entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        res = cluster.prefix_get(dat_prefix, 10)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        self.assertEqual(1, cluster.prefix_count(dat_prefix))


class QuasardbSuffix(QuasardbTest):

    def test_empty_suffix(self):

        res = cluster.suffix_get("testazeazeaze", 10)
        self.assertEqual(len(res), 0)

        self.assertEqual(0, cluster.suffix_count("testazeazeaze"))

    def test_find_one(self):
        dat_suffix = "my_dark_suffix"

        entry_name = entry_gen.next() + dat_suffix
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        res = cluster.suffix_get(dat_suffix, 10)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        self.assertEqual(1, cluster.suffix_count(dat_suffix))


def _make_expiry_time(td):
        # expires in one minute
    now = datetime.datetime.now(quasardb.tz)
    # get rid of the microsecond for the tests
    return now + td - datetime.timedelta(microseconds=now.microsecond)


class QuasardbExpiry(QuasardbTest):
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
        # these tests may run in debug. This will be enough however to check that
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
    import xmlrunner
    unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output='test-reports'))()
