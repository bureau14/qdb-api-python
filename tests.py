# pylint: disable=C0103,C0111,C0302,C0330,W0212

import datetime
import os
import subprocess
import sys
import time
import unittest
import calendar
import timeit
#import numpy

for root, dirnames, filenames in os.walk(os.path.join('..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401

# generate an unique entry name for the tests


class UniqueEntryNameGenerator(object):

    def __init__(self):
        self.__prefix = "entry_"
        self.__counter = 0

    def __iter__(self):
        return self

    def next(self):
        self.__counter += 1
        return self.__prefix + str(self.__counter)


def setUpModule():

    global uri  # pylint: disable=W0601
    global cluster  # pylint: disable=W0601
    global __clusterd  # pylint: disable=W0601
    global entry_gen  # pylint: disable=W0601

    entry_gen = UniqueEntryNameGenerator()

    __current_port = 3000
    uri = ""

    # don't save anything to disk
    __clusterd = subprocess.Popen([os.path.join(
        '.', 'qdbd'), '--address=127.0.0.1:' + str(__current_port), '--transient'])
    if __clusterd.pid == 0:
        raise Exception("daemon", "cannot run daemon")

    # startup may take a couple of seconds, temporize to make sure the
    # connection will be accepted
    time.sleep(2)

    __clusterd.poll()

    if __clusterd.returncode != None:
        raise Exception("daemon", "error while running daemon")

    uri = "qdb://127.0.0.1:" + str(__current_port)

    __current_port += 1

    try:
        cluster = quasardb.Cluster(uri)

    except quasardb.QuasardbException:
        __clusterd.terminate()
        __clusterd.wait()
        raise

    except BaseException:
        __clusterd.terminate()
        __clusterd.wait()
        raise


def tearDownModule():
    __clusterd.terminate()
    __clusterd.wait()


class QuasardbTest(unittest.TestCase):
    pass


class QuasardbBasic(QuasardbTest):
    """
    Basic operations tests such as put/get/remove
    """

    def test_info(self):
        ''' Checks build and version information. '''

        build = quasardb.build()
        self.assertGreater(len(build), 0)
        version = quasardb.version()
        self.assertGreater(len(version), 0)

    def test_trim_all_at_begin(self):
        try:
            cluster.trim_all(datetime.timedelta(minutes=1))
        except quasardb.QuasardbException:
            self.fail('cluster::trim_all raised an unexpected exception')

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

    def test_time(self):
        time_no_tz = datetime.datetime.now()
        tz_offset = time.timezone

        expected_value = long(calendar.timegm(time_no_tz.timetuple()))
        self.assertEqual(expected_value, quasardb._time_to_unix_timestamp(time_no_tz, tz_offset))

        time_qdb_tz = datetime.datetime.now(quasardb.tz)

        expected_value = long(calendar.timegm(time_qdb_tz.timetuple()))
        self.assertEqual(expected_value, quasardb._time_to_unix_timestamp(time_qdb_tz, tz_offset))

    def test_ts_convert(self):
        orig_couple = [(datetime.datetime.now(quasardb.tz), datetime.datetime.now(quasardb.tz))]
        converted_couple = quasardb._convert_time_couples_to_qdb_range_t_vector(orig_couple)
        self.assertEqual(len(converted_couple), 1)
        self.assertEqual(orig_couple[0], quasardb._convert_qdb_range_t_to_time_couple(converted_couple[0]))

    def test_timeout(self):
        # 1 day ok
        cluster.set_timeout(datetime.timedelta(days=1))

        # 1s ok
        cluster.set_timeout(datetime.timedelta(seconds=1))

        # 1 ms ok
        cluster.set_timeout(datetime.timedelta(milliseconds=1))

        # 1 us not ok, timeout must be in milliseconds
        self.assertRaises(quasardb.QuasardbException,
                          cluster.set_timeout, datetime.timedelta(microseconds=1))

    def test_put_get_and_remove(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)

        self.assertRaises(quasardb.QuasardbException, b.put, entry_content)

        got = b.get()
        self.assertEqual(got, entry_content)
        b.remove()
        self.assertRaises(quasardb.QuasardbException, b.get)
        self.assertRaises(quasardb.QuasardbException, b.remove)

    def test_update(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.update(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        entry_content = "it's a new style"
        b.update(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        b.remove()

        entry_content = ''.join('%c' % x for x in range(0, 256))
        self.assertEqual(len(entry_content), 256)

        b.update(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)

    def test_get_and_update(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)

        entry_new_content = "new stuff"
        got = b.get_and_update(entry_new_content)
        self.assertEqual(got, entry_content)
        got = b.get()
        self.assertEqual(got, entry_new_content)

        b.remove()

    def test_get_and_remove(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        got = b.get_and_remove()
        self.assertEqual(got, entry_content)
        self.assertRaises(quasardb.QuasardbException, b.get)

    def test_remove_if(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        self.assertRaises(quasardb.QuasardbException,
                          b.remove_if, entry_content + 'a')
        got = b.get()
        self.assertEqual(got, entry_content)
        b.remove_if(entry_content)
        self.assertRaises(quasardb.QuasardbException, b.get)

    def test_compare_and_swap(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        entry_new_content = "new stuff"
        got = b.compare_and_swap(entry_new_content, entry_new_content)
        self.assertEqual(got, entry_content)
        # unchanged because unmatched
        got = b.get()
        self.assertEqual(got, entry_content)
        got = b.compare_and_swap(entry_new_content, entry_content)
        self.assertEqual(got, None)
        # changed because matched
        got = b.get()
        self.assertEqual(got, entry_new_content)

        b.remove()

    def test_purge_all(self):
        # disabled by default, must raise an exception
        self.assertRaises(quasardb.QuasardbException,
                          cluster.purge_all, datetime.timedelta(minutes=1))

    def test_trim_all_at_end(self):
        try:
            cluster.trim_all(datetime.timedelta(minutes=1))
        except quasardb.QuasardbException:
            self.fail('cluster::trim_all raised an unexpected exception')


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

        for _ in range(0, 10):
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

        for _ in range(0, 10):
            entry_name = entry_gen.next()
            b = cluster.blob(entry_name)
            b.put(entry_content)
            blobs.append(b)

        res = cluster.blob_scan_regex("ScanRegexW.ll.*", 5)

        self.assertEqual(5, len(res))

        for b in blobs:
            b.remove()


class QuasardbInteger(QuasardbTest):

    def test_put_get_and_remove(self):
        entry_name = entry_gen.next()
        entry_value = 0

        i = cluster.integer(entry_name)

        i.put(entry_value)

        self.assertRaises(quasardb.QuasardbException, i.put, entry_value)

        got = i.get()
        self.assertEqual(got, entry_value)
        i.remove()
        self.assertRaises(quasardb.QuasardbException, i.get)
        self.assertRaises(quasardb.QuasardbException, i.remove)

    def test_update(self):
        entry_name = entry_gen.next()
        entry_value = 1

        i = cluster.integer(entry_name)

        i.update(entry_value)
        got = i.get()
        self.assertEqual(got, entry_value)
        entry_value = 2
        i.update(entry_value)
        got = i.get()
        self.assertEqual(got, entry_value)
        i.remove()

    def test_add(self):
        entry_name = entry_gen.next()
        entry_value = 0

        i = cluster.integer(entry_name)

        i.put(entry_value)

        got = i.get()
        self.assertEqual(got, entry_value)

        entry_increment = 10

        i.add(entry_increment)

        got = i.get()
        self.assertEqual(got, entry_value + entry_increment)

        entry_decrement = -100

        i.add(entry_decrement)

        got = i.get()
        self.assertEqual(got, entry_value + entry_increment + entry_decrement)

        i.remove()


class QuasardbDeque(QuasardbTest):

    def test_sequence(self):
        """
        A series of test to make sure back and front operations are properly wired
        """
        entry_name = entry_gen.next()
        entry_content_back = "back"

        q = cluster.deque(entry_name)

        self.assertRaises(quasardb.QuasardbException, q.pop_back)
        self.assertRaises(quasardb.QuasardbException, q.pop_front)
        self.assertRaises(quasardb.QuasardbException, q.front)
        self.assertRaises(quasardb.QuasardbException, q.back)

        q.push_back(entry_content_back)

        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_back)

        entry_content_front = "front"

        q.push_front(entry_content_front)

        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        entry_content_canary = "canary"

        q.push_back(entry_content_canary)

        got = q.back()
        self.assertEqual(got, entry_content_canary)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        got = q.pop_back()
        self.assertEqual(got, entry_content_canary)
        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        q.push_front(entry_content_canary)

        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_canary)

        got = q.pop_front()
        self.assertEqual(got, entry_content_canary)
        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        got = q.pop_back()
        self.assertEqual(got, entry_content_back)

        got = q.back()
        self.assertEqual(got, entry_content_front)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        got = q.pop_back()
        self.assertEqual(got, entry_content_front)

        self.assertRaises(quasardb.QuasardbException, q.pop_back)
        self.assertRaises(quasardb.QuasardbException, q.pop_front)
        self.assertRaises(quasardb.QuasardbException, q.front)
        self.assertRaises(quasardb.QuasardbException, q.back)


class QuasardbHSet(QuasardbTest):

    def test_insert_erase_contains(self):

        entry_name = entry_gen.next()
        entry_content = "content"

        hset = cluster.hset(entry_name)

        # does not exist yet
        self.assertRaises(quasardb.QuasardbException,
                          hset.contains, entry_content)

        hset.insert(entry_content)

        self.assertRaises(quasardb.QuasardbException,
                          hset.insert, entry_content)

        self.assertTrue(hset.contains(entry_content))

        hset.erase(entry_content)

        self.assertFalse(hset.contains(entry_content))

        hset.insert(entry_content)

        self.assertTrue(hset.contains(entry_content))

        hset.erase(entry_content)

        self.assertRaises(quasardb.QuasardbException,
                          hset.erase, entry_content)


class QuasardbInfo(QuasardbTest):
    """
    Tests the json information string query
    """

    def test_node_status(self):
        status = cluster.node_status(uri)
        self.assertGreater(len(status), 0)

    def test_node_config(self):
        config = cluster.node_config(uri)
        self.assertGreater(len(config), 0)


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

        self.assertTrue(b.detach_tag(tag_name))
        entries = t.get_entries()
        self.assertEqual(0, len(entries))


class QuasardbTimeSeries(QuasardbTest):

    def __generate_double_ts(self, start_time, start_val, count):
        result = []

        step = datetime.timedelta(microseconds=1)

        for _ in range(0, count):
            result.append((start_time, start_val))
            start_time += step
            start_val += 1.0

        return result

    def __generate_blob_ts(self, start_time, count):
        result = []

        step = datetime.timedelta(microseconds=1)

        for _ in range(0, count):
            result.append((start_time, "1911"))
            start_time += step

        return result

    def __check_double_ts(self, results, start_time, start_val, count):
        self.assertEqual(count, len(results))

        step = datetime.timedelta(microseconds=1)

        for i in range(0, count):
            self.assertEqual(results[i][0], start_time)
            self.assertEqual(results[i][1], start_val)
            start_time += step
            start_val += 1.0

    def __check_blob_ts(self, results, start_time, count):
        self.assertEqual(count, len(results))

        step = datetime.timedelta(microseconds=1)

        for i in range(0, count):
            self.assertEqual(results[i][0], start_time)
            self.assertEqual(results[i][1], "1911")
            start_time += step

    def __create_ts(self):
        entry_name = entry_gen.next()

        my_ts = cluster.ts(entry_name)

        cols = my_ts.create([quasardb.TimeSeries.DoubleColumnInfo(
            entry_gen.next()), quasardb.TimeSeries.BlobColumnInfo(entry_gen.next())])
        self.assertEqual(2, len(cols))

        return (my_ts, cols[0], cols[1])

    def test_non_existing(self):
        entry_name = entry_gen.next()

        my_ts = cluster.ts(entry_name)

        # ts doesn't exist yet
        self.assertRaises(quasardb.QuasardbException, my_ts.columns_info)
        self.assertRaises(quasardb.QuasardbException, my_ts.columns)

        double_col = my_ts.column(quasardb.TimeSeries.DoubleColumnInfo("blah"))

        start_time = datetime.datetime.now(quasardb.tz)

        self.assertRaises(quasardb.QuasardbException,
                          double_col.insert, [(start_time, 1.0)])
        self.assertRaises(quasardb.QuasardbException,
                          double_col.get_ranges,
                          [(start_time, start_time + datetime.timedelta(microseconds=1))])
        self.assertRaises(quasardb.QuasardbException,
                          double_col.aggregate,
                          quasardb.TimeSeries.Aggregation.sum,
                          [(start_time, start_time + datetime.timedelta(microseconds=1))])

    def test_creation_multiple(self):
        (my_ts, double_col, blob_col) = self.__create_ts()

        col_list = my_ts.columns_info()
        self.assertEqual(2, len(col_list))

        self.assertEqual(col_list[0].name, double_col.name())
        self.assertEqual(col_list[0].type,
                         quasardb.TimeSeries.ColumnType.double)
        self.assertEqual(col_list[1].name, blob_col.name())
        self.assertEqual(col_list[1].type, quasardb.TimeSeries.ColumnType.blob)

        # invalid columinfo
        self.assertRaises(quasardb.QuasardbException, my_ts.column, quasardb.TimeSeries.ColumnInfo(
            double_col.name(), quasardb.TimeSeries.ColumnType.uninitialized))

        # cannot double create
        self.assertRaises(quasardb.QuasardbException, my_ts.create, [
                          quasardb.TimeSeries.DoubleColumnInfo(entry_gen.next())])

    def test_double_get_ranges(self):
        (my_ts, double_col, blob_col) = self.__create_ts()

        start_time = datetime.datetime.now(quasardb.tz)

        # empty result: nothing inserted
        results = double_col.get_ranges(
            [(start_time, start_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        inserted_double_data = self.__generate_double_ts(start_time, 1.0, 1000)
        double_col.insert(inserted_double_data)

        results = double_col.get_ranges(
            [(start_time, start_time + datetime.timedelta(microseconds=10))])

        self.__check_double_ts(results, start_time, 1.0, 10)

        results = double_col.get_ranges(
            [(start_time, start_time + datetime.timedelta(microseconds=10)),
             (start_time + datetime.timedelta(microseconds=10),
              start_time + datetime.timedelta(microseconds=20))])

        self.__check_double_ts(results, start_time, 1.0, 20)

        # empty result
        out_of_time = start_time + datetime.timedelta(hours=10)
        results = double_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        # error: column doesn't exist
        wrong_col = my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo("lolilol"))

        self.assertRaises(quasardb.QuasardbException, wrong_col.get_ranges, [
                          (start_time, start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.QuasardbException,
                          wrong_col.insert, inserted_double_data)

        # error: column of wrong type
        wrong_col = my_ts.column(
            quasardb.TimeSeries.DoubleColumnInfo(blob_col.name()))

        self.assertRaises(quasardb.QuasardbException, wrong_col.get_ranges, [
                          (start_time, start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.QuasardbException,
                          wrong_col.insert, inserted_double_data)

    def test_aggregation(self):
        (_, double_col, blob_col) = self.__create_ts()
        start_time = datetime.datetime.now(quasardb.tz)

        inserted_double_data = self.__generate_double_ts(start_time, 1.0, 10)
        inserted_double_col = map(lambda x: x[1], inserted_double_data)

        inserted_blob_data = self.__generate_blob_ts(start_time, 5)

        double_col.insert(inserted_double_data)
        blob_col.insert(inserted_blob_data)

        test_intervals = [
            (start_time, start_time + datetime.timedelta(microseconds=10))]

        computed_sum = reduce(lambda x, y: x + y, inserted_double_col, 0.0)
        computed_count = len(inserted_double_data)

        # first
        agg_res = double_col.aggregate(
            quasardb.TimeSeries.Aggregation.first, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].timestamp, inserted_double_data[0][0])
        self.assertEqual(agg_res[0].value, inserted_double_data[0][1])

        # unsupported for blob?
        # self.assertRaises(quasardb.QuasardbException,
        #                   blob_col.aggregate,
        # quasardb.TimeSeries.Aggregation.first, test_intervals)

        # last
        agg_res = double_col.aggregate(
            quasardb.TimeSeries.Aggregation.last, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[-1])
        self.assertEqual(agg_res[0].timestamp, inserted_double_data[-1][0])
        self.assertEqual(agg_res[0].value, inserted_double_data[-1][1])

        # unsupported for blob?
        # self.assertRaises(quasardb.QuasardbException,
        #                   blob_col.aggregate,
        # quasardb.TimeSeries.Aggregation.first, test_intervals)

        # min
        agg_res = double_col.aggregate(
            quasardb.TimeSeries.Aggregation.min, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].timestamp, min(inserted_double_data)[0])
        self.assertEqual(agg_res[0].value, min(inserted_double_data)[1])

        # abs_min
        agg_res = double_col.aggregate(quasardb.TimeSeries.Aggregation.abs_min, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].timestamp, min(inserted_double_data)[0])
        self.assertEqual(agg_res[0].value, min(inserted_double_data)[1])

        # unsupported for blob?
        # self.assertRaises(quasardb.QuasardbException,
        #                   blob_col.aggregate,
        # quasardb.TimeSeries.Aggregation.first, test_intervals)

        # max
        agg_res = double_col.aggregate(
            quasardb.TimeSeries.Aggregation.max, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].timestamp, max(inserted_double_data)[0])
        self.assertEqual(agg_res[0].value, max(inserted_double_data)[1])

        # abs_max
        agg_res = double_col.aggregate(quasardb.TimeSeries.Aggregation.abs_max, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].timestamp, max(inserted_double_data)[0])
        self.assertEqual(agg_res[0].value, max(inserted_double_data)[1])

        # spread
        agg_res = double_col.aggregate(quasardb.TimeSeries.Aggregation.spread, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].value, max(inserted_double_data)[1] - min(inserted_double_data)[1])

        # variance
        agg_res = double_col.aggregate(quasardb.TimeSeries.Aggregation.variance, test_intervals)
        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
     #   self.assertEqual(agg_res[0].value, numpy.var(inserted_double_col))

        # standard deviation
        agg_res = double_col.aggregate(quasardb.TimeSeries.Aggregation.stddev, test_intervals)
        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
     #   self.assertEqual(agg_res[0].value, numpy.std(inserted_double_col))

        # unsupported for blob?
        # self.assertRaises(quasardb.QuasardbException,
        #                   blob_col.aggregate,
        # quasardb.TimeSeries.Aggregation.first, test_intervals)

        # average
        agg_res = double_col.aggregate(
            quasardb.TimeSeries.Aggregation.average, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].value, computed_sum / computed_count)

        # unsupported for blob?
        # self.assertRaises(quasardb.QuasardbException,
        #                   blob_col.aggregate,
        # quasardb.TimeSeries.Aggregation.first, test_intervals)

        # count
        agg_res = double_col.aggregate(
            quasardb.TimeSeries.Aggregation.count, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].value, computed_count)

        agg_res = blob_col.aggregate(
            quasardb.TimeSeries.Aggregation.count, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].value, len(inserted_blob_data))

        # sum
        agg_res = double_col.aggregate(
            quasardb.TimeSeries.Aggregation.sum, test_intervals)

        self.assertEqual(1, len(agg_res))
        self.assertEqual(agg_res[0].range, test_intervals[0])
        self.assertEqual(agg_res[0].value, computed_sum)
        # unsupported for blob?
        # self.assertRaises(quasardb.QuasardbException, blob_col.aggregate,
        # quasardb.TimeSeries.Aggregation.first, test_intervals)

    def test_blob_get_ranges(self):
        (my_ts, double_col, blob_col) = self.__create_ts()

        start_time = datetime.datetime.now(quasardb.tz)

        # empty result: nothing inserted
        results = blob_col.get_ranges(
            [(start_time, start_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        inserted_blob_data = self.__generate_blob_ts(start_time, 20)
        blob_col.insert(inserted_blob_data)

        results = blob_col.get_ranges(
            [(start_time, start_time + datetime.timedelta(microseconds=10))])

        self.__check_blob_ts(results, start_time, 10)

        results = blob_col.get_ranges(
            [(start_time, start_time + datetime.timedelta(microseconds=10)),
             (start_time + datetime.timedelta(microseconds=10),
              start_time + datetime.timedelta(microseconds=20))])

        self.__check_blob_ts(results, start_time, 20)

        # empty result
        out_of_time = start_time + datetime.timedelta(hours=10)
        results = blob_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        # error: column doesn't exist
        wrong_col = my_ts.column(quasardb.TimeSeries.BlobColumnInfo("lolilol"))

        self.assertRaises(quasardb.QuasardbException, wrong_col.get_ranges, [
                          (start_time, start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.QuasardbException,
                          wrong_col.insert, inserted_blob_data)

        # error: column of wrong type
        wrong_col = my_ts.column(
            quasardb.TimeSeries.BlobColumnInfo(double_col.name()))

        self.assertRaises(quasardb.QuasardbException, wrong_col.get_ranges, [
                          (start_time, start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.QuasardbException,
                          wrong_col.insert, inserted_blob_data)

    def test_blob_get_ranges_double_column(self):
        (_, double_col, _) = self.__create_ts()
        start_time = datetime.datetime.now(quasardb.tz)

        out_of_time = start_time + datetime.timedelta(hours=10)
        results = double_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

    def test_double_get_ranges_blob_column(self):
        (_, _, blob_col) = self.__create_ts()
        start_time = datetime.datetime.now(quasardb.tz)

        out_of_time = start_time + datetime.timedelta(hours=10)
        results = blob_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))


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


class QuasardbExpiry(QuasardbTest):

    def __make_expiry_time(self, td):
         # expires in one minute
        now = datetime.datetime.now(quasardb.tz)
        # get rid of the microsecond for the tests
        return now + td - datetime.timedelta(microseconds=now.microsecond)

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
        self.assertRaises(quasardb.QuasardbException, b.get_expiry_time)

        b.put(entry_content)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, datetime.datetime.fromtimestamp(0, quasardb.tz))

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=1))
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

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=1))

        b = cluster.blob(entry_name)
        b.put(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=2))

        b.update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=3))

        b.get_and_update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=4))

        b.compare_and_swap(entry_content, entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        b.remove()

if __name__ == '__main__':
    import xmlrunner
    unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output='test-reports'))()
