# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange, int as long  # pylint: disable=W0622
from functools import reduce  # pylint: disable=W0622
import datetime
import os
import sys
import unittest
import settings

for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))
import quasardb  # pylint: disable=C0413,E0401

HAS_NUMPY = True
try:
    import numpy
except ImportError:
    HAS_NUMPY = False


def _generate_double_ts(start_time, start_val, count):
    result = []

    step = datetime.timedelta(microseconds=1)

    for _ in xrange(count):
        result.append((start_time, start_val))
        start_time += step
        start_val += 1.0

    return result


def _generate_int64_ts(start_time, start_val, count):
    result = []

    step = datetime.timedelta(microseconds=1)

    for _ in xrange(count):
        result.append((start_time, start_val))
        start_time += step
        start_val += 1

    return result


timestamp_increase_step = datetime.timedelta(microseconds=7)


def _generate_timestamp_ts(start_time, start_val, count):

    result = []

    step = datetime.timedelta(microseconds=1)

    for _ in xrange(count):
        result.append((start_time, start_val))
        start_time += step
        start_val += timestamp_increase_step

    return result


def _generate_blob_ts(start_time, count):
    result = []

    step = datetime.timedelta(microseconds=1)

    for _ in xrange(count):
        result.append((start_time, "content_" + str(start_time)))
        start_time += step

    return result


class QuasardbTimeSeries(unittest.TestCase):
    def setUp(self):
        self.entry_name = settings.entry_gen.next()
        self.my_ts = settings.cluster.ts(self.entry_name)

        self.start_time = datetime.datetime.now(quasardb.tz)
        self.test_intervals = [(self.start_time,
                                self.start_time + datetime.timedelta(microseconds=1))]

    def _create_ts(self):
        cols = self.my_ts.create([
            quasardb.TimeSeries.DoubleColumnInfo(settings.entry_gen.next()),
            quasardb.TimeSeries.BlobColumnInfo(settings.entry_gen.next()),
            quasardb.TimeSeries.Int64ColumnInfo(settings.entry_gen.next()),
            quasardb.TimeSeries.TimestampColumnInfo(settings.entry_gen.next()),
        ])
        self.assertEqual(4, len(cols))

        return (cols[0], cols[1], cols[2], cols[3])

    def create_ts(self):
        (self.double_col, self.blob_col, self.int64_col,
         self.timestamp_col) = self._create_ts()


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

    def test_create_without_columns(self):
        cols = self.my_ts.create([])
        self.assertEqual(0, len(cols))

    def test_create_with_shard_size_less_than_1_millisecond_throws(self):
        self.assertRaises(quasardb.InputError,
                          self.my_ts.create, [], datetime.timedelta(milliseconds=0))

    def test_create_with_shard_size_of_1_millisecond(self):
        cols = self.my_ts.create([], datetime.timedelta(milliseconds=1))
        self.assertEqual(0, len(cols))

    def test_create_with_shard_size_of_1_day(self):
        cols = self.my_ts.create([], datetime.timedelta(hours=24))
        self.assertEqual(0, len(cols))

    def test_create_with_shard_size_of_4_weeks(self):
        cols = self.my_ts.create([], datetime.timedelta(weeks=4))
        self.assertEqual(0, len(cols))

    def test_create_with_shard_size_of_more_than_1_year(self):
        cols = self.my_ts.create([], datetime.timedelta(weeks=52))
        self.assertEqual(0, len(cols))


class QuasardbTimeSeriesExisting(QuasardbTimeSeries):

    def setUp(self):
        super(QuasardbTimeSeriesExisting, self).setUp()
        self.create_ts()

    def __check_double_ts(self, results, start_time, start_val, count):
        self.assertEqual(count, len(results))

        step = datetime.timedelta(microseconds=1)

        for i in xrange(count):
            self.assertEqual(results[i][0], start_time)
            self.assertEqual(results[i][1], start_val)
            start_time += step
            start_val += 1.0

    def __check_int64_ts(self, results, start_time, start_val, count):
        self.assertEqual(count, len(results))

        step = datetime.timedelta(microseconds=1)

        for i in xrange(count):
            self.assertEqual(results[i][0], start_time)
            self.assertEqual(results[i][1], start_val)
            start_time += step
            start_val += 1

    def __check_timestamp_ts(self, results, start_time, start_val, count):
        self.assertEqual(count, len(results))

        step = datetime.timedelta(microseconds=1)

        for i in xrange(count):
            self.assertEqual(results[i][0], start_time)
            self.assertEqual(results[i][1], start_val)
            start_time += step
            start_val += timestamp_increase_step

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
        self.assertEqual(agg[0].range, self.test_intervals[0])

    def test_double_aggregations_default_values(self):
        agg = quasardb.TimeSeries.DoubleAggregations()
        agg.append(quasardb.TimeSeries.Aggregation.sum,
                   self.test_intervals[0])

        self.assertEqual(agg[0].type, quasardb.TimeSeries.Aggregation.sum)
        self.assertEqual(agg[0].count, long(0))
        self.assertEqual(agg[0].value, 0.0)
        self.assertEqual(agg[0].range, self.test_intervals[0])

    def test_creation_multiple(self):
        col_list = self.my_ts.columns_info()
        self.assertEqual(4, len(col_list))

        self.assertEqual(col_list[0].name, self.double_col.name())
        self.assertEqual(col_list[0].type,
                         quasardb.TimeSeries.ColumnType.double)
        self.assertEqual(col_list[1].name, self.blob_col.name())
        self.assertEqual(col_list[1].type, quasardb.TimeSeries.ColumnType.blob)
        self.assertEqual(col_list[2].name, self.int64_col.name())
        self.assertEqual(col_list[2].type,
                         quasardb.TimeSeries.ColumnType.int64)
        self.assertEqual(col_list[3].name, self.timestamp_col.name())
        self.assertEqual(col_list[3].type,
                         quasardb.TimeSeries.ColumnType.timestamp)

        # invalid columinfo
        self.assertRaises(
            quasardb.InputError, self.my_ts.column,
            quasardb.TimeSeries.ColumnInfo(self.double_col.name(),
                                           quasardb.TimeSeries.ColumnType.uninitialized))

        # cannot double create
        self.assertRaises(quasardb.OperationError, self.my_ts.create,
                          [quasardb.TimeSeries.DoubleColumnInfo(settings.entry_gen.next())])

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

    def test_int64_get_ranges__when_timeseries_is_empty(self):
        results = self.int64_col.get_ranges(self.test_intervals)
        self.assertEqual(0, len(results))

    def test_int64_erase_ranges__when_timeseries_is_empty(self):
        erased_count = self.int64_col.erase_ranges(self.test_intervals)
        self.assertEqual(0, erased_count)

    def test_int64_get_ranges(self):
        inserted_int64_data = _generate_int64_ts(self.start_time, 1, 1000)
        self.int64_col.insert(inserted_int64_data)

        results = self.int64_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.__check_int64_ts(results, self.start_time, 1, 10)

        results = self.int64_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10)),
             (self.start_time + datetime.timedelta(microseconds=10),
              self.start_time + datetime.timedelta(microseconds=20))])

        self.__check_int64_ts(results, self.start_time, 1, 20)

        # empty result
        out_of_time = self.start_time + datetime.timedelta(hours=10)
        results = self.int64_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        # error: column doesn't exist
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.Int64ColumnInfo("lolilol"))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_int64_data)

        # error: column of wrong type
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.Int64ColumnInfo(self.blob_col.name()))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_int64_data)

    def test_int64_erase_ranges(self):
        inserted_int64_data = _generate_int64_ts(self.start_time, 1, 1000)
        self.int64_col.insert(inserted_int64_data)

        results = self.int64_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        erased_count = self.int64_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, len(results))

        erased_count = self.int64_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, 0)

        results = self.int64_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(len(results), 0)

    def test_timestamp_get_ranges__when_timeseries_is_empty(self):
        results = self.timestamp_col.get_ranges(self.test_intervals)
        self.assertEqual(0, len(results))

    def test_timestamp_erase_ranges__when_timeseries_is_empty(self):
        erased_count = self.timestamp_col.erase_ranges(self.test_intervals)
        self.assertEqual(0, erased_count)

    def test_timestamp_get_ranges(self):
        inserted_timestamp_data = _generate_timestamp_ts(
            self.start_time, self.start_time + datetime.timedelta(minutes=1), 1000)
        self.timestamp_col.insert(inserted_timestamp_data)

        results = self.timestamp_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.__check_timestamp_ts(
            results, self.start_time, self.start_time + datetime.timedelta(minutes=1), 10)

        results = self.timestamp_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10)),
             (self.start_time + datetime.timedelta(microseconds=10),
              self.start_time + datetime.timedelta(microseconds=20))])

        self.__check_timestamp_ts(
            results, self.start_time, self.start_time + datetime.timedelta(minutes=1), 20)

        # empty result
        out_of_time = self.start_time + datetime.timedelta(hours=10)
        results = self.timestamp_col.get_ranges(
            [(out_of_time, out_of_time + datetime.timedelta(microseconds=10))])
        self.assertEqual(0, len(results))

        # error: column doesn't exist
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.TimestampColumnInfo("lolilol"))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_timestamp_data)

        # error: column of wrong type
        wrong_col = self.my_ts.column(
            quasardb.TimeSeries.TimestampColumnInfo(self.blob_col.name()))

        self.assertRaises(quasardb.OperationError, wrong_col.get_ranges,
                          [(self.start_time,
                            self.start_time + datetime.timedelta(microseconds=10))])
        self.assertRaises(quasardb.OperationError,
                          wrong_col.insert, inserted_timestamp_data)

    def test_timestamp_erase_ranges(self):
        inserted_timestamp_data = _generate_timestamp_ts(
            self.start_time, self.start_time + datetime.timedelta(minutes=1), 1000)
        self.timestamp_col.insert(inserted_timestamp_data)

        results = self.timestamp_col.get_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        erased_count = self.timestamp_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, len(results))

        erased_count = self.timestamp_col.erase_ranges(
            [(self.start_time, self.start_time + datetime.timedelta(microseconds=10))])

        self.assertEqual(erased_count, 0)

        results = self.timestamp_col.get_ranges(
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
        self.create_ts()

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
        self.assertEqual(agg_res[0].range, self.test_intervals[0])
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
        self.create_ts()

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
        self.assertEqual(agg_res[0].range, self.test_intervals[0])
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
            quasardb.TimeSeries.Aggregation.min,
            min(self.inserted_double_data), len(self.inserted_double_data))

    def test_double_aggregation_max(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.max,
            max(self.inserted_double_data), len(self.inserted_double_data))

    def test_double_aggregation_abs_min(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.abs_min,
            min(self.inserted_double_data), len(self.inserted_double_data))

    def test_double_aggregation_abs_max(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.abs_max,
            max(self.inserted_double_data), len(self.inserted_double_data))

    def test_double_aggregation_spread(self):
        self._test_aggregation_of_doubles(
            quasardb.TimeSeries.Aggregation.spread,
            (None,
             max(self.inserted_double_data)[1] - min(self.inserted_double_data)[1]),
            len(self.inserted_double_data))

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


class QuasardbTimeSeriesBulk(QuasardbTimeSeries):

    def setUp(self):
        super(QuasardbTimeSeriesBulk, self).setUp()
        self.create_ts()

    def test_non_existing_bulk_insert(self):
        fake_ts = settings.cluster.ts("this_ts_does_not_exist")

        self.assertRaises(quasardb.OperationError, fake_ts.local_table)

    def _test_with_table(self, local_table):
        start_time = datetime.datetime.now(quasardb.tz)
        current_time = start_time
        tsval = start_time

        val = 1.0
        ival = 0

        for i in xrange(0, 10):
            self.assertEqual(i, local_table.append_row(
                current_time, val, "content", ival, tsval))
            val += 1.0
            ival += 2
            current_time += datetime.timedelta(seconds=1)
            tsval += datetime.timedelta(seconds=5)

        the_ranges = [(start_time - datetime.timedelta(hours=1),
                       start_time + datetime.timedelta(hours=1))]

        results = self.double_col.get_ranges(the_ranges)
        self.assertEqual(len(results), 0)

        results = self.blob_col.get_ranges(the_ranges)
        self.assertEqual(len(results), 0)

        local_table.push()

        #################################
        results = self.double_col.get_ranges(the_ranges)
        self.assertEqual(len(results), 10)

        val = 1.0

        for r in results:
            self.assertEqual(r[1], val)
            val += 1.0

        #################################
        results = self.blob_col.get_ranges(the_ranges)
        self.assertEqual(len(results), 10)

        for r in results:
            self.assertEqual(r[1], "content")

        #################################
        results = self.int64_col.get_ranges(the_ranges)
        self.assertEqual(len(results), 10)

        ival = 0

        for r in results:
            self.assertEqual(r[1], ival)
            ival += 2

        #################################
        results = self.timestamp_col.get_ranges(the_ranges)
        self.assertEqual(len(results), 10)

        tsval = start_time
        for r in results:
            self.assertEqual(r[1], tsval)
            tsval += datetime.timedelta(seconds=5)

    def test_successful_bulk_insert(self):
        local_table = self.my_ts.local_table()
        self._test_with_table(local_table)

    def test_failed_local_table_with_wrong_columns(self):
        columns = [quasardb.TimeSeries.DoubleColumnInfo("1000flavorsofwrong")]

        self.assertRaises(quasardb.OperationError,
                          self.my_ts.local_table, columns)

    def test_successful_bulk_insert_specified_columns(self):
        columns = [quasardb.TimeSeries.DoubleColumnInfo(self.double_col.name()),
                   quasardb.TimeSeries.BlobColumnInfo(self.blob_col.name()),
                   quasardb.TimeSeries.Int64ColumnInfo(self.int64_col.name()),
                   quasardb.TimeSeries.TimestampColumnInfo(
                       self.timestamp_col.name())]

        local_table = self.my_ts.local_table(columns)
        self._test_with_table(local_table)


if __name__ == '__main__':
    if settings.get_lock_status() is False:
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(
            __file__)[0], '..', 'build', 'test', 'test-reports')
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
            output=test_report_directory), exit=False)
        settings.terminate()
