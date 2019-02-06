# # pylint: disable=C0103,C0111,C0302,W0212
# from builtins import range as xrange, int as long  # pylint: disable=W0622
# from functools import reduce  # pylint: disable=W0622
# import datetime
# import os
# import sys
# import unittest
# import settings

# from settings import quasardb

# import numpy as np

# def _generate_dates(start_time, count):
#     return np.array([(start_time + np.timedelta64(i, 's')) for i in range(count)]).astype('datetime64[ns]')

# def _generate_double_ts(start_time, count):
#     return (_generate_dates(start_time, count), np.random.uniform(-100.0, 100.0, count))

# def _generate_int64_ts(start_time, count):
#     return (_generate_dates(start_time, count), np.random.randint(-100, 100, count))

# def _generate_timestamp_ts(start_time, start_val, count):
#     return (_generate_dates(start_time, count), _generate_dates(start_val, count))

# def _generate_blob_ts(start_time, count):
#     dates = _generate_dates(start_time, count)
#     values = np.array([(b"content_" + bytes(item)) for item in range(count)])
#     return (dates, values)

# class QuasardbTimeSeries(unittest.TestCase):
#     def setUp(self):
#         self.entry_name = settings.entry_gen.next()
#         self.my_ts = settings.cluster.ts(self.entry_name)

#         self.start_time = np.datetime64('2017-01-01', 'ns')
#         self.test_intervals = [(self.start_time,
#                                 self.start_time + np.timedelta64(1, 's'))]

#     def _create_ts(self):
#         self.double_col = quasardb.ColumnInfo(quasardb.ColumnType.Double, settings.entry_gen.next())
#         self.blob_col = quasardb.ColumnInfo(quasardb.ColumnType.Blob, settings.entry_gen.next())
#         self.int64_col = quasardb.ColumnInfo(quasardb.ColumnType.Int64, settings.entry_gen.next())
#         self.ts_col = quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, settings.entry_gen.next())

#         self.my_ts.create([self.double_col, self.blob_col, self.int64_col, self.ts_col])

#     def create_ts(self):
#          self._create_ts()


# class QuasardbTimeSeriesNonExisting(QuasardbTimeSeries):

#     def test_list_columns_throws_when_timeseries_does_not_exist(self):
#         self.assertRaises(quasardb.Error, self.my_ts.list_columns)

#     def test_insert_throws_when_timeseries_does_not_exist(self):
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.double_insert, "blah", np.array(np.datetime64('2011-01-01', 'ns')), np.array([1.0]))

#     def test_get_ranges_throws_when_timeseries_does_not_exist(self):
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.double_get_ranges,
#                           "blah",
#                           self.test_intervals)

#     def test_erase_ranges_throw_when_timeseries_does_not_exist(self):
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.erase_ranges,
#                           "blah",
#                           self.test_intervals)

#     def test_create_without_columns(self):
#         self.my_ts.create([])
#         self.assertEqual(len(self.my_ts.list_columns()), 0)

#     def test_create_with_shard_size_less_than_1_millisecond_throws(self):
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.create, [], datetime.timedelta(milliseconds=0))

#     def test_create_with_shard_size_of_1_millisecond(self):
#         self.my_ts.create([], datetime.timedelta(milliseconds=1))
#         self.assertEqual(len(self.my_ts.list_columns()), 0)

#     def test_create_with_shard_size_of_1_day(self):
#         self.my_ts.create([], datetime.timedelta(hours=24))
#         self.assertEqual(len(self.my_ts.list_columns()), 0)

#     def test_create_with_shard_size_of_4_weeks(self):
#         self.my_ts.create([], datetime.timedelta(weeks=4))
#         self.assertEqual(len(self.my_ts.list_columns()), 0)

#     def test_create_with_shard_size_of_more_than_1_year(self):
#         self.my_ts.create([], datetime.timedelta(weeks=52))
#         self.assertEqual(len(self.my_ts.list_columns()), 0)


# class QuasardbTimeSeriesExisting(QuasardbTimeSeries):

#     def setUp(self):
#         super(QuasardbTimeSeriesExisting, self).setUp()
#         self.create_ts()

#     def __check_ts_results(self, results, generated, count):
#         self.assertEqual(2, len(results))
#         np.testing.assert_array_equal(results[0][:count], generated[0][:count])
#         np.testing.assert_array_equal(results[1][:count], generated[1][:count])

#     def test_creation_multiple(self):
#         col_list = self.my_ts.list_columns()
#         self.assertEqual(4, len(col_list))

#         self.assertEqual(col_list[0].name, self.double_col.name)
#         self.assertEqual(col_list[0].type, quasardb.ColumnType.Double)
#         self.assertEqual(col_list[1].name, self.blob_col.name)
#         self.assertEqual(col_list[1].type, quasardb.ColumnType.Blob)
#         self.assertEqual(col_list[2].name, self.int64_col.name)
#         self.assertEqual(col_list[2].type, quasardb.ColumnType.Int64)
#         self.assertEqual(col_list[3].name, self.ts_col.name)
#         self.assertEqual(col_list[3].type, quasardb.ColumnType.Timestamp)

#         # cannot double create
#         self.assertRaises(quasardb.Error, self.my_ts.create,
#                           [quasardb.ColumnInfo(quasardb.ColumnType.Double, settings.entry_gen.next())])

#     def test_double_get_ranges__when_timeseries_is_empty(self):
#         results = self.my_ts.double_get_ranges(self.double_col.name, self.test_intervals)
#         self.assertEqual(2, len(results))
#         self.assertEqual(0, len(results[0]))
#         self.assertEqual(0, len(results[1]))

#     def test_double_erase_ranges__when_timeseries_is_empty(self):
#         erased_count = self.my_ts.erase_ranges(self.double_col.name, self.test_intervals)
#         self.assertEqual(0, erased_count)

#     def test_double_get_ranges(self):
#         inserted_double_data = _generate_double_ts(self.start_time, 1000)
#         self.my_ts.double_insert(self.double_col.name, inserted_double_data[0], inserted_double_data[1])

#         results = self.my_ts.double_get_ranges(self.double_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.__check_ts_results(results, inserted_double_data, 10)

#         results = self.my_ts.double_get_ranges(self.double_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's')),
#              (self.start_time + np.timedelta64(10, 's'),
#               self.start_time + np.timedelta64(20, 's'))])

#         self.__check_ts_results(results, inserted_double_data, 20)

#         # empty result
#         out_of_time = self.start_time + np.timedelta64(10, 'h')
#         results = self.my_ts.double_get_ranges(self.double_col.name,
#             [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
#         self.assertEqual(2, len(results))
#         self.assertEqual(0, len(results[0]))
#         self.assertEqual(0, len(results[1]))

#         # error: column doesn't exist
#         self.assertRaises(quasardb.Error, self.my_ts.double_get_ranges,
#                          "lolilol",
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])

#         self.assertRaises(quasardb.Error,
#                           self.my_ts.double_insert, "lolilol", inserted_double_data[0], inserted_double_data[1])

#         self.assertRaises(quasardb.Error, self.my_ts.blob_get_ranges,
#                           self.double_col.name,
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.blob_insert, self.double_col.name, inserted_double_data[0], inserted_double_data[1])


#     def test_double_erase_ranges(self):
#         inserted_double_data = _generate_double_ts(self.start_time, 1000)
#         self.my_ts.double_insert(self.double_col.name, inserted_double_data[0], inserted_double_data[1])

#         results = self.my_ts.double_get_ranges(self.double_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         erased_count = self.my_ts.erase_ranges(self.double_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, len(results[0]))

#         erased_count = self.my_ts.erase_ranges(self.double_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, 0)

#         results = self.my_ts.double_get_ranges(self.double_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(len(results[0]), 0)

#     def test_int64_get_ranges__when_timeseries_is_empty(self):
#         results = self.my_ts.int64_get_ranges(self.int64_col.name, self.test_intervals)
#         self.assertEqual(0, len(results[0]))

#     def test_int64_erase_ranges__when_timeseries_is_empty(self):
#         erased_count = self.my_ts.erase_ranges(self.int64_col.name, self.test_intervals)
#         self.assertEqual(0, erased_count)

#     def test_int64_get_ranges(self):
#         inserted_int64_data = _generate_int64_ts(self.start_time, 1000)
#         self.my_ts.int64_insert(self.int64_col.name, inserted_int64_data[0], inserted_int64_data[1])

#         results = self.my_ts.int64_get_ranges(self.int64_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.__check_ts_results(results, inserted_int64_data, 10)

#         results = self.my_ts.int64_get_ranges(self.int64_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's')),
#              (self.start_time + np.timedelta64(10, 's'),
#               self.start_time + np.timedelta64(20, 's'))])

#         self.__check_ts_results(results, inserted_int64_data, 20)

#         # empty result
#         out_of_time = self.start_time + np.timedelta64(10, 'h')
#         results = self.my_ts.int64_get_ranges(self.int64_col.name,
#             [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
#         self.assertEqual(0, len(results[0]))

#         # error: column doesn't exist
#         self.assertRaises(quasardb.Error, self.my_ts.int64_get_ranges,
#                           "lolilol",
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.int64_insert,
#                           "lolilol",
#                           inserted_int64_data[0],
#                           inserted_int64_data[1])

#         # error: column of wrong type
#         self.assertRaises(quasardb.Error, self.my_ts.int64_get_ranges,
#                           self.double_col.name,
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.int64_insert,
#                           self.double_col.name,
#                           inserted_int64_data[0],
#                           inserted_int64_data[1])

#     def test_int64_erase_ranges(self):
#         inserted_int64_data = _generate_int64_ts(self.start_time, 1000)
#         self.my_ts.int64_insert(self.int64_col.name, inserted_int64_data[0], inserted_int64_data[1])

#         results = self.my_ts.int64_get_ranges(self.int64_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         erased_count = self.my_ts.erase_ranges(self.int64_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, len(results[0]))

#         erased_count = self.my_ts.erase_ranges(self.int64_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, 0)

#         results = self.my_ts.int64_get_ranges(self.int64_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(len(results[0]), 0)

#     def test_timestamp_get_ranges__when_timeseries_is_empty(self):
#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name, self.test_intervals)
#         self.assertEqual(0, len(results[0]))

#     def test_timestamp_erase_ranges__when_timeseries_is_empty(self):
#         erased_count = self.my_ts.erase_ranges(self.ts_col.name, self.test_intervals)
#         self.assertEqual(0, erased_count)

#     def test_timestamp_get_ranges(self):
#         inserted_timestamp_data = _generate_timestamp_ts(
#             self.start_time, self.start_time + np.timedelta64(1, 'm'), 1000)
#         self.my_ts.timestamp_insert(self.ts_col.name, inserted_timestamp_data[0], inserted_timestamp_data[1])

#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.__check_ts_results(results, inserted_timestamp_data, 10)

#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's')),
#              (self.start_time + np.timedelta64(10, 's'),
#               self.start_time + np.timedelta64(20, 's'))])

#         self.__check_ts_results(results, inserted_timestamp_data, 20)

#         # empty result
#         out_of_time = self.start_time + np.timedelta64(10, 'h')
#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name,
#             [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
#         self.assertEqual(0, len(results[0]))

#         # error: column doesn't exist
#         self.assertRaises(quasardb.Error, self.my_ts.timestamp_get_ranges,
#                           "lolilol",
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.timestamp_insert,
#                           "lolilol",
#                           inserted_timestamp_data[0],
#                           inserted_timestamp_data[1])

#         # error: column of wrong type
#         self.assertRaises(quasardb.Error, self.my_ts.timestamp_get_ranges,
#                           self.double_col.name,
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.timestamp_insert,
#                           "lolilol",
#                           inserted_timestamp_data[0],
#                           inserted_timestamp_data[1])

#     def test_timestamp_erase_ranges(self):
#         inserted_timestamp_data = _generate_timestamp_ts(
#             self.start_time, self.start_time + np.timedelta64(1, 'm'), 1000)
#         self.my_ts.timestamp_insert(self.ts_col.name, inserted_timestamp_data[0], inserted_timestamp_data[1])

#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         erased_count = self.my_ts.erase_ranges(self.ts_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, len(results[0]))

#         erased_count = self.my_ts.erase_ranges(self.ts_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, 0)

#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(len(results[0]), 0)

#     def test_blob_get_ranges__when_timeseries_is_empty(self):
#         results = self.my_ts.blob_get_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])
#         self.assertEqual(0, len(results[0]))

#     def test_blob_erase_ranges__when_timeseries_is_empty(self):
#         erased_count = self.my_ts.erase_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])
#         self.assertEqual(0, erased_count)

#     def test_blob_get_ranges(self):
#         inserted_blob_data = _generate_blob_ts(self.start_time, 20)
#         self.my_ts.blob_insert(self.blob_col.name, inserted_blob_data[0], inserted_blob_data[1])

#         results = self.my_ts.blob_get_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.__check_ts_results(results, inserted_blob_data, 10)

#         results = self.my_ts.blob_get_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's')),
#              (self.start_time + np.timedelta64(10, 's'),
#               self.start_time + np.timedelta64(20, 's'))])

#         self.__check_ts_results(results, inserted_blob_data, 20)

#         # empty result
#         out_of_time = self.start_time + np.timedelta64(10, 'h')
#         results = self.my_ts.blob_get_ranges(self.blob_col.name,
#             [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
#         self.assertEqual(0, len(results[0]))

#         # error: column doesn't exist
#         self.assertRaises(quasardb.Error, self.my_ts.blob_get_ranges,
#                           "lolilol",
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.blob_insert,
#                           "lolilol",
#                           inserted_blob_data[0],
#                           inserted_blob_data[1])

#         # error: column of wrong type
#         self.assertRaises(quasardb.Error, self.my_ts.blob_get_ranges,
#                           self.double_col.name,
#                           [(self.start_time,
#                             self.start_time + np.timedelta64(10, 's'))])
#         self.assertRaises(quasardb.Error,
#                           self.my_ts.blob_insert,
#                           self.double_col.name,
#                           inserted_blob_data[0],
#                           inserted_blob_data[1])

#     def test_blob_erase_ranges(self):
#         inserted_blob_data = _generate_blob_ts(self.start_time, 20)
#         self.my_ts.blob_insert(self.blob_col.name, inserted_blob_data[0], inserted_blob_data[1])

#         results = self.my_ts.blob_get_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         erased_count = self.my_ts.erase_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, len(results[0]))

#         erased_count = self.my_ts.erase_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(erased_count, 0)

#         results = self.my_ts.blob_get_ranges(self.blob_col.name,
#             [(self.start_time, self.start_time + np.timedelta64(10, 's'))])

#         self.assertEqual(len(results[0]), 0)

# @unittest.skip("Not implemented")
# class QuasardbTimeSeriesExistingWithBlobs(QuasardbTimeSeries):

#     def setUp(self):
#         super(QuasardbTimeSeriesExistingWithBlobs, self).setUp()
#         self.create_ts()

#         self.inserted_blob_data = _generate_blob_ts(self.start_time, 5)
#         self.inserted_blob_col = [x[1] for x in self.inserted_blob_data]

#         self.blob_col.insert(self.inserted_blob_data)

#         self.test_intervals = [
#             (self.start_time, self.start_time + datetime.timedelta(days=1))]

#     def test_blob_get_ranges_out_of_time(self):
#         out_of_time = self.start_time + datetime.timedelta(hours=10)
#         results = self.blob_col.get_ranges(
#             [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
#         self.assertEqual(0, len(results))

#     def _test_aggregation_of_blobs(self, agg_type, expected, expected_count):
#         agg = quasardb.TimeSeries.BlobAggregations()
#         agg.append(agg_type, self.test_intervals[0])

#         agg_res = self.blob_col.aggregate(agg)

#         self.assertEqual(1, len(agg_res))
#         self.assertEqual(agg_res[0].range, self.test_intervals[0])
#         timestamp = expected[0]
#         if timestamp is None:
#             timestamp = datetime.datetime.fromtimestamp(0, quasardb.tz)
#         self.assertEqual(agg_res[0].timestamp, timestamp)
#         self.assertEqual(agg_res[0].count, expected_count)
#         expected_length = long(0)
#         if expected[1] is not None:
#             expected_length = len(expected[1])
#         self.assertEqual(agg_res[0].content_length, expected_length)
#         self.assertEqual(agg_res[0].content, expected[1])

#     def test_blob_aggregation_count(self):
#         self._test_aggregation_of_blobs(quasardb.TimeSeries.Aggregation.count,
#                                         (None, None), len(self.inserted_blob_data))

#     def test_blob_aggregation_first(self):
#         self._test_aggregation_of_blobs(quasardb.TimeSeries.Aggregation.first,
#                                         self.inserted_blob_data[0], 1)

#     def test_blob_aggregation_last(self):
#         self._test_aggregation_of_blobs(quasardb.TimeSeries.Aggregation.last,
#                                         self.inserted_blob_data[-1], 1)

# @unittest.skip("Not implemented")
# class QuasardbTimeSeriesExistingWithDoubles(QuasardbTimeSeries):

#     def setUp(self):
#         super(QuasardbTimeSeriesExistingWithDoubles, self).setUp()
#         self.create_ts()

#         self.inserted_double_data = _generate_double_ts(
#             self.start_time, 1000)
#         self.inserted_double_col = [x[1] for x in self.inserted_double_data]

#         self.double_col.insert(self.inserted_double_data)

#         self.test_intervals = [
#             (self.start_time, self.start_time + datetime.timedelta(days=1))]

#     def test_double_get_ranges_out_of_time(self):
#         out_of_time = self.start_time + datetime.timedelta(hours=10)
#         results = self.double_col.get_ranges(
#             [(out_of_time, out_of_time + np.timedelta64(10, 's'))])
#         self.assertEqual(0, len(results))

#     def _test_aggregation_of_doubles(self, agg_type, expected, expected_count):
#         agg = quasardb.TimeSeries.DoubleAggregations()
#         agg.append(agg_type, self.test_intervals[0])

#         agg_res = self.double_col.aggregate(agg)

#         self.assertEqual(1, len(agg_res))
#         self.assertEqual(agg_res[0].range, self.test_intervals[0])
#         timestamp = expected[0]
#         if timestamp is None:
#             timestamp = datetime.datetime.fromtimestamp(0, quasardb.tz)
#         self.assertEqual(agg_res[0].timestamp, timestamp)
#         self.assertEqual(agg_res[0].count, expected_count)
#         self.assertEqual(agg_res[0].value, expected[1])

#     def test_double_aggregation_first(self):
#         self._test_aggregation_of_doubles(
#             quasardb.TimeSeries.Aggregation.first, self.inserted_double_data[0], 1)

#     def test_double_aggregation_last(self):
#         self._test_aggregation_of_doubles(
#             quasardb.TimeSeries.Aggregation.last, self.inserted_double_data[-1], 1)

#     def test_double_aggregation_min(self):
#         self._test_aggregation_of_doubles(
#             quasardb.TimeSeries.Aggregation.min,
#             min(self.inserted_double_data), len(self.inserted_double_data))

#     def test_double_aggregation_max(self):
#         self._test_aggregation_of_doubles(
#             quasardb.TimeSeries.Aggregation.max,
#             max(self.inserted_double_data), len(self.inserted_double_data))

#     def test_double_aggregation_abs_min(self):
#         self._test_aggregation_of_doubles(
#             quasardb.TimeSeries.Aggregation.abs_min,
#             min(self.inserted_double_data), len(self.inserted_double_data))

#     def test_double_aggregation_abs_max(self):
#         self._test_aggregation_of_doubles(
#             quasardb.TimeSeries.Aggregation.abs_max,
#             max(self.inserted_double_data), len(self.inserted_double_data))

#     def test_double_aggregation_spread(self):
#         self._test_aggregation_of_doubles(
#             quasardb.TimeSeries.Aggregation.spread,
#             (None,
#              max(self.inserted_double_data)[1] - min(self.inserted_double_data)[1]),
#             len(self.inserted_double_data))

#     def test_double_aggregation_count(self):
#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.count,
#                                           (None, len(self.inserted_double_data)),
#                                           len(self.inserted_double_data))

#     def test_double_aggregation_arithmetic_mean(self):
#         computed_sum = reduce(lambda x, y: x + y,
#                               self.inserted_double_col, 0.0)

#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.arithmetic_mean,
#                                           (None, computed_sum /
#                                            len(self.inserted_double_data)),
#                                           len(self.inserted_double_data))

#     def test_double_aggregation_sum(self):
#         computed_sum = reduce(lambda x, y: x + y,
#                               self.inserted_double_col, 0.0)

#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sum,
#                                           (None, computed_sum), len(self.inserted_double_data))

#     def test_double_aggregation_sum_of_squares(self):
#         computed_sum = reduce(lambda x, y: x + y * y,
#                               self.inserted_double_col, 0.0)

#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sum_of_squares,
#                                           (None, computed_sum), len(self.inserted_double_data))

#     def test_double_aggregation_population_variance(self):
#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.population_variance,
#                                           (None, numpy.var(
#                                               self.inserted_double_col)),
#                                           len(self.inserted_double_data))

#     def test_double_aggregation_population_stddev(self):
#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.population_stddev,
#                                           (None, numpy.std(
#                                               self.inserted_double_col)),
#                                           len(self.inserted_double_data))

#     def test_double_aggregation_sample_variance(self):
#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sample_variance,
#                                           (None,
#                                            numpy.var(self.inserted_double_col, ddof=1)),
#                                           len(self.inserted_double_data))

#     def test_double_aggregation_sample_stddev(self):
#         self._test_aggregation_of_doubles(quasardb.TimeSeries.Aggregation.sample_stddev,
#                                           (None,
#                                            numpy.std(self.inserted_double_col, ddof=1)),
#                                           len(self.inserted_double_data))

# if __name__ == '__main__':
#     if settings.get_lock_status() is False:
#         settings.init()
#         test_directory = os.getcwd()
#         test_report_directory = os.path.join(os.path.split(
#             __file__)[0], '..', 'build', 'test', 'test-reports')
#         import xmlrunner
#         unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
#             output=test_report_directory), exit=False)
#         settings.terminate()
