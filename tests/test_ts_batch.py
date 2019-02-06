# # pylint: disable=C0103,C0111,C0302,W0212
# from builtins import range as xrange, int as long  # pylint: disable=W0622
# from functools import reduce  # pylint: disable=W0622
# import datetime
# import os
# import sys
# import unittest
# import settings
# import test_ts as tslib
# from time import sleep

# from settings import quasardb

# import numpy as np

# def _row_insertion_method(tester, batch_inserter, dates, doubles, blobs, integers, timestamps):
#      for i in range(len(dates)):
#         batch_inserter.start_row(dates[i])
#         batch_inserter.set_double(0, doubles[i])
#         batch_inserter.set_blob(1, blobs[i])
#         batch_inserter.set_int64(2, integers[i])
#         batch_inserter.set_timestamp(3, timestamps[i])

# def _regular_push(batch_inserter):
#     batch_inserter.push()


# def _async_push(batch_inserter):
#     batch_inserter.push_async()
#     # Wait for push_async to complete
#     # Ideally we could be able to get the proper flush interval
#     sleep(8)

# class QuasardbTimeSeriesBulk(tslib.QuasardbTimeSeries):

#     def setUp(self):
#         super(QuasardbTimeSeriesBulk, self).setUp()
#         self.create_ts()

#     def test_non_existing_bulk_insert(self):
#         self.assertRaises(quasardb.Error,
#             settings.cluster.ts_batch,
#             [quasardb.BatchColumnInfo(settings.entry_gen.next(), "col", 10)])

#     def _make_ts_batch_info(self):
#         return [quasardb.BatchColumnInfo(self.entry_name, self.double_col.name, 100),
#                 quasardb.BatchColumnInfo(self.entry_name, self.blob_col.name, 100),
#                 quasardb.BatchColumnInfo(self.entry_name, self.int64_col.name, 100),
#                 quasardb.BatchColumnInfo(self.entry_name, self.ts_col.name, 100)]

#     def _test_with_table(self, batch_inserter, insertion_method, push_method = _regular_push):
#         start_time = np.datetime64('2017-01-01', 'ns')

#         count = 10

#         dates = tslib._generate_dates(start_time, count)

#         # range is right exclusive, so the timestamp has to be beyond
#         whole_range = (dates[0], dates[-1:] + np.timedelta64(1, 'ns'))

#         doubles = np.random.uniform(-100.0, 100.0, count)
#         integers = np.random.randint(-100, 100, count)
#         blobs = np.array([(b"content_" + bytes(i)) for i in range(count)])
#         timestamps = tslib._generate_dates(start_time + np.timedelta64('1', 'D'), count)

#         insertion_method(self, batch_inserter, dates, doubles, blobs, integers, timestamps)

#         # before the push, there is nothing
#         results = self.my_ts.double_get_ranges(self.double_col.name, [whole_range])
#         self.assertEqual(0, len(results[0]))

#         results = self.my_ts.blob_get_ranges(self.blob_col.name, [whole_range])
#         self.assertEqual(0, len(results[0]))

#         results = self.my_ts.int64_get_ranges(self.int64_col.name, [whole_range])
#         self.assertEqual(0, len(results[0]))

#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name, [whole_range])
#         self.assertEqual(0, len(results[0]))

#         # after push, there is everything
#         push_method(batch_inserter)

#         results = self.my_ts.double_get_ranges(self.double_col.name, [whole_range])
#         np.testing.assert_array_equal(results[0], dates)
#         np.testing.assert_array_equal(results[1], doubles)

#         results = self.my_ts.blob_get_ranges(self.blob_col.name, [whole_range])
#         np.testing.assert_array_equal(results[0], dates)
#         np.testing.assert_array_equal(results[1], blobs)

#         results = self.my_ts.int64_get_ranges(self.int64_col.name, [whole_range])
#         np.testing.assert_array_equal(results[0], dates)
#         np.testing.assert_array_equal(results[1], integers)

#         results = self.my_ts.timestamp_get_ranges(self.ts_col.name, [whole_range])
#         np.testing.assert_array_equal(results[0], dates)
#         np.testing.assert_array_equal(results[1], timestamps)

#     def test_successful_bulk_row_insert(self):
#         batch_inserter = settings.cluster.ts_batch(self._make_ts_batch_info())
#         self._test_with_table(batch_inserter, _row_insertion_method, _regular_push)

#     # Same test as `test_successful_bulk_row_insert` but using `push_async` to push the entries
#     # This allows us to test the `push_async` feature
#     def test_successful_bulk_row_insert_with_push_async(self):
#         batch_inserter = settings.cluster.ts_batch(self._make_ts_batch_info())
#         self._test_with_table(batch_inserter, _row_insertion_method, _async_push)

#     def test_failed_local_table_with_wrong_columns(self):
#         columns = [quasardb.BatchColumnInfo(self.entry_name, "1000flavorsofwrong", 10)]
#         self.assertRaises(quasardb.Error, settings.cluster.ts_batch, columns)

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
