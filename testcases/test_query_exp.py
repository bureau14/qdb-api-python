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
import math

for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings
import test_ts as tslib
from quasardb import impl


class TsHelper():

    def __init__(self):
        self.setUp()
        self.create_ts()
        return

    def __del__(self):
        return

    def setUp(self):
        self.entry_name = settings.entry_gen.next()
        self.my_ts = settings.cluster.ts(self.entry_name)

        self.start_time = datetime.datetime(2017, 1, 1, tzinfo=pytz.UTC)
        self.test_intervals = [(self.start_time,
                                self.start_time + datetime.timedelta(microseconds=1))]

    def _create_ts(self):
        self.double_column_name = settings.entry_gen.next()
        self.blob_column_name = settings.entry_gen.next()
        self.int64_column_name = settings.entry_gen.next()
        self.timestamp_column_name = settings.entry_gen.next()
        cols = self.my_ts.create([
            quasardb.TimeSeries.DoubleColumnInfo(self.double_column_name),
            quasardb.TimeSeries.BlobColumnInfo(self.blob_column_name),
            quasardb.TimeSeries.Int64ColumnInfo(self.int64_column_name),
            quasardb.TimeSeries.TimestampColumnInfo(self.timestamp_column_name)
        ])
        return (cols[0], cols[1], cols[2], cols[3])

    def create_ts(self):
        (self.double_col, self.blob_col, self.int64_col,
         self.timestamp_col) = self._create_ts()


class QuasardbQueryExpErrorCodeCheck(unittest.TestCase):

    def test_returns_invalid_argument_for_null_query(self):
        self.assertRaises(quasardb.InputError,
                          settings.cluster.query_exp, None)

    def test_returns_invalid_argument_for_empty_query(self):
        self.assertRaises(quasardb.InputError, settings.cluster.query_exp, '')

    def test_returns_invalid_argument_with_invalid_query(self):
        self.assertRaises(quasardb.InputError,
                          settings.cluster.query_exp, 'select * from')

    def test_returns_alias_not_found_when_ts_doesnt_exist(self):
        self.assertRaises(quasardb.OperationError, settings.cluster.query_exp,
                          'select * from ' + 'this_ts_doesnt_exist' + ' in range(2017, +10d)')

    def test_returns_alias_not_found_when_untagged(self):
        non_existing_tag = settings.entry_gen.next()
        self.assertRaises(quasardb.OperationError, settings.cluster.query_exp,
                          "select * from find(tag='" + non_existing_tag + "') in range(2017, +10d)")

    def test_returns_columns_not_found(self):
        non_existing_column = settings.entry_gen.next()
        helper = TsHelper()
        self.assertRaises(quasardb.OperationError, settings.cluster.query_exp, "select " +
                          non_existing_column + " from " + helper.entry_name + " in range(2017, +10d)")


class QuasardbQueryExp(unittest.TestCase):

    def generate_ts_with_double_points(self, points=10):
        helper = TsHelper()
        inserted_double_data = tslib._generate_double_ts(
            helper.start_time, 1.0, points)
        helper.double_col.insert(inserted_double_data)
        return helper, inserted_double_data

    def trivial_test(self, helper, scanned_rows_count, res, rows_count, columns_count):
        self.assertEqual(res.scanned_rows_count, scanned_rows_count)
        self.assertEqual(res.tables_count, 1)
        self.assertEqual(len(res.tables), 1)
        self.assertEqual(res.tables[0].rows_count, rows_count)
        self.assertEqual(res.tables[0].columns_count, columns_count)
        self.assertEqual(res.tables[0].columns_names[0], "timestamp")

    def test_returns_empty_result(self):
        helper = TsHelper()
        res = settings.cluster.query_exp(
            "select * from " + helper.entry_name + " in range(2016-01-01 , 2016-12-12)")
        self.assertEqual(res.scanned_rows_count, 0)
        self.assertEqual(res.tables_count, 0)
        self.assertEqual(len(res.tables), 0)

    def test_returns_inserted_data_with_star_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp(
            "select * from " + helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        # Column count is 5, because, uninit, int64, blob, timestamp, double
        self.trivial_test(helper, len(inserted_double_data),
                          res, len(inserted_double_data), 5)
        for rc in range(res.tables[0].rows_count):
            self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
                res.tables[0].get_payload(rc, 0)[1]), inserted_double_data[rc][0])
            self.assertEqual(res.tables[0].get_payload(
                rc, 1)[1], inserted_double_data[rc][1])

    def test_returns_inserted_data_with_star_select_and_tag_lookup(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        tag_name = settings.entry_gen.next()
        helper.my_ts.attach_tag(tag_name)
        res = settings.cluster.query_exp("select * from find(tag = " + '"' + tag_name + '")' +
                                         " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        # Column count is 5, because, uninit, int64, blob, timestamp, double
        self.trivial_test(helper, len(inserted_double_data),
                          res, len(inserted_double_data), 5)
        for rc in range(res.tables[0].rows_count):
            self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
                res.tables[0].get_payload(rc, 0)[1]), inserted_double_data[rc][0])
            self.assertEqual(res.tables[0].get_payload(
                rc, 1)[1], inserted_double_data[rc][1])

    def test_returns_inserted_data_with_column_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp("select " + helper.double_column_name + " from " +
                                         helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        self.trivial_test(helper, len(inserted_double_data),
                          res, len(inserted_double_data), 2)
        self.assertEqual(
            res.tables[0].columns_names[1], helper.double_column_name)
        for rc in range(res.tables[0].rows_count):
            self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
                res.tables[0].get_payload(rc, 0)[1]), inserted_double_data[rc][0])
            self.assertEqual(res.tables[0].get_payload(
                rc, 1)[1], inserted_double_data[rc][1])

    def test_returns_inserted_data_twice_with_double_column_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp("select " + helper.double_column_name + "," + helper.double_column_name +
                                         " from " + helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        self.trivial_test(helper, len(inserted_double_data),
                          res, len(inserted_double_data), 3)
        self.assertEqual(
            res.tables[0].columns_names[1], helper.double_column_name)
        for rc in range(res.tables[0].rows_count):
            self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
                res.tables[0].get_payload(rc, 0)[1]), inserted_double_data[rc][0])
            self.assertEqual(res.tables[0].get_payload(
                rc, 1)[1], inserted_double_data[rc][1])
            self.assertEqual(res.tables[0].get_payload(
                rc, 2)[1], inserted_double_data[rc][1])

    def test_returns_sum_with_sum_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp("select sum(" + helper.double_column_name + ") from " +
                                         helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        self.trivial_test(helper, len(inserted_double_data), res, 1, 2)
        self.assertEqual(res.tables[0].columns_names[1],
                         "sum(" + helper.double_column_name + ")")
        expected_sum = 10.0 * (10.0 + 1.0) / 2.0
        self.assertEqual(expected_sum, res.tables[0].get_payload(0, 1)[1])

    def test_returns_sum_with_sum_divided_by_count_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp("select sum(" + helper.double_column_name + ")/count(" + helper.double_column_name +
                                         ") from " + helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        self.trivial_test(helper, 2 * len(inserted_double_data), res, 1, 2)
        self.assertEqual(res.tables[0].columns_names[1], "(sum(" +
                         helper.double_column_name + ")/count(" + helper.double_column_name + "))")
        expected_avg = (10.0 * (10.0 + 1.0)) / (10.0 * 2.0)
        self.assertEqual(expected_avg, res.tables[0].get_payload(0, 1)[1])

    def test_returns_max_minus_min_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp("select max(" + helper.double_column_name + ") - min(" + helper.double_column_name +
                                         ") from " + helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        self.trivial_test(helper, len(inserted_double_data) * 2.0, res, 1, 2)
        self.assertEqual(res.tables[0].columns_names[1], "(max(" +
                         helper.double_column_name + ")-min(" + helper.double_column_name + "))")
        self.assertEqual(10.0 - 1.0, res.tables[0].get_payload(0, 1)[1])

    def test_returns_max_minus_1_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp("select max(" + helper.double_column_name + ") - 1 from " +
                                         helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        self.trivial_test(helper, len(inserted_double_data), res, 1, 2)
        self.assertEqual(res.tables[0].columns_names[1],
                         "(max(" + helper.double_column_name + ")-1)")
        self.assertEqual(10.0 - 1.0, res.tables[0].get_payload(0, 1)[1])

    def test_returns_max_and_scalar_1_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        res = settings.cluster.query_exp("select max(" + helper.double_column_name + "), 1 from " +
                                         helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        self.trivial_test(helper, len(inserted_double_data), res, 2, 3)
        self.assertEqual(res.tables[0].columns_names[1],
                         "max(" + helper.double_column_name + ")")
        self.assertEqual(res.tables[0].columns_names[2], "1")

        self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
            res.tables[0].get_payload(0, 0)[1]), datetime.datetime(1970, 1, 1, 0, 0,  tzinfo=pytz.UTC))
        self.assertTrue(math.isnan(res.tables[0].get_payload(0, 1)[1]))
        self.assertEqual(res.tables[0].get_payload(0, 2)[1], 1)
        self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
            res.tables[0].get_payload(1, 0)[1]), inserted_double_data[len(inserted_double_data) - 1][0])
        self.assertEqual(res.tables[0].get_payload(
            1, 1)[1], inserted_double_data[len(inserted_double_data) - 1][1])
        self.assertEqual(res.tables[0].get_payload(1, 2)[1], 0)

    def test_returns_inserted_multi_data_with_star_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        inserted_blob_data = tslib._generate_blob_ts(helper.start_time, 10)
        inserted_int64_data = tslib._generate_int64_ts(
            helper.start_time, 10000000000, 10)
        inserted_timestamp_data = tslib._generate_timestamp_ts(
            helper.start_time, helper.start_time + datetime.timedelta(minutes=1), 10)

        helper.blob_col.insert(inserted_blob_data)
        helper.int64_col.insert(inserted_int64_data)
        helper.timestamp_col.insert(inserted_timestamp_data)

        res = settings.cluster.query_exp(
            "select * from " + helper.entry_name + " in range(" + str(inserted_double_data[0][0].year) + ", +100d)")
        # Column count is 5, because, uninit, int64, blob, timestamp, double
        self.trivial_test(helper, 4 * len(inserted_double_data),
                          res, len(inserted_double_data), 5)

        for rc in range(res.tables[0].rows_count):
            self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
                res.tables[0].get_payload(rc, 0)[1]), inserted_double_data[rc][0])
            self.assertEqual(res.tables[0].get_payload(
                rc, 1)[1], inserted_double_data[rc][1])
            self.assertEqual(res.tables[0].get_payload(
                rc, 2)[1], inserted_blob_data[rc][1])
            self.assertEqual(res.tables[0].get_payload(
                rc, 3)[1], inserted_int64_data[rc][1])
            self.assertEqual(quasardb.qdb_convert.convert_qdb_timespec_to_time(
                res.tables[0].get_payload(rc, 4)[1]), inserted_timestamp_data[rc][1])


if __name__ == '__main__':
    if settings.get_lock_status() == False:
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(
            __file__)[0], '..', 'build', 'test', 'test-reports')
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
            output=test_report_directory), exit=False)
        settings.terminate()
