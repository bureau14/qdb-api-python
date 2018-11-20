# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import os
import sys
import unittest
import pytz
import settings
import test_ts as tslib

from settings import quasardb
import numpy as np

class TsHelper(object):

    def __init__(self):
        self.setUp()
        self.create_ts()
        return

    def __del__(self):
        return

    def setUp(self):
        self.entry_name = settings.entry_gen.next()
        self.my_ts = settings.cluster.ts(self.entry_name)

        self.start_time = np.datetime64('2017-01-01', 'ns')
        self.start_year = self.start_time.astype('datetime64[Y]').astype(int) + 1970

    def _create_ts(self):
        self.double_col = quasardb.ColumnInfo(quasardb.ColumnType.Double, settings.entry_gen.next())
        self.blob_col = quasardb.ColumnInfo(quasardb.ColumnType.Blob, settings.entry_gen.next())
        self.int64_col = quasardb.ColumnInfo(quasardb.ColumnType.Int64, settings.entry_gen.next())
        self.ts_col = quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, settings.entry_gen.next())

        self.my_ts.create([self.double_col, self.blob_col, self.int64_col, self.ts_col])

    def create_ts(self):
        self._create_ts()

class QuasardbQueryExpErrorCodeCheck(unittest.TestCase):

    def test_returns_invalid_argument_for_null_query(self):
        self.assertRaises(TypeError,
                          settings.cluster.query, None)

    def test_returns_invalid_argument_for_empty_query(self):
        q = settings.cluster.query('')
        self.assertRaises(quasardb.Error, q.run)

    def test_returns_invalid_argument_with_invalid_query(self):
        q = settings.cluster.query('select * from')
        self.assertRaises(quasardb.Error, q.run)

    def test_returns_alias_not_found_when_ts_doesnt_exist(self):
        q = settings.cluster.query('select * from ' + 'this_ts_doesnt_exist' + ' in range(2017, +10d)')
        self.assertRaises(quasardb.Error, q.run)

    def test_returns_alias_not_found_when_untagged(self):
        non_existing_tag = settings.entry_gen.next()
        q = settings.cluster.query("select * from find(tag='" + non_existing_tag + "') in range(2017, +10d)")
        self.assertRaises(quasardb.Error, q.run)

    def test_returns_columns_not_found(self):
        non_existing_column = settings.entry_gen.next()
        helper = TsHelper()
        q = settings.cluster.query("select " + non_existing_column + " from " + helper.entry_name + " in range(2017, +10d)")
        self.assertRaises(quasardb.Error, q.run)


class QuasardbQueryExp(unittest.TestCase):

    def generate_ts_with_double_points(self, points=10):
        helper = TsHelper()
        inserted_double_data = tslib._generate_double_ts(helper.start_time, points)
        helper.my_ts.double_insert(helper.double_col.name, inserted_double_data[0], inserted_double_data[1])
        return helper, inserted_double_data

    def trivial_test(self, ts_name, scanned_point_count, res, rows_count, columns_count):
        self.assertEqual(res.scanned_point_count, scanned_point_count)
        self.assertEqual(len(res.tables), 1)
        self.assertEqual(len(res.tables[ts_name][0].data), rows_count)
        self.assertEqual(len(res.tables[ts_name]), columns_count)
        self.assertEqual(res.tables[ts_name][0].name, "timestamp")

    def test_returns_empty_result(self):
        helper = TsHelper()
        res = settings.cluster.query(
            "select * from " + helper.entry_name + " in range(2016-01-01 , 2016-12-12)").run()
        self.assertEqual(res.scanned_point_count, 0)
        self.assertEqual(len(res.tables), 0)

    def test_returns_inserted_data_with_star_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select * from " + helper.entry_name + \
            " in range(" + str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()
        # Column count is 5, because, uninit, int64, blob, timestamp, double
        self.trivial_test(helper.entry_name, len(inserted_double_data[0]),
                          res, len(inserted_double_data[0]), 5)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        np.testing.assert_array_equal(res.tables[helper.entry_name][0].data, inserted_double_data[0])
        self.assertEqual(res.tables[helper.entry_name][1].name, helper.double_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][1].data, inserted_double_data[1])

    def test_returns_inserted_data_with_star_select_and_tag_lookup(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        tag_name = settings.entry_gen.next()
        helper.my_ts.attach_tag(tag_name)
        query = "select * from find(tag = " + '"' + tag_name + '")' + \
            " in range(" + str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()
        # Column count is 5, because, uninit, int64, blob, timestamp, double
        self.trivial_test(helper.entry_name, len(inserted_double_data[0]),
                          res, len(inserted_double_data[0]), 5)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        np.testing.assert_array_equal(res.tables[helper.entry_name][0].data, inserted_double_data[0])
        self.assertEqual(res.tables[helper.entry_name][1].name, helper.double_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][1].data, inserted_double_data[1])

    def test_returns_inserted_data_with_column_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select " + helper.double_col.name + " from " + helper.entry_name + \
            " in range(" + str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()
        self.trivial_test(helper.entry_name, len(inserted_double_data[0]),
                          res, len(inserted_double_data[0]), 2)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        np.testing.assert_array_equal(res.tables[helper.entry_name][0].data, inserted_double_data[0])
        self.assertEqual(res.tables[helper.entry_name][1].name, helper.double_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][1].data, inserted_double_data[1])

    def test_returns_inserted_data_twice_with_double_column_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select " + helper.double_col.name + "," + helper.double_col.name + \
            " from " + helper.entry_name + \
                " in range(" + \
            str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()
        self.trivial_test(helper.entry_name, len(inserted_double_data[0]),
                          res, len(inserted_double_data[0]), 2)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        np.testing.assert_array_equal(res.tables[helper.entry_name][0].data, inserted_double_data[0])
        self.assertEqual(res.tables[helper.entry_name][1].name, helper.double_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][1].data, inserted_double_data[1])

    def test_returns_sum_with_sum_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select sum(" + helper.double_col.name + ") from " + helper.entry_name + \
                " in range(" + \
                str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()
        self.trivial_test(helper.entry_name, len(inserted_double_data[0]), res, 1, 2)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        self.assertEqual(res.tables[helper.entry_name][0].data[0], np.datetime64('NaT'))
        self.assertEqual(res.tables[helper.entry_name][1].name,  "sum(" + helper.double_col.name + ")")
        self.assertAlmostEqual(res.tables[helper.entry_name][1].data[0], np.sum(inserted_double_data[1]))

    def test_returns_sum_with_sum_divided_by_count_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select sum(" + helper.double_col.name + ")/count(" + \
            helper.double_col.name + ") from " + helper.entry_name + \
                " in range(" + \
            str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()
        self.trivial_test(helper.entry_name, len(inserted_double_data[0]) * 2, res, 1, 2)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        self.assertEqual(res.tables[helper.entry_name][0].data[0], np.datetime64('NaT'))
        self.assertEqual(res.tables[helper.entry_name][1].name,  "(sum(" +
                         helper.double_col.name + ")/count(" + helper.double_col.name + "))")
        self.assertAlmostEqual(res.tables[helper.entry_name][1].data[0], np.average(inserted_double_data[1]))

    def test_returns_max_minus_min_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select max(" + helper.double_col.name + ") - min(" + \
            helper.double_col.name + ") from " + helper.entry_name + \
            " in range(" + \
            str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()

        self.trivial_test(helper.entry_name, len(inserted_double_data[0]) * 2, res, 1, 2)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        self.assertEqual(res.tables[helper.entry_name][0].data[0], np.datetime64('NaT'))
        self.assertEqual(res.tables[helper.entry_name][1].name,  "(max(" +
                         helper.double_col.name + ")-min(" + helper.double_col.name + "))")
        self.assertAlmostEqual(res.tables[helper.entry_name][1].data[0], np.max(inserted_double_data[1]) - np.min(inserted_double_data[1]))

    def test_returns_max_minus_1_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select max(" + helper.double_col.name + ") - 1 from " + \
            helper.entry_name + \
                " in range(" + \
                str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()

        self.trivial_test(helper.entry_name, len(inserted_double_data[0]), res, 1, 2)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        self.assertGreaterEqual(res.tables[helper.entry_name][0].data[0], helper.start_time)
        self.assertEqual(res.tables[helper.entry_name][1].name,  "(max(" +
                         helper.double_col.name + ")-1)")
        self.assertAlmostEqual(res.tables[helper.entry_name][1].data[0], np.max(inserted_double_data[1]) -1)

    def test_returns_max_and_scalar_1_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        query = "select max(" + helper.double_col.name + "), 1 from " + \
            helper.entry_name + \
                " in range(" + \
                str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()

        self.assertEqual(len(res.tables), 2)

        self.assertEqual(res.tables["$none"][0].name, "timestamp")
        self.assertEqual(res.tables["$none"][0].data[0], np.datetime64('NaT'))
        self.assertEqual(res.tables["$none"][1].name,  "max(" + helper.double_col.name + ")")
        self.assertEqual(res.tables["$none"][2].name,  "1")
        self.assertEqual(res.tables["$none"][2].data[0], 1)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        self.assertGreaterEqual(res.tables[helper.entry_name][0].data[0], helper.start_time)
        self.assertEqual(res.tables[helper.entry_name][1].name,  "max(" + helper.double_col.name + ")")
        self.assertAlmostEqual(res.tables[helper.entry_name][1].data[0], np.max(inserted_double_data[1]))
        self.assertEqual(res.tables[helper.entry_name][2].name,  "1")

    def test_returns_inserted_multi_data_with_star_select(self):
        helper, inserted_double_data = self.generate_ts_with_double_points()
        inserted_blob_data = tslib._generate_blob_ts(helper.start_time, 10)
        inserted_int64_data = tslib._generate_int64_ts(helper.start_time, 10)
        inserted_timestamp_data = tslib._generate_timestamp_ts(helper.start_time, helper.start_time, 10)

        helper.my_ts.blob_insert(helper.blob_col.name, inserted_blob_data[0], inserted_blob_data[1])
        helper.my_ts.int64_insert(helper.int64_col.name, inserted_int64_data[0], inserted_int64_data[1])
        helper.my_ts.timestamp_insert(helper.ts_col.name, inserted_timestamp_data[0], inserted_timestamp_data[1])

        query = "select * from " + helper.entry_name + \
            " in range(" + str(helper.start_year) + ", +100d)"
        res = settings.cluster.query(query).run()

        # Column count is 5, because, uninit, int64, blob, timestamp, double
        self.trivial_test(helper.entry_name, 4 * len(inserted_double_data[0]),
                          res, len(inserted_double_data[0]), 5)

        self.assertEqual(res.tables[helper.entry_name][0].name, "timestamp")
        np.testing.assert_array_equal(res.tables[helper.entry_name][0].data, inserted_double_data[0])
        self.assertEqual(res.tables[helper.entry_name][1].name, helper.double_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][1].data, inserted_double_data[1])
        self.assertEqual(res.tables[helper.entry_name][2].name, helper.blob_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][2].data, inserted_blob_data[1])
        self.assertEqual(res.tables[helper.entry_name][3].name, helper.int64_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][3].data, inserted_int64_data[1])
        self.assertEqual(res.tables[helper.entry_name][4].name, helper.ts_col.name)
        np.testing.assert_array_equal(res.tables[helper.entry_name][4].data, inserted_timestamp_data[1])

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
