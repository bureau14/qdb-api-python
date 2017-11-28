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


for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))
import quasardb  # pylint: disable=C0413,E0401
import settings

class QuasardbTimeUtils(unittest.TestCase):

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
            self.assertEqual(datetime.datetime.fromtimestamp(
                quasardb.qdb_convert.time_to_unix_timestamp(r)), r.astimezone(pytz.UTC))

    def test_duration_converter(self):
        '''
        Test conversion of time durations.
        '''

        self.assertEqual(
            24 * 3600 * 1000, quasardb.qdb_convert.duration_to_timeout_ms(datetime.timedelta(days=1)))
        self.assertEqual(
            3600 * 1000, quasardb.qdb_convert.duration_to_timeout_ms(datetime.timedelta(hours=1)))
        self.assertEqual(
            60 * 1000, quasardb.qdb_convert.duration_to_timeout_ms(datetime.timedelta(minutes=1)))
        self.assertEqual(1000, quasardb.qdb_convert.duration_to_timeout_ms(
            datetime.timedelta(seconds=1)))
        self.assertEqual(1, quasardb.qdb_convert.duration_to_timeout_ms(
            datetime.timedelta(milliseconds=1)))
        self.assertEqual(0, quasardb.qdb_convert.duration_to_timeout_ms(
            datetime.timedelta(microseconds=1)))

    def test_ts_convert(self):
        orig_couple = [(datetime.datetime.now(quasardb.tz),
                        datetime.datetime.now(quasardb.tz))]
        converted_couple = quasardb.qdb_convert.convert_time_couples_to_qdb_filtered_range_t_vector(
            orig_couple)
        self.assertEqual(len(converted_couple), 1)
        self.assertEqual(orig_couple[0], quasardb.qdb_convert.convert_qdb_filtered_range_t_to_time_couple(
            converted_couple[0]))

if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()
