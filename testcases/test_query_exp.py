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


for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings

class QuasardbQueryExp(unittest.TestCase):
    
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
        ])
        self.assertEqual(2, len(cols))

        return (cols[0], cols[1])

    def create_ts(self):
        (self.double_col, self.blob_col) = self._create_ts()
        self.double_col.insert([(self.start_time, 1.0)])

    def test_return_table(self) :
        self.setUp()
        self.create_ts()
        res = settings.cluster.query_exp("select * from " + self.entry_name + " in range(2018-01-01 , 2018-12-12)")
        self.assertEqual(len(res.tables), 1)

if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' , 'build' , 'test' , 'test-reports')
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()
