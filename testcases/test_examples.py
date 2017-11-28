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
import re
import random

for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings

sys.path.append(os.path.join(os.path.split(__file__)[0], '..', 'examples'))

class QuasardbExamples(unittest.TestCase) :

    def test_time_series_example(self):
        import time_series
        time_series.main(settings.INSECURE_URI, settings.entry_gen.next())
    def test_insert_example(self):
        import insert
        insert.main(settings.INSECURE_URI, 100)
    def test_csv_insert_example(self):
        HAS_PANDAS = True
        try:
            import pandas
        except ImportError:
            HAS_PANDAS = False
        if HAS_PANDAS :
            import csv_insert
            csv_insert.main(settings.INSECURE_URI, settings.entry_gen.next(), os.path.join(os.path.split(__file__)[0], '..', 'examples/' "fake_currency_minutes.csv"))
        else :
            print "\nCannot run the example csv_insert as the module pandas is not found.\n"
    def test_temperature_example(self):
        import temperature
        temperature.main(settings.INSECURE_URI, settings.entry_gen.next(), 100)



if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()