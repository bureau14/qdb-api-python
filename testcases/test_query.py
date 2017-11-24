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

for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings

class QuasardbQuery(unittest.TestCase):
    

    def test_types(self):

        my_tag = "my_tag" + settings.entry_gen.next()

        entry_name =settings.entry_gen.next()
        entry_content = "content"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)
        b.attach_tag(my_tag)

        res = settings.cluster.query("tag='" + my_tag + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.query("tag='" + my_tag + "' AND type=blob")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.query("tag='" + my_tag + "' AND type=integer")

        self.assertEqual(len(res), 0)

    def test_two_tags(self):

        tag1 = "tag1" + settings.entry_gen.next()
        tag2 = "tag2" + settings.entry_gen.next()

        entry_name = settings.entry_gen.next()
        entry_content = "content"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)
        b.attach_tag(tag1)
        b.attach_tag(tag2)

        res = settings.cluster.query("tag='" + tag1 + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.query("tag='" + tag2 + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.query("tag='" + tag1 + "' AND tag='" + tag2 + "'")

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()