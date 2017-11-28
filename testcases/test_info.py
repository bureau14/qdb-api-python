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

class QuasardbInfo(unittest.TestCase):
    def test_node_status(self):
        status = settings.cluster.node_status(settings.INSECURE_URI)
        self.assertGreater(len(status), 0)
        self.assertIsNotNone(status.get('overall'))

    def test_node_config(self):
        config = settings.cluster.node_config(settings.INSECURE_URI)
        self.assertGreater(len(config), 0)
        self.assertIsNotNone(config.get('global'))
        self.assertIsNotNone(config.get('local'))

    def test_node_topology(self):
        topology = settings.cluster.node_topology(settings.INSECURE_URI)
        self.assertGreater(len(topology), 0)
        self.assertIsNotNone(topology.get('predecessor'))
        self.assertIsNotNone(topology.get('center'))
        self.assertIsNotNone(topology.get('successor'))

if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()
