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

HAS_NUMPY = True
try:
    import numpy
except ImportError:
    HAS_NUMPY = False

for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401
import settings


class QuasardbConnection(unittest.TestCase):

    def test_connect_throws_input_error__when_uri_is_invalid(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri='invalid_uri')

    def test_connect_throws_connection_error__when_no_cluster_on_given_uri(self):
        self.assertRaises(quasardb.ConnectionError,
                          quasardb.Cluster, uri='qdb://127.0.0.1:1')

    def test_connect_throws_connection_error__when_no_cluster_public_key(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri=settings.SECURE_URI,
                          user_name=settings.SECURE_USER_NAME,
                          user_private_key=settings.SECURE_USER_PRIVATE_KEY)

    def test_connect_throws_connection_error__when_no_user_private_key(self):
        self.assertRaises(quasardb.InputError,
                          quasardb.Cluster, uri=settings.SECURE_URI,
                          user_name=settings.SECURE_USER_NAME,
                          cluster_public_key=settings.SECURE_CLUSTER_PUBLIC_KEY)

    def test_connect_throws_connection_error__when_no_user_name(self):
        self.assertRaises(quasardb.RemoteSystemError,
                          quasardb.Cluster, uri=settings.SECURE_URI,
                          user_private_key=settings.SECURE_USER_PRIVATE_KEY,
                          cluster_public_key=settings.SECURE_CLUSTER_PUBLIC_KEY)

    def test_connect_ok__secure_cluster(self):
        try:
            quasardb.Cluster(uri=settings.SECURE_URI,
                             user_name=settings.SECURE_USER_NAME,
                             user_private_key=settings.SECURE_USER_PRIVATE_KEY,
                             cluster_public_key=settings.SECURE_CLUSTER_PUBLIC_KEY)
        except Exception as ex:  # pylint: disable=W0703
            self.fail(msg='Cannot connect to secure settings.cluster: ' + str(ex))

if __name__ == '__main__':
    if settings.get_lock_status() == False :
        settings.init()
        test_directory = os.getcwd()
        test_report_directory = os.path.join(os.path.split(__file__)[0], '..' + "/build/test/test-reports/")
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory),exit=False)
        settings.terminate()