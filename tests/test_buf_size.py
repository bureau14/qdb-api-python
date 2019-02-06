# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import os
import sys
import unittest
import settings

from settings import quasardb

class QuasardbOptionBufSize(unittest.TestCase):

    def test_set_client_max_in_buf_1(self):
         self.assertRaises(quasardb.Error,
                          settings.cluster.options().set_client_max_in_buf_size, 1)

    def test_set_client_max_in_buf_1MB(self):
        try:
            settings.cluster.options().set_client_max_in_buf_size(1024 * 1024)
            self.assertEqual(1024 * 1024, settings.cluster.options().get_client_max_in_buf_size())
        except:  # pylint: disable=W0702
            self.fail(
                msg='set/get client max buf should not have raised an exception')

    def test_get_client_max_in_buf(self):
        try:
            self.assertTrue(0 < settings.cluster.options().get_client_max_in_buf_size())
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.option().get_client_max_in_buf_size should not have raised an exception')

    def test_get_cluster_max_in_buf(self):
        try:
            self.assertTrue(0 < settings.cluster.options().get_cluster_max_in_buf_size())
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.option().get_cluster_max_in_buf_size should not have raised an exception')



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
