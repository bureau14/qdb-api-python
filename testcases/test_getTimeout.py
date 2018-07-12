# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import os
import sys
import unittest
import settings

class QuasardbClusterGetTimeout(unittest.TestCase):
    def test_get_timeout(self):
        settings.cluster.options().set_timeout(datetime.timedelta(
            seconds=1))  # First setting the timeout
        duration = settings.cluster.options().get_timeout()
        # Checking, with the set timeout argument
        self.assertEqual(duration, datetime.timedelta(seconds=1))

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
