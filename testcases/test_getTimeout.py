# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import os
import sys
import unittest
import settings


class QuasardbClusterGetTimeout(unittest.TestCase):
    def test_get_timeout(self):
        try:
            settings.cluster.set_timeout(datetime.timedelta(
                seconds=1))  # First setting the timeout
            duration = settings.cluster.get_timeout()
            # Checking, with the set timoeout argument
            self.assertEqual(duration, 1000)
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.get_timeout should not have raised an exception')


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
