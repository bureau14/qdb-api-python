# pylint: disable=C0103,C0111,C0302,W0212
import os
import sys
import unittest
import settings

from settings import quasardb

class QuasardbClusterSetMaxCardinality(unittest.TestCase):
    def test_max_cardinality_ok(self):
        try:
            settings.cluster.options().set_max_cardinality(140000)
        except:  # pylint: disable=W0702
            self.fail(
                msg='cluster.set_max_cardinality should not have raised an exception')

    def test_max_cardinality_throws_when_value_is_zero(self):
        self.assertRaises(quasardb.Error,
                          settings.cluster.options().set_max_cardinality, 0)

    def test_max_cardinality_throws_when_value_is_negative(self):
        self.assertRaises(TypeError,
                          settings.cluster.options().set_max_cardinality, -143)


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
