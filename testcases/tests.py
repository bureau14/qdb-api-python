# pylint: disable=C0103,C0111,C0302,W0212
import os
import unittest
import sys
import settings


class QuasardbTest(unittest.TestCase):
    pass


if __name__ == '__main__':
    settings.init()
    test_directory = os.getcwd()
    test_report_directory = os.path.join(os.path.split(
        __file__)[0], '..', 'build', 'test', 'test-reports')

    loader = unittest.TestLoader()
    test_suite = loader.discover(test_directory, 'test_*.py')
    import xmlrunner
    xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output=test_report_directory).run(test_suite)
    settings.terminate()
