# pylint: disable=C0103,C0111,C0302,W0212
import os
import unittest
import re


if __name__ == '__main__':
    test_directory = os.getcwd()
    test_report_directory = re.search(".*qdb-api-python", os.getcwd()).group() + "\\build\\test-reports\\"

    loader = unittest.TestLoader()
    test_suite = loader.discover(test_directory,'test_*')
    import xmlrunner
    xmlrunner.XMLTestRunner(  # pylint: disable=E1102
        output= test_report_directory + 'test-reports').run(test_suite)
