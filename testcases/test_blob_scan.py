# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
import os
import sys
import unittest
import settings


class QuasardbScan(unittest.TestCase):

    def test_blob_scan_nothing(self):
        res = settings.cluster.blob_scan("ScanWillNotFind", 1000)
        self.assertEqual(0, len(res))

    def test_blob_scan_match(self):
        entry_name = settings.entry_gen.next()
        entry_content = "ScanWillFind"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)

        res = settings.cluster.blob_scan("ScanWill", 1000)

        self.assertEqual(1, len(res))
        self.assertEqual(entry_name, res[0])

        b.remove()

    def test_blob_scan_match_many(self):
        entry_content = "ScanWillFind"

        blobs = []

        for _ in xrange(10):
            entry_name = settings.entry_gen.next()
            b = settings.cluster.blob(entry_name)
            b.put(entry_content)
            blobs.append(b)

        res = settings.cluster.blob_scan("ScanWill", 5)

        self.assertEqual(5, len(res))

        for b in blobs:
            b.remove()

    def test_blob_scan_regex_nothing(self):
        res = settings.cluster.blob_scan_regex("ScanRegexW.ll.*", 1000)
        self.assertEqual(0, len(res))

    def test_blob_scan_regex_match(self):
        entry_name = settings.entry_gen.next()
        entry_content = "ScanRegexWillFind"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)

        res = settings.cluster.blob_scan_regex("ScanRegexW.ll.*", 1000)

        self.assertEqual(1, len(res))
        self.assertEqual(entry_name, res[0])

        b.remove()

    def test_blob_scan_regex_match_many(self):
        entry_content = "ScanRegexWillFind"

        blobs = []

        for _ in xrange(10):
            entry_name = settings.entry_gen.next()
            b = settings.cluster.blob(entry_name)
            b.put(entry_content)
            blobs.append(b)

        res = settings.cluster.blob_scan_regex("ScanRegexW.ll.*", 5)

        self.assertEqual(5, len(res))

        for b in blobs:
            b.remove()


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
