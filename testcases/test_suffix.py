# pylint: disable=C0103,C0111,C0302,W0212
import os
import unittest
import settings


class QuasardbSuffix(unittest.TestCase):
    def test_empty_suffix(self):

        res = settings.cluster.suffix_get("testazeazeaze", 10)
        self.assertEqual(len(res), 0)

        self.assertEqual(0, settings.cluster.suffix_count("testazeazeaze"))

    def test_find_one(self):
        dat_suffix = "my_dark_suffix"

        entry_name = settings.entry_gen.next() + dat_suffix
        entry_content = "content"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)

        res = settings.cluster.suffix_get(dat_suffix, 10)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        self.assertEqual(1, settings.cluster.suffix_count(dat_suffix))


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
