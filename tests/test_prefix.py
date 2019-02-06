# pylint: disable=C0103,C0111,C0302,W0212
import os
import unittest
import settings

class QuasardbPrefix(unittest.TestCase):
    def test_empty_prefix(self):

        res = settings.cluster.prefix_get("testazeazeaze", 10)
        self.assertEqual(len(res), 0)

        self.assertEqual(0, settings.cluster.prefix_count("testazeazeaze"))

    def test_find_one(self):
        dat_prefix = "my_dark_prefix"
        entry_name = dat_prefix + settings.entry_gen.next()
        entry_content = "content"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)

        res = settings.cluster.prefix_get(dat_prefix, 10)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        self.assertEqual(1, settings.cluster.prefix_count(dat_prefix))


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
