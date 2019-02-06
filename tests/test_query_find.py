# pylint: disable=C0103,C0111,C0302,W0212
import os
import unittest
import settings

class QuasardbQueryFind(unittest.TestCase):

    def test_types(self):

        my_tag = "my_tag" + settings.entry_gen.next()

        entry_name = settings.entry_gen.next()
        entry_content = "content"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)
        b.attach_tag(my_tag)

        res = settings.cluster.find("find(tag='" + my_tag + "')").run()

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.find(
            "find(tag='" + my_tag + "' AND type=blob)").run()

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.find(
            "find(tag='" + my_tag + "' AND type=integer)").run()

        self.assertEqual(len(res), 0)

    def test_two_tags(self):

        tag1 = "tag1" + settings.entry_gen.next()
        tag2 = "tag2" + settings.entry_gen.next()

        entry_name = settings.entry_gen.next()
        entry_content = "content"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)
        b.attach_tag(tag1)
        b.attach_tag(tag2)

        res = settings.cluster.find("find(tag='" + tag1 + "')").run()

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.find("find(tag='" + tag2 + "')").run()

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)

        res = settings.cluster.find(
            "find(tag='" + tag1 + "' AND tag='" + tag2 + "')").run()

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], entry_name)


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