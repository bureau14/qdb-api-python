# pylint: disable=C0103,C0111,C0302,W0212
import os
import unittest
import settings


class QuasardbTag(unittest.TestCase):

    def test_get_entries(self):
        entry_name = settings.entry_gen.next()
        entry_content = "content"

        tag_name = settings.entry_gen.next()

        b = settings.cluster.blob(entry_name)

        b.put(entry_content)

        tags = b.get_tags()
        self.assertEqual(0, len(tags))

        self.assertFalse(b.has_tag(tag_name))

        self.assertTrue(b.attach_tag(tag_name))
        self.assertFalse(b.attach_tag(tag_name))

        tags = b.get_tags()
        self.assertEqual(1, len(tags))
        self.assertEqual(tags[0], tag_name)

        self.assertTrue(b.has_tag(tag_name))

        self.assertTrue(b.detach_tag(tag_name))
        self.assertFalse(b.detach_tag(tag_name))

        tags = b.get_tags()
        self.assertEqual(0, len(tags))

    def test_tag_sequence(self):
        entry_name = settings.entry_gen.next()
        entry_content = "content"

        tag_name = settings.entry_gen.next()

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)

        t = settings.cluster.tag(tag_name)

        entries = t.get_entries()
        self.assertEqual(0, len(entries))

        self.assertTrue(b.attach_tag(tag_name))

        tags = t.get_entries()
        self.assertEqual(1, len(tags))
        self.assertEqual(tags[0], entry_name)

        self.assertEqual(1, t.count())

        self.assertTrue(b.detach_tag(tag_name))
        entries = t.get_entries()
        self.assertEqual(0, len(entries))


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
