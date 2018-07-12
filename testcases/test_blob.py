# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange  # pylint: disable=W0622
import datetime
import os
import sys
import unittest
import settings

from settings import quasardb

class QuasardbBlob(unittest.TestCase):

    def setUp(self):
        self.entry_name = settings.entry_gen.next()
        self.entry_content = b"content"

        self.b = settings.cluster.blob(self.entry_name)

    def test_trim_all_at_begin(self):
        try:
            settings.cluster.trim_all(datetime.timedelta(minutes=1))
        except quasardb.Error:
            self.fail('settings.cluster.trim_all raised an unexpected exception')

    def test_put(self):
        self.b.put(self.entry_content)

    def test_put_throws_exception__when_called_twice(self):
        self.b.put(self.entry_content)

        self.assertRaises(quasardb.Error,
                          self.b.put, self.entry_content)

    def test_get(self):
        self.b.put(self.entry_content)

        got = self.b.get()

        self.assertEqual(self.entry_content, got)

    def test_remove(self):
        self.b.put(self.entry_content)

        self.b.remove()

        self.assertRaises(quasardb.Error, self.b.get)

    def test_put_after_remove(self):
        self.b.put(self.entry_content)

        self.b.remove()

        try:
            self.b.put(self.entry_content)
        except:  # pylint: disable=W0702
            self.fail(msg='blob.put should not have raised an exception')

    def test_remove_throws_exception__when_called_twice(self):
        self.b.put(self.entry_content)
        self.b.remove()

        self.assertRaises(quasardb.Error, self.b.remove)

    def test_update(self):
        self.b.update(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        new_entry_content = b"It's the new style"
        self.b.update(new_entry_content)
        got = self.b.get()
        self.assertEqual(new_entry_content, got)
        self.b.remove()

        new_entry_content = b"I'm never in training, my voice is not straining"

        self.b.update(new_entry_content)
        got = self.b.get()
        self.assertEqual(new_entry_content, got)

    def test_get_and_update(self):
        self.b.put(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)

        entry_new_content = b"new stuff"
        got = self.b.get_and_update(entry_new_content)
        self.assertEqual(self.entry_content, got)
        got = self.b.get()
        self.assertEqual(entry_new_content, got)

        self.b.remove()

    def test_get_and_remove(self):
        self.b.put(self.entry_content)

        got = self.b.get_and_remove()
        self.assertEqual(self.entry_content, got)
        self.assertRaises(quasardb.Error, self.b.get)

    def test_remove_if(self):
        self.b.put(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        self.assertRaises(quasardb.Error,
                          self.b.remove_if, self.entry_content + b'a')
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        self.b.remove_if(self.entry_content)
        self.assertRaises(quasardb.Error, self.b.get)

    def test_compare_and_swap(self):
        self.b.put(self.entry_content)
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        entry_new_content = b"new stuff"
        got = self.b.compare_and_swap(entry_new_content, entry_new_content)
        self.assertEqual(self.entry_content, got)
        # unchanged because unmatched
        got = self.b.get()
        self.assertEqual(self.entry_content, got)
        got = self.b.compare_and_swap(entry_new_content, self.entry_content)
        self.assertEqual(0, len(got))
        # changed because matched
        got = self.b.get()
        self.assertEqual(entry_new_content, got)

        self.b.remove()


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
