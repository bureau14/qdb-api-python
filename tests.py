import unittest
import qdb
import cPickle as pickle
import subprocess
import os


class QuasardbTest(unittest.TestCase):
    """
    Base class for all test, attempts to connect on a quasardb cluster listening on the default port on the IPV4 localhost
    all tests depend on it!
    """
    def setUp(self):
        # we use the debug version for our unit tests because in the debug version the licensing verification is disabled

        qdbdd_path = os.path.join(os.getcwd(), '..', '..', '..', 'bin64', 'Debug', 'qdbdd')
        if not os.path.exists(qdbdd_path):
            qdbdd_path = os.path.join(os.getcwd(), '..', '..', '..', 'bin', 'Debug', 'qdbdd')

        self.assertTrue(os.path.exists(qdbdd_path))

        # don't save anything to disk
        self.qdbd = subprocess.Popen([qdbdd_path, '--transient'])
        self.assertTrue(self.qdbd.pid != 0)
        self.assertTrue(self.qdbd.returncode == None)

        self.remote_node = qdb.RemoteNode("127.0.0.1")
        self.qdb = qdb.Client(self.remote_node)

    def tearDown(self):
        self.qdbd.terminate()
        self.qdbd.wait()

class QuasardbBasic(QuasardbTest):
    """
    Basic operations tests such as put/get/remove
    """
    def test_info(self):
        str = qdb.build()
        self.assertTrue(len(str) > 0)
        str = qdb.version()
        self.assertTrue(len(str) > 0)

    def test_put_get_remove(self):
        self.qdb.remove_all()

        entry_name = "entry"
        entry_content = "content"
        self.qdb.put(entry_name, entry_content)
        self.assertRaises(qdb.QuasardbException, self.qdb.put, entry_name, entry_content)
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        self.qdb.remove(entry_name)
        self.assertRaises(qdb.QuasardbException, self.qdb.get, entry_name)
        self.assertRaises(qdb.QuasardbException, self.qdb.remove, entry_name)

    def test_update(self):
        self.qdb.remove_all()

        entry_name = "entry"
        entry_content = "content"
        self.qdb.update(entry_name, entry_content)
        self.qdb.update(entry_name, entry_content)
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        entry_content = "it's a new style"
        self.qdb.update(entry_name, entry_content)
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        self.qdb.remove(entry_name)

    def test_get_update(self):
        self.qdb.remove_all()

        entry_name = "entry"
        entry_content = "content"
        self.qdb.put(entry_name, entry_content)
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        entry_new_content = "new stuff"
        got = self.qdb.get_update(entry_name, entry_new_content)
        self.assertEqual(got, entry_content)
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_new_content)

        self.qdb.remove(entry_name)

    def test_get_remove(self):
        self.qdb.remove_all()

        entry_name = "entry"
        entry_content = "content"
        self.qdb.put(entry_name, entry_content)
        got = self.qdb.get_remove(entry_name)
        self.assertEqual(got, entry_content)
        self.assertRaises(qdb.QuasardbException, self.qdb.get, entry_name)

    def test_remove_if(self):
        self.qdb.remove_all()

        entry_name = "entry"
        entry_content = "content"
        self.qdb.put(entry_name, entry_content)
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        self.assertRaises(qdb.QuasardbException, self.qdb.remove_if, entry_name, entry_content + 'a')
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        self.qdb.remove_if(entry_name, entry_content)
        self.assertRaises(qdb.QuasardbException, self.qdb.get, entry_name)

    def test_compare_and_swap(self):
        self.qdb.remove_all()

        entry_name = "entry"
        entry_content = "content"
        self.qdb.put(entry_name, entry_content)
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        entry_new_content = "new stuff"
        got = self.qdb.compare_and_swap(entry_name, entry_new_content, entry_new_content)
        self.assertEqual(got, entry_content)
        # unchanged because unmatched
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_content)
        got = self.qdb.compare_and_swap(entry_name, entry_new_content, entry_content)
        self.assertEqual(got, entry_content)
        # changed because matched
        got = self.qdb.get(entry_name)
        self.assertEqual(got, entry_new_content)

        self.qdb.remove(entry_name)

class QuasardbInfo(QuasardbTest):
    """
    Tests the json information string query
    """
    def test_node_status(self):
        status = self.qdb.node_status(self.remote_node)
        self.assertTrue(len(status) > 0)

    def test_node_config(self):
        config = self.qdb.node_config(self.remote_node)
        self.assertTrue(len(config) > 0)

class QuasardbIteration(QuasardbTest):
    """
    Test forward iteration. The purpose isn't to tests the bulk of the function that is thoroughly tested in C++ unit and integration tests
    but to make sure that the Python wrapping is working as expected
    """
    def test_forward(self):
        self.qdb.remove_all()

        # add 10 entries 
        entries = dict()

        for e in range(0, 10):
            k = 'entry' + str(e)
            v = 'content' + str(e)
            entries[k] = v
            self.qdb.put(k, v)

        # now iterate and check values
        for k, v in self.qdb:
            self.assertEqual(entries[k], v)

        self.qdb.remove_all()


if __name__ == '__main__':
    unittest.main()
