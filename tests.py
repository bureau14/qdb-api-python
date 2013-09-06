import unittest
import qdb
import cPickle as pickle
import subprocess
import os
import datetime

class QuasardbTest(unittest.TestCase):
    """
    Base class for all test, attempts to connect on a quasardb cluster listening on the default port on the IPV4 localhost
    all tests depend on it!
    """
    def setUp(self):
        # we use the debug version for our unit tests because in the debug version the licensing verification is disabled

        possible_paths = [ os.path.join(os.getcwd(), '..', '..', '..', 'bin64', 'Debug', 'qdbdd'),
            os.path.join(os.getcwd(), '..', '..', '..', 'bin64', 'Debug', 'qdbdd.exe'),
            os.path.join(os.getcwd(), '..', '..', '..', 'bin', 'Debug', 'qdbdd'),
            os.path.join(os.getcwd(), '..', '..', '..', 'bin', 'Debug', 'qdbdd.exe') ]

        for p in possible_paths:
            if os.path.exists(p):
                qdbdd_path = p
                break

        self.assertTrue(os.path.exists(qdbdd_path))

        # check license presence
        license_file = os.path.join('..', '..', '..', 'scripts', 'tests', 'qdb_test_license.txt')
        self.assertTrue(os.path.exists(license_file))

        # don't save anything to disk
        self.qdbd = subprocess.Popen([qdbdd_path, '--transient', '--license-file=' + license_file])
        self.assertNotEqual(self.qdbd.pid, 0)
        self.assertEqual(self.qdbd.returncode, None)

        self.remote_node = qdb.RemoteNode("127.0.0.1")
        self.qdb = qdb.Client(self.remote_node)

        # remove_all is asynchronous, don't call it after init, it might be finished in the middle of our test...

    def tearDown(self):
        self.qdb.remove_all()
        self.qdbd.terminate()
        self.qdbd.wait()

class QuasardbBasic(QuasardbTest):
    """
    Basic operations tests such as put/get/remove
    """
    def test_info(self):
        str = qdb.build()
        self.assertGreater(len(str), 0)
        str = qdb.version()
        self.assertGreater(len(str), 0)

    def test_put_get_remove(self):
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
        entry_name = "entry"
        entry_content = "content"
        self.qdb.put(entry_name, entry_content)
        got = self.qdb.get_remove(entry_name)
        self.assertEqual(got, entry_content)
        self.assertRaises(qdb.QuasardbException, self.qdb.get, entry_name)

    def test_remove_if(self):
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
        self.assertGreater(len(status), 0)

    def test_node_config(self):
        config = self.qdb.node_config(self.remote_node)
        self.assertGreater(len(config), 0)

class QuasardbIteration(QuasardbTest):
    """
    Test forward iteration. The purpose isn't to tests the bulk of the function that is thoroughly tested in C++ unit and integration tests
    but to make sure that the Python wrapping is working as expected
    """
    def test_forward(self):

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

class QuasardbPrefix(QuasardbTest):

    def test_prefix_get(self):
        # prefix too short, must raise an exception
        self.assertRaises(qdb.QuasardbException, self.qdb.prefix_get, "a")

        res = self.qdb.prefix_get("blah")
        self.assertEqual(len(res), 0)

        entries = [ "blah", "maybe", "Romulan", "Rome", "Rosa", "Romanus" ]
        entry_content = "content"

        for e in entries:
            self.qdb.put(e, entry_content)

        res = self.qdb.prefix_get("blah")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], "blah")

        res = self.qdb.prefix_get("rom")
        self.assertEqual(len(res), 0)

        res = self.qdb.prefix_get("Rom")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0], "Romanus")
        self.assertEqual(res[1], "Rome")
        self.assertEqual(res[2], "Romulan")

class QuasardbExpiry(QuasardbTest):

    def __make_expiry_time(self, td):
         # expires in one minute
        now = datetime.datetime.now(qdb.tz)
        # get rid of the microsecond for the tests
        return now + td - datetime.timedelta(microseconds=now.microsecond)        

    """
    Test for expiry. We want to make sure, in particular, that the conversion from Python datetime is right.
    """
    def test_expires_at(self):
        # add one entry
        entry_name = "entry_expires_at"
        entry_content = "content"

        # entry does not exist yet
        self.assertRaises(qdb.QuasardbException, self.qdb.get_expiry_time, entry_name)

        self.qdb.put(entry_name, entry_content)

        exp = self.qdb.get_expiry_time(entry_name)
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, datetime.datetime.fromtimestamp(0, qdb.tz))

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=1))
        self.qdb.expires_at(entry_name, future_exp)

        exp = self.qdb.get_expiry_time(entry_name)
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        self.qdb.remove_all()

    def test_expires_from_now(self):
        # add one entry
        entry_name = "entry_expires_from_now"
        entry_content = "content"
        self.qdb.put(entry_name, entry_content)

        exp = self.qdb.get_expiry_time(entry_name)
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, datetime.datetime.fromtimestamp(0, qdb.tz))

        self.qdb.expires_at(entry_name, None)

        # expires in one minute from now
        future_exp = 60
        
        self.qdb.expires_from_now(entry_name, future_exp)

        # We use a wide 10 se interval for the check because we have no idea at which speed these tests
        # may run in debug, this will be enough however to check that the interval has properly been converted
        # and the time zone is correct
        future_exp_lower_bound = datetime.datetime.now(qdb.tz) + datetime.timedelta(seconds=future_exp-10)
        future_exp_higher_bound = future_exp_lower_bound + datetime.timedelta(seconds=future_exp+10)

        exp = self.qdb.get_expiry_time(entry_name)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertIsInstance(exp, datetime.datetime)
        self.assertLess(future_exp_lower_bound, exp)
        self.assertLess(exp, future_exp_higher_bound)

        self.qdb.remove_all()

    """
    Test that methods that accept an expiry date properly forward the value
    """
    def test_methods(self):
        entry_name = "entry"
        entry_content = "content"

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=1))

        self.qdb.put(entry_name, entry_content, future_exp)

        exp = self.qdb.get_expiry_time(entry_name)

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=2))

        self.qdb.update(entry_name, entry_content, future_exp)

        exp = self.qdb.get_expiry_time(entry_name)

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=3))

        self.qdb.get_update(entry_name, entry_content, future_exp)

        exp = self.qdb.get_expiry_time(entry_name)

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=4))

        self.qdb.compare_and_swap(entry_name, entry_content, entry_content, future_exp)

        exp = self.qdb.get_expiry_time(entry_name)

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        self.qdb.remove(entry_name)

if __name__ == '__main__':
    unittest.main()
