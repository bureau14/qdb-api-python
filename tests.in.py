import cPickle as pickle
import datetime
import os
import subprocess
import sys
import time
import unittest

for root, dirnames, filenames in os.walk('@CMAKE_BINARY_DIR@/build/'):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import qdb

# generate an unique entry name for the tests

class UniqueEntryNameGenerator(object):

    def __init__(self):
        self.__prefix  = "entry_"
        self.__counter = 0

    def __iter__(self):
        return self

    def next(self):
        self.__counter += 1
        return self.__prefix + str(self.__counter)

def setUpModule():

    global uri
    global cluster
    global __clusterd
    global entry_gen

    entry_gen = UniqueEntryNameGenerator()

    __current_port = 3000
    uri = ""

    # don't save anything to disk
    __clusterd = subprocess.Popen(['@QDB_DAEMON@', '--address=127.0.0.1:' + str(__current_port), '--transient'])
    if __clusterd.pid == 0:
        raise Exception("daemon", "cannot run daemon")

    # startup may take a couple of seconds, temporize to make sure the connection will be accepted
    time.sleep(2)

    __clusterd.poll()

    if __clusterd.returncode != None:
        raise Exception("daemon", "error while running daemon")

    uri = "qdb://127.0.0.1:" + str(__current_port)

    __current_port += 1

    try:
        cluster = qdb.Cluster(uri)

    except qdb.QuasardbException, q:
        __clusterd.terminate()
        __clusterd.wait()
        raise

    except BaseException, e:
        __clusterd.terminate()
        __clusterd.wait()
        raise

def tearDownModule():
    __clusterd.terminate()
    __clusterd.wait()

class QuasardbTest(unittest.TestCase):

    pass

class QuasardbBasic(QuasardbTest):
    """
    Basic operations tests such as put/get/remove
    """
    def test_info(self):
        str = qdb.build()
        self.assertGreater(len(str), 0)
        str = qdb.version()
        self.assertGreater(len(str), 0)

    def test_put_get_and_remove(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)

        self.assertRaises(qdb.QuasardbException, b.put, entry_content)

        got = b.get()
        self.assertEqual(got, entry_content)
        b.remove()
        self.assertRaises(qdb.QuasardbException, b.get)
        self.assertRaises(qdb.QuasardbException, b.remove)

    def test_update(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.update(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        entry_content = "it's a new style"
        b.update(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        b.remove()

    def test_get_and_update(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)

        entry_new_content = "new stuff"
        got = b.get_and_update(entry_new_content)
        self.assertEqual(got, entry_content)
        got = b.get()
        self.assertEqual(got, entry_new_content)

        b.remove()

    def test_get_and_remove(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        got = b.get_and_remove()
        self.assertEqual(got, entry_content)
        self.assertRaises(qdb.QuasardbException, b.get)

    def test_remove_if(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        self.assertRaises(qdb.QuasardbException, b.remove_if, entry_content + 'a')
        got = b.get()
        self.assertEqual(got, entry_content)
        b.remove_if(entry_content)
        self.assertRaises(qdb.QuasardbException, b.get)

    def test_compare_and_swap(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        b.put(entry_content)
        got = b.get()
        self.assertEqual(got, entry_content)
        entry_new_content = "new stuff"
        got = b.compare_and_swap(entry_new_content, entry_new_content)
        self.assertEqual(got, entry_content)
        # unchanged because unmatched
        got = b.get()
        self.assertEqual(got, entry_content)
        got = b.compare_and_swap(entry_new_content, entry_content)
        self.assertEqual(got, entry_content)
        # changed because matched
        got = b.get()
        self.assertEqual(got, entry_new_content)

        b.remove()

class QuasardbInteger(QuasardbTest):

    def test_put_get_and_remove(self):
        entry_name = entry_gen.next()
        entry_value = 0

        i = cluster.integer(entry_name)

        i.put(entry_value)

        self.assertRaises(qdb.QuasardbException, i.put, entry_value)

        got = i.get()
        self.assertEqual(got, entry_value)
        i.remove()
        self.assertRaises(qdb.QuasardbException, i.get)
        self.assertRaises(qdb.QuasardbException, i.remove)

    def test_update(self):
        entry_name = entry_gen.next()
        entry_value = 1

        i = cluster.integer(entry_name)

        i.update(entry_value)
        got = i.get()
        self.assertEqual(got, entry_value)
        entry_value = 2
        i.update(entry_value)
        got = i.get()
        self.assertEqual(got, entry_value)
        i.remove()

    def test_add(self):
        entry_name = entry_gen.next()
        entry_value = 0

        i = cluster.integer(entry_name)

        i.put(entry_value)

        got = i.get()
        self.assertEqual(got, entry_value)

        entry_increment = 10

        i.add(entry_increment)

        got = i.get()
        self.assertEqual(got, entry_value + entry_increment)

        entry_decrement = -100

        i.add(entry_decrement)

        got = i.get()
        self.assertEqual(got, entry_value + entry_increment + entry_decrement)

        i.remove()

class QuasardbDeque(QuasardbTest):
    
    def test_sequence(self):
        """
        A series of test to make sure back and front operations are properly wired
        """
        entry_name = entry_gen.next()
        entry_content_back = "back"

        q = cluster.deque(entry_name)

        self.assertRaises(qdb.QuasardbException, q.pop_back)
        self.assertRaises(qdb.QuasardbException, q.pop_front)
        self.assertRaises(qdb.QuasardbException, q.front)
        self.assertRaises(qdb.QuasardbException, q.back)

        q.push_back(entry_content_back)

        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_back)

        entry_content_front = "front"

        q.push_front(entry_content_front)

        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        entry_content_canary = "canary"

        q.push_back(entry_content_canary)

        got = q.back()
        self.assertEqual(got, entry_content_canary)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        got = q.pop_back()
        self.assertEqual(got, entry_content_canary)
        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        q.push_front(entry_content_canary)

        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_canary)

        got = q.pop_front()
        self.assertEqual(got, entry_content_canary)
        got = q.back()
        self.assertEqual(got, entry_content_back)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        got = q.pop_back()
        self.assertEqual(got, entry_content_back)

        got = q.back()
        self.assertEqual(got, entry_content_front)
        got = q.front()
        self.assertEqual(got, entry_content_front)

        got = q.pop_back()
        self.assertEqual(got, entry_content_front)

        self.assertRaises(qdb.QuasardbException, q.pop_back)
        self.assertRaises(qdb.QuasardbException, q.pop_front)
        self.assertRaises(qdb.QuasardbException, q.front)
        self.assertRaises(qdb.QuasardbException, q.back)

class QuasardbHSet(QuasardbTest):

    def test_insert_erase_contains(self):

        entry_name = entry_gen.next()
        entry_content = "content"

        hset = cluster.hset(entry_name)

        # does not exist yet
        self.assertRaises(qdb.QuasardbException, hset.contains, entry_content)

        hset.insert(entry_content)

        self.assertRaises(qdb.QuasardbException, hset.insert, entry_content)

        self.assertTrue(hset.contains(entry_content))

        hset.erase(entry_content)

        self.assertFalse(hset.contains(entry_content))

        hset.insert(entry_content)

        self.assertTrue(hset.contains(entry_content))

        hset.erase(entry_content)

        self.assertRaises(qdb.QuasardbException, hset.erase, entry_content)


class QuasardbInfo(QuasardbTest):
    """
    Tests the json information string query
    """
    def test_node_status(self):
        status = cluster.node_status(uri)
        self.assertGreater(len(status), 0)

    def test_node_config(self):
        config = cluster.node_config(uri)
        self.assertGreater(len(config), 0)

class QuasardbTag(QuasardbTest):
    """
    """

    def test_get_entries(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        tag_name = entry_gen.next()

        b = cluster.blob(entry_name)

        b.put(entry_content)

        tags = b.get_tags()
        self.assertEqual(0, len(tags))

        self.assertFalse(b.has_tag(tag_name))

        self.assertTrue(b.add_tag(tag_name))
        self.assertFalse(b.add_tag(tag_name))

        tags = b.get_tags()
        self.assertEqual(1, len(tags))
        self.assertEqual(tags[0], tag_name)

        self.assertTrue(b.has_tag(tag_name))

        self.assertTrue(b.remove_tag(tag_name))
        self.assertFalse(b.remove_tag(tag_name))       

        tags = b.get_tags()
        self.assertEqual(0, len(tags))

    def test_tag_sequence(self):
        entry_name = entry_gen.next()
        entry_content = "content"

        tag_name = entry_gen.next()

        b = cluster.blob(entry_name)
        b.put(entry_content)

        t = cluster.tag(tag_name)

        entries = t.get_entries()
        self.assertEqual(0, len(entries))

        self.assertTrue(b.add_tag(tag_name))      

        tags = t.get_entries()
        self.assertEqual(1, len(tags))
        self.assertEqual(tags[0], entry_name)

        self.assertTrue(b.remove_tag(tag_name))
        entries = t.get_entries()
        self.assertEqual(0, len(entries))


class QuasardbExpiry(QuasardbTest):

    def __make_expiry_time(self, td):
         # expires in one minute
        now = datetime.datetime.now(qdb.tz)
        # get rid of the microsecond for the tests
        return now + td - datetime.timedelta(microseconds=now.microsecond)

    def test_expires_at(self):
        """
        Test for expiry. We want to make sure, in particular, that the conversion from Python datetime is right.
        """
        # add one entry
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)

        # entry does not exist yet
        self.assertRaises(qdb.QuasardbException, b.get_expiry_time)

        b.put(entry_content)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, datetime.datetime.fromtimestamp(0, qdb.tz))

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=1))
        b.expires_at(future_exp)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

    def test_expires_from_now(self):
        # add one entry
        entry_name = entry_gen.next()
        entry_content = "content"

        b = cluster.blob(entry_name)
        b.put(entry_content)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, datetime.datetime.fromtimestamp(0, qdb.tz))

        b.expires_at(None)

        # expires in one minute from now
        future_exp = 60

        b.expires_from_now(future_exp)

        # We use a wide 10 se interval for the check because we have no idea at which speed these tests
        # may run in debug, this will be enough however to check that the interval has properly been converted
        # and the time zone is correct
        future_exp_lower_bound = datetime.datetime.now(qdb.tz) + datetime.timedelta(seconds=future_exp-10)
        future_exp_higher_bound = future_exp_lower_bound + datetime.timedelta(seconds=future_exp+10)

        exp = b.get_expiry_time()
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertIsInstance(exp, datetime.datetime)
        self.assertLess(future_exp_lower_bound, exp)
        self.assertLess(exp, future_exp_higher_bound)

    def test_methods(self):
        """
        Test that methods that accept an expiry date properly forward the value
        """
        entry_name = entry_gen.next()
        entry_content = "content"

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=1))

        b = cluster.blob(entry_name)
        b.put(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=2))

        b.update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=3))

        b.get_and_update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        future_exp = self.__make_expiry_time(datetime.timedelta(minutes=4))

        b.compare_and_swap(entry_content, entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertNotEqual(exp.tzinfo, None)
        self.assertEqual(exp.utcoffset(), datetime.timedelta(0))
        self.assertEqual(exp, future_exp)

        b.remove()

if __name__ == '__main__':
    import xmlrunner
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))()
