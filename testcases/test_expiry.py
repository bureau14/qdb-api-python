# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import os
import sys
import unittest
import settings

sys.path.append(os.path.join(os.path.split(__file__)[0], '..', 'bin', 'Release'))
sys.path.append(os.path.join(os.path.split(__file__)[0], '..', 'bin', 'release'))
import quasardb  # pylint: disable=C0413,E0401


def _make_expiry_time(td):
    # expires in one minute
    now = datetime.datetime.now()
    # get rid of the microsecond for the testcases
    return now + td - datetime.timedelta(microseconds=now.microsecond)


class QuasardbExpiry(unittest.TestCase):
    def test_expires_at(self):
        """
        Test for expiry.
        We want to make sure, in particular, that the conversion from Python datetime is right.
        """

        # add one entry
        entry_name = settings.entry_gen.next()
        entry_content = "content"

        b = settings.cluster.blob(entry_name)

        # entry does not exist yet
        self.assertRaises(quasardb.Error, b.get_expiry_time)

        b.put(entry_content)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertEqual(exp.year, 1970)

        future_exp = _make_expiry_time(datetime.timedelta(minutes=1))
        b.expires_at(future_exp)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertEqual(exp, future_exp)

    def test_expires_from_now(self):
        # add one entry
        entry_name = settings.entry_gen.next()
        entry_content = "content"

        b = settings.cluster.blob(entry_name)
        b.put(entry_content)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertEqual(exp.year, 1970)

        # expires in one minute from now
        b.expires_from_now(datetime.timedelta(minutes=1))

        # We use a wide 10s interval for the check, because we have no idea at which speed
        # these testcases may run in debug. This will be enough however to check that
        # the interval has properly been converted and the time zone is
        # correct.
        future_exp_lower_bound = datetime.datetime.now() + datetime.timedelta(seconds=50)
        future_exp_higher_bound = future_exp_lower_bound + \
            datetime.timedelta(seconds=80)

        exp = b.get_expiry_time()
        self.assertIsInstance(exp, datetime.datetime)
        self.assertLess(future_exp_lower_bound, exp)
        self.assertLess(exp, future_exp_higher_bound)

    def test_methods(self):
        """
        Test that methods that accept an expiry date properly forward the value
        """
        entry_name = settings.entry_gen.next()
        entry_content = "content"

        future_exp = _make_expiry_time(datetime.timedelta(minutes=1))

        b = settings.cluster.blob(entry_name)
        b.put(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertEqual(exp, future_exp)

        future_exp = _make_expiry_time(datetime.timedelta(minutes=2))

        b.update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertEqual(exp, future_exp)

        future_exp = _make_expiry_time(datetime.timedelta(minutes=3))

        b.get_and_update(entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertEqual(exp, future_exp)

        future_exp = _make_expiry_time(datetime.timedelta(minutes=4))

        b.compare_and_swap(entry_content, entry_content, future_exp)

        exp = b.get_expiry_time()

        self.assertIsInstance(exp, datetime.datetime)
        self.assertEqual(exp, future_exp)

        b.remove()

    def test_trim_all_at_end(self):
        try:
            settings.cluster.trim_all(datetime.timedelta(minutes=1))
        except:  # pylint: disable=W0702
            self.fail('settings.cluster.trim_all raised an unexpected exception')


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
