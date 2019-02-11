# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import quasardb


def _make_expiry_time(td):
    # expires in one minute
    now = datetime.datetime.now()
    # get rid of the microsecond for the testcases
    return now + td - datetime.timedelta(microseconds=now.microsecond)


def test_expires_at(blob_entry, random_blob):
    """
    Test for expiry.
    We want to make sure, in particular, that the conversion from Python datetime is right.
    """

    # entry does not exist yet
    with pytest.raises(quasardb.Error):
        blob_entry.get_expiry_time()

    blob_entry.put(random_blob)

    exp = blob_entry.get_expiry_time()
    assert isinstance(exp, datetime.datetime)
    assert exp.year == 1970

    future_exp = _make_expiry_time(datetime.timedelta(minutes=1))
    blob_entry.expires_at(future_exp)

    exp = blob_entry.get_expiry_time()
    assert isinstance(exp, datetime.datetime)
    assert exp == future_exp


def test_expires_from_now(blob_entry, random_blob):
    # entry does not exist yet
    with pytest.raises(quasardb.Error):
        blob_entry.get_expiry_time()

    blob_entry.put(random_blob)

    exp = blob_entry.get_expiry_time()
    assert isinstance(exp, datetime.datetime)
    assert exp.year == 1970

    # expires in one minute from now
    blob_entry.expires_from_now(datetime.timedelta(minutes=1))

    # We use a wide 10s interval for the check, because we have no idea at which speed
    # these testcases may run in debug. This will be enough however to check that
    # the interval has properly been converted and the time zone is
    # correct.
    future_exp_lower_bound = datetime.datetime.now() + datetime.timedelta(seconds=50)
    future_exp_higher_bound = future_exp_lower_bound + \
        datetime.timedelta(seconds=80)

    exp = blob_entry.get_expiry_time()
    assert isinstance(exp, datetime.datetime)
    assert future_exp_lower_bound < exp
    assert future_exp_higher_bound > exp


def test_methods(blob_entry, random_blob):

    future_exp = _make_expiry_time(datetime.timedelta(minutes=1))

    blob_entry.put(random_blob, future_exp)

    exp = blob_entry.get_expiry_time()

    assert isinstance(exp, datetime.datetime)
    assert exp == future_exp

    future_exp = _make_expiry_time(datetime.timedelta(minutes=2))

    blob_entry.update(random_blob, future_exp)

    exp = blob_entry.get_expiry_time()

    assert isinstance(exp, datetime.datetime)
    assert exp == future_exp

    future_exp = _make_expiry_time(datetime.timedelta(minutes=3))

    blob_entry.get_and_update(random_blob, future_exp)

    exp = blob_entry.get_expiry_time()

    assert isinstance(exp, datetime.datetime)
    assert exp == future_exp

    future_exp = _make_expiry_time(datetime.timedelta(minutes=4))

    blob_entry.compare_and_swap(random_blob, random_blob, future_exp)

    exp = blob_entry.get_expiry_time()

    assert isinstance(exp, datetime.datetime)
    assert exp == future_exp
