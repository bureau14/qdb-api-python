# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import quasardb


def test_expires_at(blob_entry, random_blob, datetime_, timedelta):
    """
    Test for expiry.
    We want to make sure, in particular, that the conversion from Python datetime is right.
    """

    # entry does not exist yet
    with pytest.raises(quasardb.AliasNotFoundError):
        blob_entry.get_expiry_time()

    blob_entry.put(random_blob)

    exp = blob_entry.get_expiry_time()
    assert isinstance(exp, datetime.datetime)
    assert exp.year == 1970

    future_exp = datetime_ + timedelta
    blob_entry.expires_at(future_exp)

    output_exp = blob_entry.get_expiry_time()
    assert isinstance(output_exp, datetime.datetime)
    assert isinstance(output_exp.tzinfo, datetime.timezone)

    check_exp = future_exp
    if check_exp.tzinfo is None:
        # Per conftest.datetime(), the only "bare" datetime we create is one
        # of `datetime.datetime.now()`, which is local time. As such, as
        # QuasarDB always emits UTC datetimes, we should cast our input time
        # to UTC as well before comparing.
        check_exp = check_exp.astimezone(datetime.timezone.utc)

    assert check_exp == output_exp


def test_expires_from_now(blob_entry, random_blob, timedelta):
    # entry does not exist yet
    with pytest.raises(quasardb.AliasNotFoundError):
        blob_entry.get_expiry_time()

    blob_entry.put(random_blob)

    exp = blob_entry.get_expiry_time()
    assert isinstance(exp, datetime.datetime)
    assert exp.year == 1970

    # expires in one minute from now
    blob_entry.expires_from_now(timedelta)

    # We use a wide 10s interval for the check, because we have no idea at which speed
    # these testcases may run in debug. This will be enough however to check that
    # the interval has properly been converted and the time zone is
    # correct.
    future_exp_lower_bound = datetime.datetime.now(tz=datetime.timezone.utc) + timedelta - datetime.timedelta(seconds=30)
    future_exp_higher_bound = future_exp_lower_bound + timedelta + datetime.timedelta(seconds=30)

    exp = blob_entry.get_expiry_time()
    assert isinstance(exp, datetime.datetime)
    assert future_exp_lower_bound < exp
    assert future_exp_higher_bound > exp
