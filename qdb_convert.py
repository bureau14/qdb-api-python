# pylint: disable=C0103,C0111,C0302,R0903

# Copyright (c) 2009-2017, quasardb SAS
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of quasardb nor the names of its contributors may
#      be used to endorse or promote products derived from this software
#      without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

from builtins import range as xrange, int as long  # pylint: disable=W0622
import quasardb.impl as impl  # pylint: disable=C0413,E0401
import time
import datetime
import calendar
import pytz
import pytz

from tzlocal import get_localzone

DoublePointsVector = impl.DoublePointVec
BlobPointsVector = impl.BlobPointVec
Int64PointsVector = impl.Int64PointVec
TimestampPointsVector = impl.TimestampPointVec

def duration_to_timeout_ms(duration):
    seconds_in_day = long(24) * long(60) * long(60)
    return ((duration.days * seconds_in_day + duration.seconds) * long(1000)
            + (duration.microseconds // long(1000)))

def api_buffer_to_string(buf):
    return None if buf is None else impl.api_buffer_ptr_as_string(buf)

def string_to_api_buffer(h, s):
    """
    Converts a string to an internal qdb::api_buffer_ptr
    """
    return None if s is None else impl.make_api_buffer_ptr_from_string(h, s)

tz = pytz.UTC

try:
    local_tz = get_localzone()
except:
    local_tz = tz

_epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC)

# don't use timetuple because of tz
def time_to_unix_timestamp(t):
    if t.tzinfo:
        return (t - _epoch).total_seconds()
    else:
        return long(time.mktime((t.year, t.month, t.day, t.hour, t.minute, t.second, -1, -1, -1))) + long(local_tz.utcoffset(t).total_seconds())

def time_to_qdb_timestamp(t):
    return time_to_unix_timestamp(t) * long(1000) + t.microsecond / long(1000)

def convert_expiry_time(expiry_time):
    return time_to_qdb_timestamp(expiry_time) if expiry_time != None else long(0)

def convert_qdb_timespec_to_time(qdb_timespec):
    return datetime.datetime.fromtimestamp(qdb_timespec.tv_sec, tz) \
        + datetime.timedelta(microseconds=qdb_timespec.tv_nsec / 1000)

def convert_qdb_ts_range_t_to_time_couple(qdb_range):
    return (convert_qdb_timespec_to_time(qdb_range.begin),
            convert_qdb_timespec_to_time(qdb_range.end))

def convert_time_couple_to_qdb_ts_range_t(time_couple):
    rng = impl.qdb_ts_range_t()

    rng.begin.tv_sec = time_to_unix_timestamp(time_couple[0])
    rng.begin.tv_nsec = time_couple[0].microsecond * long(1000)
    rng.end.tv_sec = time_to_unix_timestamp(time_couple[1])
    rng.end.tv_nsec = time_couple[1].microsecond * long(1000)

    return rng

def convert_time_to_qdb_timespec(python_time):
    res = impl.qdb_timespec_t()

    res.tv_sec = time_to_unix_timestamp(python_time)
    res.tv_nsec = python_time.microsecond * long(1000)

    return res

def convert_time_couples_to_qdb_ts_range_t_vector(time_couples):
    vec = impl.RangeVec()

    c = len(time_couples)

    vec.resize(c)
    for i in xrange(c):
        vec[i].begin.tv_sec = time_to_unix_timestamp(time_couples[i][0])
        vec[i].begin.tv_nsec = time_couples[i][0].microsecond * long(1000)
        vec[i].end.tv_sec = time_to_unix_timestamp(time_couples[i][1])
        vec[i].end.tv_nsec = time_couples[i][1].microsecond * long(1000)

    return vec

def convert_to_wrap_ts_blop_points_vector(tuples):
    vec = BlobPointsVector()

    c = len(tuples)

    vec.resize(c)

    for i in xrange(c):
        vec[i].timestamp.tv_sec = time_to_unix_timestamp(tuples[i][0])
        vec[i].timestamp.tv_nsec = tuples[i][0].microsecond * long(1000)
        vec[i].data = tuples[i][1]

    return vec

def make_qdb_ts_double_point_vector(time_points):
    vec = DoublePointsVector()

    c = len(time_points)

    vec.resize(c)

    for i in xrange(c):
        vec[i].timestamp.tv_sec = time_to_unix_timestamp(time_points[i][0])
        vec[i].timestamp.tv_nsec = time_points[i][0].microsecond * long(1000)
        vec[i].value = time_points[i][1]

    return vec

def make_qdb_ts_int64_point_vector(time_points):
    vec = Int64PointsVector()

    c = len(time_points)

    vec.resize(c)

    for i in xrange(c):
        vec[i].timestamp.tv_sec = time_to_unix_timestamp(time_points[i][0])
        vec[i].timestamp.tv_nsec = time_points[i][0].microsecond * long(1000)
        vec[i].value = time_points[i][1]

    return vec

def make_qdb_ts_timestamp_point_vector(time_points):
    vec = TimestampPointsVector()

    c = len(time_points)

    vec.resize(c)

    for i in xrange(c):
        vec[i].timestamp.tv_sec = time_to_unix_timestamp(time_points[i][0])
        vec[i].timestamp.tv_nsec = time_points[i][0].microsecond * long(1000)
        vec[i].value.tv_sec = time_to_unix_timestamp(time_points[i][1])
        vec[i].value.tv_nsec = time_points[i][1].microsecond * long(1000)

    return vec

def make_error_carrier():
    err = impl.error_carrier()
    err.error = impl.error_uninitialized
    return err
