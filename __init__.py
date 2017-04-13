#
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

"""
.. module: quasardb
    :platform: Unix, Windows
    :synopsis: quasardb official Python API

.. moduleauthor: quasardb SAS. All rights reserved
"""

import quasardb.impl as impl
import json
import datetime
import time

def __make_enum(type_name, prefix):
    """
    Builds an enum from ints present whose prefix matches
    """
    members = dict()

    prefix_len = len(prefix)

    for x in dir(impl):
        if not x.startswith(prefix):
            continue

        v = getattr(impl, x)
        if isinstance(v, int):
            members[x[prefix_len:]] = v

    return type(type_name, (), members)

Error = __make_enum('Error', 'error_')
Options = __make_enum('Option', 'option_')
Operation = __make_enum('Operation', 'operation_')
Protocol = __make_enum('Protocol', 'protocol_')
TSAggregation = __make_enum('Aggregation', 'aggregation_')
TSColumnType = __make_enum('ColumnType', 'column_')

def make_error_string(error_code):
    """ Returns a meaningful error message corresponding to the quasardb error code.

    :param error_code: The error code to translate
    :returns: str -- An error string
    """
    return impl.make_error_string(error_code)

class QuasardbException(Exception):
    """The quasardb exception, based on the API error codes."""
    def __init__(self, error_code = 0):
        assert(isinstance(error_code, int))
        Exception.__init__(self)
        self.error_code = error_code

    def __repr__(self):
        return "quasardb exception - code " + str(self.error_code)

    def __str__(self):
        return make_error_string(self.error_code)

def version():
    """ Returns the API's version number as a string

    :returns: str -- The API version number
    """
    return impl.version()

def build():
    """ Returns the build tag and build date as a string

    :returns: str -- The API build tag
    """
    return impl.build()

import inspect

def _duration_to_timeout_ms(duration):
    return duration.days * 24 * 3600 * 1000 + duration.seconds * 1000 + duration.microseconds / 1000

def _api_buffer_to_string(buf):
    return None if buf is None else impl.api_buffer_ptr_as_string(buf)

def _string_to_api_buffer(h, s):
    """
    Converts a string to an internal qdb::api_buffer_ptr
    """
    return None if s is None else impl.make_api_buffer_ptr_from_string(h, s)

class TimeZone(datetime.tzinfo):
    """The quasardb time zone is UTC. Please refer to the documentation for further information."""

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)

tz = TimeZone()

def _time_to_unix_timestamp(t, tz_offset):
    return long(time.mktime((t.year, t.month, t.day, t.hour, t.minute, t.second, -1, -1, 0))) - tz_offset

def _time_to_qdb_timestamp(t, tz_offset):
    return _time_to_unix_timestamp(t, tz_offset) * 1000 + (t.microsecond / 1000)

def _convert_expiry_time(expiry_time):
    return _time_to_qdb_timestamp(expiry_time, time.timezone) if expiry_time != None else long(0)

def _convert_time_couples_to_qdb_range_t_vector(time_couples):
    vec = impl.RangeVec()

    c = len(time_couples)

    vec.resize(c)

    tz_offset = time.timezone

    for i in range(0, c):
        vec[i].begin.tv_sec = _time_to_unix_timestamp(time_couples[i][0], tz_offset)
        vec[i].begin.tv_nsec = time_couples[i][0].microsecond * 1000
        vec[i].end.tv_sec = _time_to_unix_timestamp(time_couples[i][1], tz_offset)
        vec[i].end.tv_nsec = time_couples[i][1].microsecond * 1000

    return vec

def _convert_to_wrap_ts_blop_points_vector(tuples):
    vec = impl.BlobPointVec()

    c = len(tuples)

    vec.resize(c)

    tz_offset = time.timezone

    for i in range(0, c):
        vec[i].timestamp.tv_sec =  _time_to_unix_timestamp(tuples[i][0], tz_offset)
        vec[i].timestamp.tv_nsec = tuples[i][0].microsecond * 1000
        vec[i].data = tuples[i][1]

    return vec

def make_qdb_ts_double_point_vector(time_points):
    vec = impl.DoublePointVec()

    c = len(time_points)

    vec.resize(c)

    tz_offset = time.timezone

    for i in range(0, c):
        vec[i].timestamp.tv_sec = _time_to_unix_timestamp(time_points[i][0], tz_offset)
        vec[i].timestamp.tv_nsec = time_points[i][0].microsecond * 1000
        vec[i].value = time_points[i][1]

    return vec

def _convert_qdb_timespec_to_time(qdb_timespec):
    return datetime.datetime.fromtimestamp(qdb_timespec.tv_sec, tz) + datetime.timedelta(microseconds=qdb_timespec.tv_nsec / 1000)

def _convert_qdb_range_t_to_time_couple(qdb_range):
    return (_convert_qdb_timespec_to_time(qdb_range.begin), _convert_qdb_timespec_to_time(qdb_range.end))

def make_error_carrier():
    err = impl.error_carrier()
    err.error = impl.error_uninitialized
    return err

class Entry(object):

    def __init__(self, handle, alias):
        self.handle = handle
        self.__alias = alias

    def alias(self):
        """
        :returns: The alias of the entry
        """
        return self.__alias

    def attach_tag(self, tag):
        """
            Attach a tag to the entry

            :param tag: The tag to attach
            :type tag: str

            :returns: True if the tag was successfully attached, False if it was already attached
            :raises: QuasardbException
        """
        err = self.handle.attach_tag(self.alias(), tag)
        if err == impl.error_tag_already_set:
            return False

        if err != impl.error_ok:
            raise QuasardbException(err)

        return True

    def detach_tag(self, tag):
        """
            Detach a tag from the entry

            :param tag: The tag to detach
            :type tag: str

            :returns: True if the tag was successfully detached, False if it was not attached
            :raises: QuasardbException
        """
        err = self.handle.detach_tag(self.alias(), tag)

        if err == impl.error_tag_not_set:
            return False

        if err != impl.error_ok:
            raise QuasardbException(err)

        return True

    def get_tags(self):
        """
            Returns the list of tags attached to the entry

            :returns: A list of alias (stings) of tags
            :raises: QuasardbException
        """
        err = make_error_carrier()
        result = impl.get_tags(self.handle, self.alias(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return result

    def has_tag(self, tag):
        """
            Test if a tag is attached to the entry

            :param tag: The tag to test
            :type tag: str

            :returns: True if the entry has the speciefied tag, False otherwise
            :raises: QuasardbException
        """
        err = self.handle.has_tag(self.alias(), tag)
        if err == impl.error_tag_not_set:
            return False

        if err != impl.error_ok:
            raise QuasardbException(err)

        return True


class RemoveableEntry(Entry):

    def __init__(self, handle, alias, *args, **kwargs):
        super(RemoveableEntry, self).__init__(handle, alias)

    def remove(self):
        """
            Removes the given Entry from the repository. It is an error to remove a non-existing Entry.

            :raises: QuasardbException
        """
        err = self.handle.remove(self.alias())
        if err != impl.error_ok:
            raise QuasardbException(err)

class ExpirableEntry(RemoveableEntry):

    def __init__(self, handle, alias, *args, **kwargs):
        super(ExpirableEntry, self).__init__(handle, alias)

    def expires_at(self, expiry_time):
        """
            Sets the expiry time of an existing Entry. If the value is None, the Entry never expires.

            :param expiry_time: The expiry time, must be offset aware
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        err = self.handle.expires_at(super(Blob, self).alias(), _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def expires_from_now(self, expiry_delta):
        """
            Sets the expiry time of an existing Entry relative to the current time, in milliseconds.

            :param expiry_delta: The expiry delta in milliseconds
            :type expiry_delta: long

            :raises: QuasardbException
        """
        err = self.handle.expires_from_now(super(Blob, self).alias(), long(expiry_delta))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def get_expiry_time(self):
        """
            Returns the expiry time of the Entry.

            :returns: datetime.datetime -- The expiry time, offset aware
            :raises: QuasardbException
        """
        err = make_error_carrier()
        t = impl.get_expiry_time_wrapper(self.handle, super(Blob, self).alias(), err)

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return datetime.datetime.fromtimestamp(t, tz)

class Tag(Entry):
    """
    A tag to perform tag-based queries, such as listing all entries having the tag.
    """
    def __init__(self, handle, alias, *args, **kwargs):
        super(Tag, self).__init__(handle, alias)

    def get_entries(self):
        """
            Returns all entries with the tag
            :raises: QuasardbException
        """
        err = make_error_carrier()
        result = impl.get_tagged(self.handle, self.alias(),  err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return result

class Integer(ExpirableEntry):
    """
    A 64-bit signed integer. Depending on your Python implementation and platform, the number represented in Python may or may
    not be a 64-bit signed integer
    """
    def __init__(self, handle, alias, *args, **kwargs):
        super(Integer, self).__init__(handle, alias)

    def get(self):
        """
            Returns the current value of the entry, as an integer.

            :returns: The value of the entry as an integer
            :raises: QuasardbException
        """
        err = make_error_carrier()
        res = impl.int_get(self.handle, super(Integer, self).alias(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return res

    def put(self, number, expiry_time = None):
        """
            Creates an integer of the specified value. The entry must not exist.

            :param number: The value of the entry to created
            :type number: long
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        assert(isinstance(number, long) or isinstance(number, int))
        err = self.handle.int_put(super(Integer, self).alias(), number, _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def update(self, number, expiry_time = None):
        """
            Updates an integer to the specified value. The entry may or may not exist.

            :param number: The value of the entry to created
            :type number: long
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        assert(isinstance(number, long) or isinstance(number, int))
        err = self.handle.int_update(super(Integer, self).alias(), number, _convert_expiry_time(expiry_time))
        if not ((err == impl.error_ok) or (err == impl.error_ok_created)):
            raise QuasardbException(err)

    def add(self, addend):
        """
            Adds the supplied addend to an existing integer. The operation is atomic and thread safe. The entry must exist.
            If addend is negative, the value will be substracted to the existing entry.

            :param number: The value to add to the existing entry.
            :type number: long

            :returns: The value of the entry post add
            :raises: QuasardbException
        """
        assert(isinstance(addend, long) or isinstance(addend, int))
        err = make_error_carrier()
        res = impl.int_add(self.handle, super(Integer, self).alias(), addend, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return res

class Deque(RemoveableEntry):
    """
    An unlimited, distributed, concurrent deque.
    """
    def __init__(self, handle, alias, *args, **kwargs):
        super(Deque, self).__init__(handle, alias)

    def push_front(self, data):
        """
            Appends a new Entry at the beginning of the deque. The deque will be created if it does not exist.

            :param data: The content for the Entry
            :type data: str

            :raises: QuasardbException
        """
        err = self.handle.deque_push_front(super(Deque, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def push_back(self, data):
        """
            Appends a new Entry at the end of the deque. The deque will be created if it does not exist.

            :param data: The content for the Entry
            :type data: str

            :raises: QuasardbException
        """
        err = self.handle.deque_push_back(super(Deque, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def __deque_getter(self, f):
        err = make_error_carrier()
        buf = f(self.handle, super(Deque, self).alias(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return _api_buffer_to_string(buf)

    def pop_front(self):
        """
            Atomically returns and remove the first Entry of the deque. The deque must exist and must not be empty.

            :raises: QuasardbException
            :returns: The first Entry of the deque
        """
        return self.__deque_getter(impl.deque_pop_front)

    def pop_back(self):
        """
            Atomically returns and remove the last Entry of the deque. The deque must exist and must not be empty.

            :raises: QuasardbException
            :returns: The last Entry of the deque
        """
        return self.__deque_getter(impl.deque_pop_back)

    def front(self):
        """
            Returns the first Entry of the deque. The deque must exist and must not be empty.

            :raises: QuasardbException
            :returns: The first Entry of the deque
        """
        return self.__deque_getter(impl.deque_front)

    def back(self):
        """
            Returns the last Entry of the deque. The deque must exist and must not be empty.

            :raises: QuasardbException
            :returns: The last Entry of the deque
        """
        return self.__deque_getter(impl.deque_back)

    def size(self):
        """
            Returns the current size of the deque.

            :raises: QuasardbException
            :returns: The current size of the deque.
        """
        raise QuasardbException(impl.error_not_implemented)

class HSet(RemoveableEntry):
    """
    An unlimited, distributed, concurrent hash set.
    """
    def __init__(self, handle, alias, *args, **kwargs):
        super(HSet, self).__init__(handle, alias)

    def insert(self, data):
        """
            Inserts a new Entry into the hash set. If the hash set does not exist, it will be created.

            :raises: QuasardbException
        """
        err = self.handle.hset_insert(super(HSet, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def erase(self, data):
        """
            Erases an existing an Entry from an existing hash set.

            :raises: QuasardbException
        """
        err = self.handle.hset_erase(super(HSet, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def contains(self, data):
        """
            Tests if the Entry exists in the hash set. The hash set must exist.

            :raises: QuasardbException
            :returns: True if the Entry exists, false otherwise
        """
        err = self.handle.hset_contains(super(HSet, self).alias(), data)

        if err == impl.error_element_not_found:
            return False

        if err != impl.error_ok:
            raise QuasardbException(err)

        return True

    def size(self):
        """
            Returns the current size of the hash set.

            :raises: QuasardbException
            :returns: The current size of the hash set.
        """
        raise QuasardbException(impl.error_not_implemented)

class TimeSeries(RemoveableEntry):
    """
    An unlimited, distributed, time series with nanosecond granularity and server-side aggregation capabilities.
    """

    Aggregation = TSAggregation
    ColumnType = TSColumnType

    class AggregationResult:
        """
        An aggregation result holding the range on which the aggregation was perfomed, the result value as well as the timestamp if it applies.
        """
        def __init__(self, r, ts, value):
            self.range = r
            self.timestamp = ts
            self.value = value

    class ColumnInfo:
        """
        An object holding column information such as the name and the type.
        """
        def __init__(self, col_name, col_type):
            """
            Creates a TimeSeries.ColumnInfo object

            :param col_name: The name of the column
            :type col_name: str
            :param col_type: The type of the column
            :type col_type: A TimeSeries.ColumnType value
            """
            self.name = col_name
            self.type = col_type

    class DoubleColumnInfo(ColumnInfo):
        def __init__(self, col_name):
            TimeSeries.ColumnInfo.__init__(self, col_name, TimeSeries.ColumnType.double)

    class BlobColumnInfo(ColumnInfo):
        def __init__(self, col_name):
            TimeSeries.ColumnInfo.__init__(self, col_name, TimeSeries.ColumnType.blob)

    class Column(object):
        """
        A column object within a time series on which one can get ranges and run aggregations.
        """
        def __init__(self, ts, col_name):
            self.__ts = ts
            self.__col_name = col_name

        def call_ts_fun(self, ts_func, *args, **kwargs):
            return self.__ts.call_ts_fun(ts_func, self.__col_name, *args, **kwargs)

        def name(self):
            """
            Returns the name of the column

            :returns: The name of the column
            """
            return self.__col_name

        def aggregate(self, agg_type, intervals):
            """
            Aggregates values over the given intervals

            :param agg_type: The aggregation type
            :type agg_type: A value from TimeSeries.Aggregation
            :param intervals: The ranges on which to perform the aggregations
            :type intervals: a list of datetime.datetime couples

            :raises: QuasardbException
            :returns: The list of aggregation results
            """
            error_carrier = make_error_carrier()

            res = self.call_ts_fun(impl.ts_double_aggregation, agg_type, _convert_time_couples_to_qdb_range_t_vector(intervals), error_carrier)
            if error_carrier.error != impl.error_ok:
                raise QuasardbException(error_carrier.error)

            return map(lambda x: TimeSeries.AggregationResult(_convert_qdb_range_t_to_time_couple(x.range), _convert_qdb_timespec_to_time(x.result.timestamp), x.result.value), res)

    class DoubleColumn(Column):
        """
        A column whose value are double precision floats
        """
        def __init__(self, ts, col_name):
            super(TimeSeries.DoubleColumn, self).__init__(ts, col_name)

        def insert(self, tuples):
            """
            Inserts values into the time series.

            :param tuples: The list of couples to insert into the time series
            :type tuples: list of datetime.datetime, float couples

            :raises: QuasardbException
            """
            err = super(TimeSeries.DoubleColumn, self).call_ts_fun(impl.ts_double_insert, make_qdb_ts_double_point_vector(tuples))
            if err != impl.error_ok:
                raise QuasardbException(err)

        def get_ranges(self, intervals):
            """
            Returns the ranges matching the provided intervals.

            :param intervals: The intervals for which the ranges should be returned
            :type intervals: A list of datetime.datetime couples

            :raises: QuasardbException
            :returns: A flattened list of datetime.datetime, float couples
            """
            error_carrier = make_error_carrier()

            res = super(TimeSeries.DoubleColumn, self).call_ts_fun(impl.ts_double_get_range, _convert_time_couples_to_qdb_range_t_vector(intervals), error_carrier)
            if error_carrier.error != impl.error_ok:
                raise QuasardbException(error_carrier.error)

            return map(lambda x: (_convert_qdb_timespec_to_time(x.timestamp), x.value), res)

    class BlobColumn(Column):
        """
        A column whose values are blobs
        """
        def __init__(self, ts, col_name):
            super(TimeSeries.BlobColumn, self).__init__(ts, col_name)

        def insert(self, tuples):
            """
            Inserts values into the time series.

            :param tuples: The list of couples to insert into the time series
            :type tuples: list of datetime.datetime, str couples

            :raises: QuasardbException
            """
            err = super(TimeSeries.BlobColumn, self).call_ts_fun(impl.ts_blob_insert, _convert_to_wrap_ts_blop_points_vector(tuples))
            if err != impl.error_ok:
                raise QuasardbException(err)

        def get_ranges(self, intervals):
            """
            Returns the ranges matching the provided intervals.

            :param intervals: The intervals for which the ranges should be returned
            :type intervals: A list of datetime.datetime couples

            :raises: QuasardbException
            :returns: A flattened list of datetime.datetime, str couples
            """
            error_carrier = make_error_carrier()

            res = super(TimeSeries.BlobColumn, self).call_ts_fun(impl.ts_blob_get_range, _convert_time_couples_to_qdb_range_t_vector(intervals), error_carrier)
            if error_carrier.error != impl.error_ok:
                raise QuasardbException(error_carrier.error)

            return map(lambda x: (_convert_qdb_timespec_to_time(x.timestamp), x.data), res)

    def __init__(self, handle, alias, *args, **kwargs):
        super(TimeSeries, self).__init__(handle, alias)

    def call_ts_fun(self, ts_func, *args, **kwargs):
        return ts_func(self.handle, super(TimeSeries, self).alias(), *args, **kwargs)

    def create(self, columns):
        """
        Creates a time series with the provided columns information

        :param columns: A list describing the columns to create
        :type columns: a list of TimeSeries.ColumnInfo

        :raises: QuasardbException
        :returns: A list of columns matching the created columns
        """
        err = self.call_ts_fun(impl.ts_create, map(lambda x: impl.wrap_ts_column(x.name, x.type), columns))
        if err != impl.error_ok:
            raise QuasardbException(err)

        return map(lambda x: self.column(x), columns)

    def column(self, col_info):
        """
        Accesses an existing column.

        :param col_info: A description of the column to access
        :type col_info: TimeSeries.ColumnInfo

        :raises: QuasardbException
        :returns: A TimeSeries.Column matching the provided information
        """
        if col_info.type == TimeSeries.ColumnType.blob:
            return TimeSeries.BlobColumn(self, col_info.name)

        if col_info.type == TimeSeries.ColumnType.double:
            return TimeSeries.DoubleColumn(self, col_info.name)

        raise QuasardbException(Error.invalid_argument)

    def columns(self):
        """
        Returns all existing columns.

        :raises: QuasardbException
        :returns: A list of all existing columns as TimeSeries.Column objects
        """
        return map(lambda x: self.column(x), self.columns_info())

    def columns_info(self):
        """
        Returns all existing columns information.

        :raises: QuasardbException
        :returns: A list of all existing columns as TimeSeries.ColumnInfo objects
        """
        error_carrier = make_error_carrier()
        raw_cols = self.call_ts_fun(impl.ts_list_columns, error_carrier)
        if error_carrier.error != impl.error_ok:
            raise QuasardbException(error_carrier.error)

        return map(lambda x: TimeSeries.ColumnInfo(x.name, x.type), raw_cols)

class Blob(ExpirableEntry):

    def __init__(self, handle, alias, *args, **kwargs):
        super(Blob, self).__init__(handle, alias)

    def put(self, data, expiry_time=None):
        """ Creates a blob into the repository.
        It is an error to call this method on a blob that already exists. Use the :py:meth:`update` method to update a blob.
        If expiry_time is None, the blob never expires.

            :param data: The content for the alias
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        err = self.handle.blob_put(super(Blob, self).alias(), data, _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def update(self, data, expiry_time=None):
        """ Updates the given alias.
        If the alias is not found in the repository, the blob is created.
        If expiry_time is None, the blob never expires.

            :param data: The content for the alias
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        err = self.handle.blob_update(super(Blob, self).alias(), data, _convert_expiry_time(expiry_time))
        if not ((err == impl.error_ok) or (err == impl.error_ok_created)):
            raise QuasardbException(err)

    def get(self):
        """ Gets the data for the given alias.
        It is an error to request a non-existing blob.

            :returns: str -- The associated content
            :raises: QuasardbException
        """
        err = make_error_carrier()
        buf = impl.blob_get(self.handle, super(Blob, self).alias(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def get_and_update(self, data, expiry_time=None):
        """ Updates the blob and returns the previous value.
        It is an error to call this method on a non-existing blob.
        If expiry_time is None, the blob never expires.

            :param data: The new data to put in place
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :returns: str -- The original content
            :raises: QuasardbException
        """
        err = make_error_carrier()
        buf = impl.blob_get_and_update(self.handle, super(Blob, self).alias(), data, _convert_expiry_time(expiry_time), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def get_and_remove(self):
        """ Atomically gets the data from the blob and removes it.
        It is an error to call this method on a non-existing blob.
        If expiry_time is None, the entry never expires.

            :returns: str -- The associated content
            :raises: QuasardbException
        """
        err = make_error_carrier()
        buf = impl.blob_get_and_remove(self.handle, super(Blob, self).alias(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def compare_and_swap(self, new_data, comparand, expiry_time=None):
        """ Atomically compares the blob with comparand and replaces it with new_data if it matches.
            If expiry_time is None, the entry never expires.

            :param new_data: The new content to put in place if the comparand matches
            :type new_data: str
            :param comparand: The content to compare to
            :type comparand: str
            :param expiry_time: The expiry time for the blob
            :type expiry_time: datetime.datetime

            :returns: str -- The original content or None if it mached
            :raises: QuasardbException
        """
        err = make_error_carrier()
        buf = impl.blob_compare_and_swap(self.handle, super(Blob, self).alias(), new_data, comparand, _convert_expiry_time(expiry_time), err)
        if err.error == impl.error_unmatched_content:
            return _api_buffer_to_string(buf)

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return None

    def remove_if(self, comparand):
        """ Removes the blob from the repository if it matches comparand. The operation is atomic.
            It is an error to attempt to remove a non-existing blob.

            :param comparand: The content to compare to
            :type comparand: str

            :raises: QuasardbException
        """
        err = self.handle.blob_remove_if(super(Blob, self).alias(), comparand)
        if err != impl.error_ok:
            raise QuasardbException(err)

class Cluster(object):
    """
    """

    def __init__(self, uri, timeout, *args, **kwargs):
        """ Creates the raw client.
        Connection is delayed until the client is actually used.
        If the client is not used for some time, the connection is dropped and reestablished at need.

            :param uri: The connection string
            :type uri: str
            :param timeout: The connection timeout
            :type timeout: datetime.timedelta

            :raises: QuasardbException
        """
        err = make_error_carrier()

        timeout_value = _duration_to_timeout_ms(timeout)
        if timeout_value == 0:
            raise QuasardbException(impl.error_invalid_argument)

        self.handle = None
        self.handle = impl.connect(uri, timeout_value, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

    def __del__(self):
        """ On delete, the connection is closed.
        """
        if self.handle != None:
            self.handle.close()
            self.handle = None

    def set_timeout(self, duration):
        """
        Sets the timeout value for requests. Requests that take longer than timeout value will raise an exception.

        The minimum timemout value is 1 ms.

        :param duration: The timeout value
        :type duration: datetime.timedelta

        :raises: QuasardbException
        """
        timeout_value = _duration_to_timeout_ms(duration)
        if timeout_value == 0:
            raise QuasardbException(impl.error_invalid_argument)
        err = self.handle.set_timeout(timeout_value)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def blob(self, alias):
        """
        Returns an object representing a blob with the provided alias. The blob may or may not exist yet.

        :param alias: The alias of the blob to work on
        :type alias: str

        :returns: The blob named alias
        """
        return Blob(self.handle, alias)

    def integer(self, alias):
        """
        Returns an object representing an integer with the provided alias. The blob may or may not exist yet.

        :param alias: The alias of the integer to work on
        :type alias: str

        :returns: The integer named alias
        """
        return Integer(self.handle, alias)

    def deque(self, alias):
        """
        Returns an object representing a deque with the provided alias. The deque may or may not exist yet.

        :param alias: The alias of the deque to work on
        :type alias: str

        :returns: The deque named alias
        """
        return Deque(self.handle, alias)

    def hset(self, alias):
        """
        Returns an object representing a hash set with the provided alias. The hash set may or may not exist yet.

        :param alias: The alias of the hash set to work on
        :type alias: str

        :returns: The hash set named alias
        """
        return HSet(self.handle, alias)

    def ts(self, alias):
        """
        Return an object representing a time series with the provided alias. The time series may or may not exist yet.

        :param alias: The alias of the time series set to work on
        :type alias: str

        :returns: The time series named alias
        """
        return TimeSeries(self.handle, alias)

    def tag(self, alias):
        """
        Returns an object representing a tag with the provided alias. The tag may or may not exist yet.

        :param alias: The alias of the tag to work on
        :type alias: str

        :returns: The tag named alias
        """
        return Tag(self.handle, alias)

    def purge_all(self, timeout):
        """ Removes all the entries from all nodes of the cluster.

            :param timeout: Operation timeout
            :type timeout: datetime.timedelta

            :raises: QuasardbException

            .. caution::
                This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.purge_all(_duration_to_timeout_ms(timeout))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def trim_all(self, timeout):
        """ Trims all entries of all nodes of the cluster.

            :param timeout: Operation timeout
            :type timeout: datetime.timedelta

            :raises: QuasardbException
        """
        err = self.handle.trim_all(_duration_to_timeout_ms(timeout))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def stop_node(self, uri, reason):
        """ Stops a node.

            :param uri: The connection string
            :type uri: str
            :param reason: A string describing the reason for the stop
            :type reason: str

            :raises: QuasardbException

            .. caution::
                This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.stop_node(uri, reason)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def node_config(self, uri):
        """ Retrieves the configuration of a given node in JSON format.

            :param uri: The connection string
            :type uri: str

            :returns: dict -- The requested configuration

            :raises: QuasardbException
        """
        err = make_error_carrier()
        res = impl.node_config(self.handle, uri, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return json.loads(res)

    def node_status(self, uri):
        """ Retrieves the status of a given node in JSON format.

            :param uri: The connection string
            :type uri: str

            :returns: dict -- The requested status

            :raises: QuasardbException
        """
        err = make_error_carrier()
        res = impl.node_status(self.handle, uri, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return json.loads(res)

    def node_topology(self, uri):
        """ Retrieves the topology of a given node in JSON format.

            :param uri: The connection string
            :type uri: str

            :returns: dict -- The requested topology

            :raises: QuasardbException
        """
        err = make_error_carrier()
        res = impl.node_topology(self.handle, uri, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return json.loads(res)

    def query(self, q):
        """
        Retrieves all entries' aliases that match the specified query.

        :param q: The query to run
        :type q: str

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: QuasardbException
        """
        err = make_error_carrier()
        result = impl.run_query(self.handle, q, err)

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return result

    def prefix_get(self, prefix, max_count):
        """
        Retrieves the list of all entries matching the provided prefix.

        :param prefix: The prefix to use for the search
        :type prefix: str

        :param max_count: The maximum number of entries to return
        :type max_count: long

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: QuasardbException
        """
        err = make_error_carrier()
        result = impl.prefix_get(self.handle, prefix, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return result

    def prefix_count(self, prefix):
        """
        Returns the count of all entries matching the provided prefix.

        :param prefix: The prefix to use for the count
        :type prefix: str

        :returns: The count of entries matching the provided prefix. 0 if there is no match.

        :raises: QuasardbException
        """
        err = make_error_carrier()
        count = impl.prefix_count(self.handle, prefix, err)

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return count

    def suffix_get(self, suffix, max_count):
        """
        Retrieves the list of all entries matching the provided suffix.

        :param suffix: The suffix to use for the search
        :type suffix: str

        :param max_count: The maximum number of entries to return
        :type max_count: long

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: QuasardbException

        """
        err = make_error_carrier()
        result = impl.suffix_get(self.handle, suffix, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return result

    def suffix_count(self, suffix):
        """
        Returns the count of all entries matching the provided suffix.

        :param suffix: The suffix to use for the count
        :type suffix: str

        :returns: The count of entries matching the provided suffix. 0 if there is no match.

        :raises: QuasardbException
        """
        err = make_error_carrier()
        count = impl.suffix_count(self.handle, suffix, err)

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return count

    def blob_scan(self, pattern, max_count):
        """ Scans all blobs and returns the list of entries whose content matches the provided sub-string.

            :param pattern: The substring to look for
            :type pattern: str

            :param max_count: The maximum number of entries to return
            :type max_count: long

            :returns: The list of matching aliases. If no match is found, returns an empty list.

            :raises: QuasardbException
        """
        err = make_error_carrier()
        result = impl.blob_scan(self.handle, pattern, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return result

    def blob_scan_regex(self, pattern, max_count):
        """ Scans all blobs and returns the list of entries whose content that match the provided
         regular expression (ECMA-262).

        :param pattern: The regular expression to use for matching
        :type pattern: str

        :param max_count: The maximum number of entries to return
        :type max_count: long

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: QuasardbException
        """
        err = make_error_carrier()
        result = impl.blob_scan_regex(self.handle, pattern, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return result