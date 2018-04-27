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

"""
.. module: quasardb
    :platform: Unix, Windows
    :synopsis: quasardb official Python API

.. moduleauthor: quasardb SAS. All rights reserved
"""

from builtins import range as xrange, int as long  # pylint: disable=W0622
import json
import datetime
import numbers
import quasardb.impl as impl  # pylint: disable=C0413,E0401
import quasardb.qdb_convert

from quasardb.qdb_enum import Compression, Encryption, ErrorCode, Operation, Options, Protocol, \
    TSAggregation, TSColumnType

tz = qdb_convert.tz

DoublePointsVector = quasardb.qdb_convert.DoublePointsVector
BlobPointsVector = quasardb.qdb_convert.BlobPointsVector
Int64PointsVector = quasardb.qdb_convert.Int64PointsVector
TimestampPointsVector = quasardb.qdb_convert.TimestampPointsVector


def make_error_string(error_code):
    """ Returns a meaningful error message corresponding to the quasardb error code.

    :param error_code: The error code to translate
    :returns: str -- An error string
    """
    return impl.make_error_string(error_code)


class Error(Exception):
    """The quasardb database exception, based on the API error codes."""

    def __init__(self, error_code=0):
        assert isinstance(error_code, long)
        Exception.__init__(self)
        self.code = error_code
        self.origin = impl.error_origin(error_code)
        self.severity = impl.error_severity(error_code)

        self.error_code = self.code  # Deprecated. Use code.

    def __repr__(self):
        return "quasardb exception - code " + format(self.error_code, '#0x')

    def __str__(self):
        return make_error_string(self.error_code) + ' (code ' + format(self.error_code, '#0x') + ')'


# Deprecated name. Please use Error.
QuasardbException = Error


class RemoteSystemError(Error):
    pass


class LocalSystemError(Error):
    pass


class ConnectionError(Error):
    pass


class InputError(Error):
    pass


class OperationError(Error):
    pass


class ProtocolError(Error):
    pass


def chooseError(error_code=0):
    return {
        impl.error_origin_system_remote: RemoteSystemError(error_code),
        impl.error_origin_system_local: LocalSystemError(error_code),
        impl.error_origin_connection: ConnectionError(error_code),
        impl.error_origin_input: InputError(error_code),
        impl.error_origin_operation: OperationError(error_code),
        impl.error_origin_protocol: ProtocolError(error_code)
    }.get(impl.error_origin(error_code), Error(error_code))


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
            :raises: Error
        """
        err = self.handle.attach_tag(self.alias(), tag)
        if err == impl.error_tag_already_set:
            return False

        if err != impl.error_ok:
            raise chooseError(err)

        return True

    def detach_tag(self, tag):
        """
            Detach a tag from the entry

            :param tag: The tag to detach
            :type tag: str

            :returns: True if the tag was successfully detached, False if it was not attached
            :raises: Error
        """
        err = self.handle.detach_tag(self.alias(), tag)

        if err == impl.error_tag_not_set:
            return False

        if err != impl.error_ok:
            raise chooseError(err)

        return True

    def get_tags(self):
        """
            Returns the list of tags attached to the entry

            :returns: A list of alias (stings) of tags
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        result = impl.get_tags(self.handle, self.alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result

    def has_tag(self, tag):
        """
            Test if a tag is attached to the entry

            :param tag: The tag to test
            :type tag: str

            :returns: True if the entry has the specified tag, False otherwise
            :raises: Error
        """
        err = self.handle.has_tag(self.alias(), tag)
        if err == impl.error_tag_not_set:
            return False

        if err != impl.error_ok:
            raise chooseError(err)

        return True


class RemoveableEntry(Entry):

    def __init__(self, handle, alias, *args, **kwargs):  # pylint: disable=W0613
        super(RemoveableEntry, self).__init__(handle, alias)

    def remove(self):
        """
        Removes the given Entry from the repository. It is an error to remove a non-existing Entry.

        :raises: Error
        """
        err = self.handle.remove(self.alias())
        if err != impl.error_ok:
            raise chooseError(err)


class ExpirableEntry(RemoveableEntry):

    def __init__(self, handle, alias, *args, **kwargs):  # pylint: disable=W0613
        super(ExpirableEntry, self).__init__(handle, alias)

    def expires_at(self, expiry_time):
        """
        Sets the expiry time of an existing Entry. If the value is None, the Entry never expires.

        :param expiry_time: The expiry time, must be offset aware
        :type expiry_time: datetime.datetime

        :raises: Error
        """
        err = self.handle.expires_at(
            super(ExpirableEntry, self).alias(), qdb_convert.convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise chooseError(err)

    def expires_from_now(self, expiry_delta):
        """
        Sets the expiry time of an existing Entry relative to the current time, in milliseconds.

        :param expiry_delta: The expiry delta in milliseconds
        :type expiry_delta: long

        :raises: Error
        """
        err = self.handle.expires_from_now(
            super(ExpirableEntry, self).alias(), long(expiry_delta))
        if err != impl.error_ok:
            raise chooseError(err)

    def get_expiry_time(self):
        """
        Returns the expiry time of the Entry.

        :returns: datetime.datetime -- The expiry time, offset aware
        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        t = impl.get_expiry_time_wrapper(
            self.handle, super(ExpirableEntry, self).alias(), err)

        if err.error != impl.error_ok:
            raise chooseError(err.error)

        return datetime.datetime.fromtimestamp(t, tz)


class Tag(Entry):
    """
    A tag to perform tag-based queries, such as listing all entries having the tag.
    """

    def __init__(self, handle, alias, *args, **kwargs):  # pylint: disable=W0613
        super(Tag, self).__init__(handle, alias)

    def get_entries(self):
        """
            Returns all entries with the tag

            :returns: The list of entries aliases tagged
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        result = impl.get_tagged(self.handle, self.alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result

    def count(self):
        """
            Returns an approximate count of the entries matching the tag,
            up to the configured maximum cardinality.

            :returns: The approximative count of entries tagged
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        result = impl.get_tagged_count(self.handle, self.alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result


class Integer(ExpirableEntry):
    """
    A 64-bit signed integer.
    Depending on your Python implementation and platform,
    the number represented in Python may or may not be a 64-bit signed integer.
    """

    def __init__(self, handle, alias, *args, **kwargs):
        super(Integer, self).__init__(handle, alias)

    def get(self):
        """
            Returns the current value of the entry, as an integer.

            :returns: The value of the entry as an integer
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        res = impl.int_get(self.handle, super(Integer, self).alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return res

    def put(self, number, expiry_time=None):
        """
            Creates an integer of the specified value. The entry must not exist.

            :param number: The value of the entry to created
            :type number: long
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: Error
        """
        assert isinstance(number, long)
        err = self.handle.int_put(super(Integer, self).alias(
        ), number, qdb_convert.convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise chooseError(err)

    def update(self, number, expiry_time=None):
        """
            Updates an integer to the specified value. The entry may or may not exist.

            :param number: The value of the entry to created
            :type number: long
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: Error
        """
        assert isinstance(number, long)
        err = self.handle.int_update(
            super(Integer, self).alias(), number, qdb_convert.convert_expiry_time(expiry_time))
        if not ((err == impl.error_ok) or (err == impl.error_ok_created)):
            raise chooseError(err)

    def add(self, addend):
        """
            Adds the supplied addend to an existing integer.
            The operation is atomic and thread safe. The entry must exist.
            If addend is negative, the value will be substracted to the existing entry.

            :param number: The value to add to the existing entry.
            :type number: long

            :returns: The value of the entry post add
            :raises: Error
        """
        assert isinstance(addend, long)
        err = qdb_convert.make_error_carrier()
        res = impl.int_add(self.handle, super(
            Integer, self).alias(), addend, err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return res


class Deque(RemoveableEntry):
    """
    An unlimited, distributed, concurrent deque.
    """

    def __init__(self, handle, alias, *args, **kwargs):
        super(Deque, self).__init__(handle, alias)

    def push_front(self, data):
        """
        Appends a new Entry at the beginning of the deque.
        The deque will be created if it does not exist.

        :param data: The content for the Entry
        :type data: str

        :raises: Error
        """
        err = self.handle.deque_push_front(super(Deque, self).alias(), data)
        if err != impl.error_ok:
            raise chooseError(err)

    def push_back(self, data):
        """
        Appends a new Entry at the end of the deque.
        The deque will be created if it does not exist.

        :param data: The content for the Entry
        :type data: str

        :raises: Error
        """
        err = self.handle.deque_push_back(super(Deque, self).alias(), data)
        if err != impl.error_ok:
            raise chooseError(err)

    def __deque_getter(self, f):
        err = qdb_convert.make_error_carrier()
        buf = f(self.handle, super(Deque, self).alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return qdb_convert.api_buffer_to_string(buf)

    def pop_front(self):
        """
        Atomically returns and remove the first Entry of the deque.
        The deque must exist and must not be empty.

        :raises: Error
        :returns: The first Entry of the deque
        """
        return self.__deque_getter(impl.deque_pop_front)

    def pop_back(self):
        """
        Atomically returns and remove the last Entry of the deque.
        The deque must exist and must not be empty.

        :raises: Error
        :returns: The last Entry of the deque
        """
        return self.__deque_getter(impl.deque_pop_back)

    def front(self):
        """
        Returns the first Entry of the deque.
        The deque must exist and must not be empty.

        :raises: Error
        :returns: The first Entry of the deque
        """
        return self.__deque_getter(impl.deque_front)

    def back(self):
        """
        Returns the last Entry of the deque.
        The deque must exist and must not be empty.

        :raises: Error
        :returns: The last Entry of the deque
        """
        return self.__deque_getter(impl.deque_back)

    def size(self):
        """
        Returns the current size of the deque.

        :raises: Error
        :returns: The current size of the deque.
        """
        err = qdb_convert.make_error_carrier()
        res = impl.deque_size(self.handle, super(Deque, self).alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return res


class TimeSeries(RemoveableEntry):
    """
    An unlimited, distributed, time series with nanosecond granularity
    and server-side aggregation capabilities.
    """

    Aggregation = TSAggregation
    ColumnType = TSColumnType

    class LocalTable(object):
        """
        A local table object that enables row by row bulk inserts.
        """

        def __init__(self, ts, columns=None):
            """
            Creates a local table bound to a time series object. If columns is None, the
            LocalTable will match all existing columns.

            :param ts: The time series object to which the local table must be bound
            :type ts: quasardb.TimeSeries
            :param columns: A list of columns to build the LocalTable
            :type columns: A list of quasardb.ColumnInfo

            :raises: Error
            """
            self.__ts = ts

            error_carrier = qdb_convert.make_error_carrier()

            if columns is None:
                self.__table_handle = impl.ts_make_local_table(
                    self.__ts.handle,
                    super(
                        TimeSeries, self.__ts).alias(),
                    error_carrier)
            else:
                self.__table_handle = impl.ts_make_local_table_with_columns(
                    self.__ts.handle,
                    super(
                        TimeSeries, self.__ts).alias(),
                    [impl.wrap_ts_column(
                        x.name, x.type) for x in columns],
                    error_carrier)

            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

        def __del__(self):
            """ On delete, the object is released. """
            if self.__table_handle != None:
                impl.ts_release_local_table(
                    self.__ts.handle, self.__table_handle)
                self.__table_handle = None

        def fast_append_row(self, timestamp, *args):
            """
            Appends a row to the local table.
            The content will not be sent to the cluster until push is called

            :param timestamp: The timestamp at which to add the row
            :type timestamp: qdb_timespec_t

            :param args: The columns of the row, to skip a column, use None
            :type args: floating points or blobs or None

            :raises: Error
            :returns: The current row index
            """
            col_index = 0

            for arg in args:
                if arg != None:
                    if isinstance(arg, numbers.Integral):
                        err = impl.ts_row_set_int64(
                            self.__table_handle, col_index, long(arg))
                    elif isinstance(arg, numbers.Real):
                        err = impl.ts_row_set_double(
                            self.__table_handle, col_index, float(arg))
                    elif isinstance(arg, datetime.datetime):
                        err = impl.ts_row_set_timestamp(
                            self.__table_handle, col_index,
                            qdb_convert.convert_time_to_qdb_timespec(arg))
                    else:
                        err = impl.ts_row_set_blob(
                            self.__table_handle, col_index, str(arg))

                    if err != impl.error_ok:
                        raise chooseError(err)

                col_index += 1

            error_carrier = qdb_convert.make_error_carrier()
            row_index = impl.ts_table_row_append(
                self.__table_handle, timestamp, error_carrier)
            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

            return row_index

        def append_row(self, timestamp, *args):
            """
            Appends a row to the local table.
            The content will not be sent to the cluster until push is called

            :param timestamp: The timestamp at which to add the row
            :type timestamp: datetime.datetime

            :param args: The columns of the row, to skip a column, use None
            :type args: floating points or blobs or None

            :raises: Error
            :returns: The current row index
            """
            return self.fast_append_row(qdb_convert.convert_time_to_qdb_timespec(timestamp), *args)

        def push(self):
            """
            Pushes the content of the local table to the remote cluster.

            :raises: Error
            """
            err = impl.ts_push(self.__table_handle)

            if err != impl.error_ok:
                raise chooseError(err)

    class BlobAggregationResult(object):
        """
        An aggregation result holding the range on which the aggregation was perfomed,
        the result value as well as the timestamp if it applies.
        """

        def __init__(self, t, r, ts, count, content, content_length):
            self.type = t
            self.range = r
            self.timestamp = ts
            self.count = count
            self.content = impl.api_content_as_string(content, content_length)
            self.content_length = content_length

    class DoubleAggregationResult(object):
        """
        An aggregation result holding the range on which the aggregation was perfomed,
        the result value as well as the timestamp if it applies.
        """

        def __init__(self, t, r, ts, count, value):
            self.type = t
            self.range = r
            self.timestamp = ts
            self.count = count
            self.value = value

    class BlobAggregations(object):
        def __init__(self, size=0):
            self.__aggregations = impl.BlobAggVec()
            self.__aggregations.reserve(size)

        def append(self, agg_type, time_couple):
            """
            Appends an aggregation

            :param agg_type: the type of aggregation
            :type agg_type: TimeSeries.Aggregation

            :param time_couple: the interval on which to perform the aggregation
            :type time_couple: a couple of datetime.datetime
            """
            agg = impl.qdb_ts_blob_aggregation_t()

            agg.type = agg_type
            agg.range = qdb_convert.convert_time_couple_to_qdb_ts_range_t(
                time_couple)
            agg.count = 0
            agg.result.content_length = 0

            self.__aggregations.push_back(agg)

        def __getitem__(self, index):
            if self.__aggregations.size() <= index:
                raise IndexError()

            agg = self.__aggregations[index]
            return TimeSeries.BlobAggregationResult(
                agg.type,
                qdb_convert.convert_qdb_ts_range_t_to_time_couple(
                    agg.range),
                qdb_convert.convert_qdb_timespec_to_time(agg.result.timestamp),
                agg.count,
                agg.result.content,
                agg.result.content_length)

        def __len__(self):
            return self.__aggregations.size()

        def cpp_struct(self):
            return self.__aggregations

    class DoubleAggregations(object):
        def __init__(self, size=0):
            self.__aggregations = impl.DoubleAggVec()
            self.__aggregations.reserve(size)

        def append(self, agg_type, time_couple):
            """
            Appends an aggregation

            :param agg_type: the type of aggregation
            :type agg_type: TimeSeries.Aggregation

            :param time_couple: the interval on which to perform the aggregation
            :type time_couple: a couple of datetime.datetime
            """
            agg = impl.qdb_ts_double_aggregation_t()

            agg.type = agg_type
            agg.range = qdb_convert.convert_time_couple_to_qdb_ts_range_t(
                time_couple)
            agg.count = 0
            agg.result.value = 0.0

            self.__aggregations.push_back(agg)

        def __getitem__(self, index):
            if self.__aggregations.size() <= index:
                raise IndexError()

            agg = self.__aggregations[index]
            return TimeSeries.DoubleAggregationResult(
                agg.type,
                qdb_convert.convert_qdb_ts_range_t_to_time_couple(
                    agg.range),
                qdb_convert.convert_qdb_timespec_to_time(agg.result.timestamp),
                agg.count,
                agg.result.value)

        def __len__(self):
            return self.__aggregations.size()

        def cpp_struct(self):
            return self.__aggregations

    class ColumnInfo(object):
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
            TimeSeries.ColumnInfo.__init__(
                self, col_name, TimeSeries.ColumnType.double)

    class Int64ColumnInfo(ColumnInfo):

        def __init__(self, col_name):
            TimeSeries.ColumnInfo.__init__(
                self, col_name, TimeSeries.ColumnType.int64)

    class TimestampColumnInfo(ColumnInfo):

        def __init__(self, col_name):
            TimeSeries.ColumnInfo.__init__(
                self, col_name, TimeSeries.ColumnType.timestamp)

    class BlobColumnInfo(ColumnInfo):

        def __init__(self, col_name):
            TimeSeries.ColumnInfo.__init__(
                self, col_name, TimeSeries.ColumnType.blob)

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

        def aggregate(self, ts_func, aggregations):
            """
            Aggregates values over the given intervals

            :param ts_func: Function to call
            :param aggregations: The aggregations to perform

            :raises: Error
            :returns: A list of aggregation results
            """
            error_carrier = qdb_convert.make_error_carrier()

            self.call_ts_fun(ts_func, aggregations.cpp_struct(),
                             error_carrier)

            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

            return aggregations

        def erase_ranges(self, intervals):
            """
            Erase points within the specified intervals, left inclusive.

            :param intervals: The intervals for which the ranges should be erased
            :type intervals: A list of (datetime.datetime, datetime.datetime) couples

            :raises: Error
            :returns: The number of erased points
            """
            error_carrier = qdb_convert.make_error_carrier()

            erased_count = self.call_ts_fun(
                impl.ts_erase_ranges,
                qdb_convert.convert_time_couples_to_qdb_ts_range_t_vector(
                    intervals),
                error_carrier)

            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

            return erased_count

    class DoubleColumn(Column):
        """
        A column whose value are double precision floats
        """

        def fast_insert(self, vector):
            """
            Inserts value into the time series.

            :param vector: A vector of double points
            :type vector: quasardb.DoublePointsVector

            :raises: Error
            """
            err = super(TimeSeries.DoubleColumn, self).call_ts_fun(
                impl.ts_double_insert, vector)
            if err != impl.error_ok:
                raise chooseError(err)

        def insert(self, tuples):
            """
            Inserts values into the time series.

            :param tuples: The list of couples to insert into the time series
            :type tuples: A list of (datetime.datetime, float) couples

            :raises: Error
            """
            self.fast_insert(
                qdb_convert.make_qdb_ts_double_point_vector(tuples))

        def get_ranges(self, intervals):
            """
            Returns the ranges matching the provided intervals, left inclusive.

            :param intervals: The intervals for which the ranges should be returned
            :type intervals: A list of (datetime.datetime, datetime.datetime) couples

            :raises: Error
            :returns: A flattened list of (datetime.datetime, float) couples
            """
            error_carrier = qdb_convert.make_error_carrier()

            res = super(TimeSeries.DoubleColumn, self).call_ts_fun(
                impl.ts_double_get_ranges,
                qdb_convert.convert_time_couples_to_qdb_ts_range_t_vector(
                    intervals),
                error_carrier)
            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

            return [(qdb_convert.convert_qdb_timespec_to_time(x.timestamp), x.value) for x in res]

        def aggregate(self, aggregations):  # pylint: disable=W0221
            """
            Aggregates values over the given intervals

            :param aggregations: The aggregations to perform
            :type aggregations: The list of (quasardb.TimeSeries.Aggregation,
                                (datetime.datetime, datetime.datetime)) couples

            :raises: Error
            :returns: The list of aggregation results
            """
            return super(TimeSeries.DoubleColumn, self).aggregate(
                impl.ts_double_aggregation, aggregations)

    class Int64Column(Column):
        """
        A column whose value are signed 64-bit integers
        """

        def fast_insert(self, vector):
            """
            Inserts value into the time series.

            :param vector: A vector of int64 points
            :type vector: quasardb.Int64PointsVector

            :raises: Error
            """
            err = super(TimeSeries.Int64Column, self).call_ts_fun(
                impl.ts_int64_insert, vector)
            if err != impl.error_ok:
                raise chooseError(err)

        def insert(self, tuples):
            """
            Inserts values into the time series.

            :param tuples: The list of couples to insert into the time series
            :type tuples: A list of (datetime.datetime, float) couples

            :raises: Error
            """
            self.fast_insert(
                qdb_convert.make_qdb_ts_int64_point_vector(tuples))

        def get_ranges(self, intervals):
            """
            Returns the ranges matching the provided intervals, left inclusive.

            :param intervals: The intervals for which the ranges should be returned
            :type intervals: A list of (datetime.datetime, datetime.datetime) couples

            :raises: Error
            :returns: A flattened list of (datetime.datetime, float) couples
            """
            error_carrier = qdb_convert.make_error_carrier()

            res = super(TimeSeries.Int64Column, self).call_ts_fun(
                impl.ts_int64_get_ranges,
                qdb_convert.convert_time_couples_to_qdb_ts_range_t_vector(
                    intervals),
                error_carrier)
            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

            return [(qdb_convert.convert_qdb_timespec_to_time(x.timestamp), x.value) for x in res]

    class TimestampColumn(Column):
        """
        A column whose value are nanosecond-precise timestamps
        """

        def fast_insert(self, vector):
            """
            Inserts value into the time series.

            :param vector: A vector of nanosecond-precise timestamps
            :type vector: quasardb.TimestampPointsVector

            :raises: Error
            """
            err = super(TimeSeries.TimestampColumn, self).call_ts_fun(
                impl.ts_timestamp_insert, vector)
            if err != impl.error_ok:
                raise chooseError(err)

        def insert(self, tuples):
            """
            Inserts values into the time series.

            :param tuples: The list of couples to insert into the time series
            :type tuples: A list of (datetime.datetime, datetime.datetime) couples

            :raises: Error
            """
            self.fast_insert(
                qdb_convert.make_qdb_ts_timestamp_point_vector(tuples))

        def get_ranges(self, intervals):
            """
            Returns the ranges matching the provided intervals, left inclusive.

            :param intervals: The intervals for which the ranges should be returned
            :type intervals: A list of (datetime.datetime, datetime.datetime) couples

            :raises: Error
            :returns: A flattened list of (datetime.datetime, datetime.datetime) couples
            """
            error_carrier = qdb_convert.make_error_carrier()

            res = super(TimeSeries.TimestampColumn, self).call_ts_fun(
                impl.ts_timestamp_get_ranges,
                qdb_convert.convert_time_couples_to_qdb_ts_range_t_vector(
                    intervals),
                error_carrier)
            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

            return [(qdb_convert.convert_qdb_timespec_to_time(x.timestamp),
                     qdb_convert.convert_qdb_timespec_to_time(x.value)) for x in res]

    class BlobColumn(Column):
        """
        A column whose values are blobs
        """

        def fast_insert(self, vector):
            """
            Inserts value into the time series.

            :param vector: A vector of blob points
            :type vector: quasardb.BlobPointsVector

            :raises: Error
            """
            err = super(TimeSeries.BlobColumn, self).call_ts_fun(
                impl.ts_blob_insert, vector)
            if err != impl.error_ok:
                raise chooseError(err)

        def insert(self, tuples):
            """
            Inserts values into the time series.

            :param tuples: The list of couples to insert into the time series
            :type tuples: A list of (datetime.datetime, string) couples

            :raises: Error
            """
            self.fast_insert(
                qdb_convert.convert_to_wrap_ts_blop_points_vector(tuples))

        def get_ranges(self, intervals):
            """
            Returns the ranges matching the provided intervals.

            :param intervals: The intervals for which the ranges should be returned
            :type intervals: A list of (datetime.datetime, datetime.datetime) couples

            :raises: Error
            :returns: A flattened list of (datetime.datetime, string) couples
            """
            error_carrier = qdb_convert.make_error_carrier()

            res = super(TimeSeries.BlobColumn, self).call_ts_fun(
                impl.ts_blob_get_ranges,
                qdb_convert.convert_time_couples_to_qdb_ts_range_t_vector(
                    intervals),
                error_carrier)
            if error_carrier.error != impl.error_ok:
                raise chooseError(error_carrier.error)

            return [(qdb_convert.convert_qdb_timespec_to_time(x.timestamp), x.data) for x in res]

        def aggregate(self, aggregations):  # pylint: disable=W0221
            """
            Aggregates values over the given intervals

            :param aggregations: The aggregations to perform
            :type aggregations: A list of (quasardb.TimeSeries.Aggregation,
                                (datetime.datetime, datetime.datetime)) couples

            :raises: Error
            :returns: The list of aggregation results
            """
            return super(TimeSeries.BlobColumn, self).aggregate(
                impl.ts_blob_aggregation, aggregations)

    def __init__(self, handle, alias, *args, **kwargs):
        super(TimeSeries, self).__init__(handle, alias)

    def call_ts_fun(self, ts_func, *args, **kwargs):
        return ts_func(self.handle, super(TimeSeries, self).alias(), *args, **kwargs)

    def create(self, columns,
               shard_size=datetime.timedelta(milliseconds=impl.duration_default_shard_size)):
        """
        Creates a time series with the provided columns information

        :param columns: A list describing the columns to create
        :type columns: a list of TimeSeries.ColumnInfo

        :param shard_size: The length of a single timeseries shard (bucket).
        :type shard_size: datetime.timedelta

        :raises: Error
        :returns: A list of columns matching the created columns
        """
        millis = 1000 * shard_size.total_seconds() + shard_size.microseconds / 1000
        err = self.call_ts_fun(
            impl.ts_create,
            [impl.wrap_ts_column(x.name, x.type) for x in columns],
            long(millis * impl.duration_millisecond))
        if err != impl.error_ok:
            raise chooseError(err)

        return [self.column(x) for x in columns]

    def column(self, col_info):
        """
        Accesses an existing column.

        :param col_info: A description of the column to access
        :type col_info: TimeSeries.ColumnInfo

        :raises: Error
        :returns: A TimeSeries.Column matching the provided information
        """
        if col_info.type == TimeSeries.ColumnType.blob:
            return TimeSeries.BlobColumn(self, col_info.name)

        if col_info.type == TimeSeries.ColumnType.double:
            return TimeSeries.DoubleColumn(self, col_info.name)

        if col_info.type == TimeSeries.ColumnType.int64:
            return TimeSeries.Int64Column(self, col_info.name)

        if col_info.type == TimeSeries.ColumnType.timestamp:
            return TimeSeries.TimestampColumn(self, col_info.name)

        raise chooseError(ErrorCode.invalid_argument)

    def columns(self):
        """
        Returns all existing columns.

        :raises: Error
        :returns: A list of all existing columns as TimeSeries.Column objects
        """
        return [self.column(x) for x in self.columns_info()]

    def columns_info(self):
        """
        Returns all existing columns information.

        :raises: Error
        :returns: A list of all existing columns as TimeSeries.ColumnInfo objects
        """
        error_carrier = qdb_convert.make_error_carrier()
        raw_cols = self.call_ts_fun(impl.ts_list_columns, error_carrier)
        if error_carrier.error != impl.error_ok:
            raise chooseError(error_carrier.error)

        return [TimeSeries.ColumnInfo(x.name, x.type) for x in raw_cols]

    def local_table(self, columns=None):
        """
        Returns a LocalTable matching the provided columns or the full time series if None.

        :param columns: A list of columns to build the LocalTable
        :type columns: A list of quasardb.ColumnInfo

        :raises: Error
        :returns: A list quasardb.LocalTable initialized
        """
        return TimeSeries.LocalTable(self, columns)


class Blob(ExpirableEntry):

    def __init__(self, handle, alias, *args, **kwargs):
        super(Blob, self).__init__(handle, alias)

    def put(self, data, expiry_time=None):
        """ Creates a blob into the repository.
        It is an error to call this method on a blob that already exists.
        Use the :py:meth:`update` method to update a blob.
        If expiry_time is None, the blob never expires.

            :param data: The content for the alias
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: Error
        """
        err = self.handle.blob_put(
            super(Blob, self).alias(), data, qdb_convert.convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise chooseError(err)

    def update(self, data, expiry_time=None):
        """ Updates the given alias.
        If the alias is not found in the repository, the blob is created.
        If expiry_time is None, the blob never expires.

            :param data: The content for the alias
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: Error
        """
        err = self.handle.blob_update(
            super(Blob, self).alias(), data, qdb_convert.convert_expiry_time(expiry_time))
        if not ((err == impl.error_ok) or (err == impl.error_ok_created)):
            raise chooseError(err)

    def get(self):
        """ Gets the data for the given alias.
        It is an error to request a non-existing blob.

            :returns: str -- The associated content
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        buf = impl.blob_get(self.handle, super(Blob, self).alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)

        return qdb_convert.api_buffer_to_string(buf)

    def get_and_update(self, data, expiry_time=None):
        """ Updates the blob and returns the previous value.
        It is an error to call this method on a non-existing blob.
        If expiry_time is None, the blob never expires.

            :param data: The new data to put in place
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :returns: str -- The original content
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        buf = impl.blob_get_and_update(self.handle, super(
            Blob, self).alias(), data, qdb_convert.convert_expiry_time(expiry_time), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)

        return qdb_convert.api_buffer_to_string(buf)

    def get_and_remove(self):
        """ Atomically gets the data from the blob and removes it.
        It is an error to call this method on a non-existing blob.
        If expiry_time is None, the entry never expires.

            :returns: str -- The associated content
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        buf = impl.blob_get_and_remove(
            self.handle, super(Blob, self).alias(), err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)

        return qdb_convert.api_buffer_to_string(buf)

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
            :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        buf = impl.blob_compare_and_swap(self.handle, super(Blob, self).alias(
        ), new_data, comparand, qdb_convert.convert_expiry_time(expiry_time), err)
        if err.error == impl.error_unmatched_content:
            return qdb_convert.api_buffer_to_string(buf)

        if err.error != impl.error_ok:
            raise chooseError(err.error)

        return None

    def remove_if(self, comparand):
        """ Removes the blob from the repository if it matches comparand. The operation is atomic.
            It is an error to attempt to remove a non-existing blob.

            :param comparand: The content to compare to
            :type comparand: str

            :raises: Error
        """
        err = self.handle.blob_remove_if(super(Blob, self).alias(), comparand)
        if err != impl.error_ok:
            raise chooseError(err)


"""
    This class works as a wrapper to a returned cpp_object in query_exp.
    It was needed primarily to reuse the union data type of the qdb_point_result_t.
"""


class QueryExpResult(object):

    """
        This class is needed to combine all different get_payload methods into one
        single get_payload method
    """
    class QueryExpTable(object):
        def __init__(self, table):
            self.captured_cpp_table_object = table
            self.table_name = table.table_name
            self.columns_names = table.columns_names
            self.rows_count = table.rows_count
            self.columns_count = table.columns_count

        def get_payload(self, row_index, col_index):
            result = self.captured_cpp_table_object.get_type(
                row_index, col_index)
            err = result[0]
            payload_type = result[1]
            if err != impl.error_ok:
                return [err, payload_type]
            if payload_type == impl.qdb_query_result_double:
                return self.captured_cpp_table_object.get_payload_double(row_index, col_index)
            if payload_type == impl.qdb_query_result_blob:
                return self.captured_cpp_table_object.get_payload_blob(row_index, col_index)
            if payload_type == impl.qdb_query_result_int64:
                return self.captured_cpp_table_object.get_payload_int64(row_index, col_index)
            if payload_type == impl.qdb_query_result_timestamp:
                return self.captured_cpp_table_object.get_payload_timestamp(row_index, col_index)

    def __init__(self, cpp_object):
        self.captured_cpp_query_result_object = cpp_object
        self.scanned_rows_count = cpp_object.scanned_rows_count
        self.tables_count = cpp_object.tables_count
        self.tables = [self.QueryExpTable(table)
                       for table in cpp_object.tables]

    def __del__(self):
        impl.delete_wrap_qdb_query_result(
            self.captured_cpp_query_result_object)


class Cluster(object):

    def __init__(self, uri, timeout=None,
                 user_name=None, user_private_key=None,
                 cluster_public_key=None,
                 encryption=Encryption.none,
                 *args, **kwargs):  # pylint: disable=W0613
        """
        Creates the raw client.
        If the client is not used for some time,
        the connection is dropped and reestablished at need.

            :param uri: The connection string
            :type uri: str
            :param timeout: The connection timeout
            :type timeout: datetime.timedelta
            :param user_name: An optional user name
            :type user_name: str
            :param user_private_key: An optional user private key
            :type user_private_key: str
            :param cluster_public_key: An optional cluster public key
            :type cluster_public_key: str
            :param encryption: Configure the encryption level with the cluster
            :type encryption: quasardb.Encryption

            :raises: Error
        """
        self.handle = None

        if timeout is None:
            timeout = datetime.timedelta(minutes=1)

        timeout_value = qdb_convert.duration_to_timeout_ms(timeout)
        if timeout_value == 0:
            raise chooseError(impl.error_invalid_argument)

        self.handle = impl.create_handle()

        if not (user_name is None and user_private_key is None and cluster_public_key is None):
            error = self.handle.set_user_credentials(
                str(user_name), str(user_private_key))
            if error != impl.error_ok:
                raise chooseError(error)

            error = self.handle.set_cluster_public_key(str(cluster_public_key))
            if error != impl.error_ok:
                raise chooseError(error)

            error = self.handle.set_encryption(encryption)
            if error != impl.error_ok:
                raise chooseError(error)

        error = impl.connect(self.handle, uri, timeout_value)
        if error != impl.error_ok:
            raise chooseError(error)

    def __del__(self):
        """ On delete, the connection is closed. """
        if self.handle != None:
            self.handle.close()
            self.handle = None

    def set_compression(self, compression_level):
        """
        Sets the compression level for communications with the cluster.

        By default fast compression is enabled, but you may want to disable the compression.

        :param compression_level: The compression level

        :raises: Error
        """
        err = self.handle.set_compression(compression_level)
        if err != impl.error_ok:
            raise chooseError(err)

    def set_max_cardinality(self, max_cardinality):
        """
        Sets the maximum allowed cardinality of a quasardb query.

        The default values is 10,007. The minimum value is 1,000.

        :param max_cardinality: The cardinality value
        :type max_cardinality: long

        :raises: Error
        """
        long_max_cardinality = long(max_cardinality)
        if long_max_cardinality < 0:
            raise chooseError(impl.error_invalid_argument)

        err = self.handle.set_max_cardinality(long_max_cardinality)
        if err != impl.error_ok:
            raise chooseError(err)

    def set_timeout(self, duration):
        """
        Sets the timeout value for requests.
        Requests that take longer than timeout value will raise an exception.

        The minimum timemout value is 1 ms.

        :param duration: The timeout value
        :type duration: datetime.timedelta

        :raises: Error
        """
        timeout_value = qdb_convert.duration_to_timeout_ms(duration)
        if timeout_value == 0:
            raise chooseError(impl.error_invalid_argument)
        err = self.handle.set_timeout(timeout_value)
        if err != impl.error_ok:
            raise chooseError(err)

    def get_timeout(self):
        """
        Gets the timeout value for requests in ms.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        timeout = impl.get_timeout_wrapper(self.handle, err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return timeout

    def blob(self, alias):
        """
        Returns an object representing a blob with the provided alias.
        The blob may or may not exist yet.

        :param alias: The alias of the blob to work on
        :type alias: str

        :returns: The blob named alias
        """
        return Blob(self.handle, alias)

    def integer(self, alias):
        """
        Returns an object representing an integer with the provided alias.
        The blob may or may not exist yet.

        :param alias: The alias of the integer to work on
        :type alias: str

        :returns: The integer named alias
        """
        return Integer(self.handle, alias)

    def deque(self, alias):
        """
        Returns an object representing a deque with the provided alias.
        The deque may or may not exist yet.

        :param alias: The alias of the deque to work on
        :type alias: str

        :returns: The deque named alias
        """
        return Deque(self.handle, alias)

    def ts(self, alias):
        """
        Return an object representing a time series with the provided alias.
        The time series may or may not exist yet.

        :param alias: The alias of the time series set to work on
        :type alias: str

        :returns: The time series named alias
        """
        return TimeSeries(self.handle, alias)

    def tag(self, alias):
        """
        Returns an object representing a tag with the provided alias.
        The tag may or may not exist yet.

        :param alias: The alias of the tag to work on
        :type alias: str

        :returns: The tag named alias
        """
        return Tag(self.handle, alias)

    def purge_all(self, timeout):
        """
        Removes all the entries from all nodes of the cluster.

        :param timeout: Operation timeout
        :type timeout: datetime.timedelta

        :raises: Error

        .. caution::
            This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.purge_all(
            qdb_convert.duration_to_timeout_ms(timeout))
        if err != impl.error_ok:
            raise chooseError(err)

    def trim_all(self, timeout):
        """
        Trims all entries of all nodes of the cluster.

        :param timeout: Operation timeout
        :type timeout: datetime.timedelta

        :raises: Error
        """
        err = self.handle.trim_all(qdb_convert.duration_to_timeout_ms(timeout))
        if err != impl.error_ok:
            raise chooseError(err)

    def stop_node(self, uri, reason):
        """
        Stops a node.

        :param uri: The connection string
        :type uri: str
        :param reason: A string describing the reason for the stop
        :type reason: str

        :raises: Error

        .. caution::
            This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.stop_node(uri, reason)
        if err != impl.error_ok:
            raise chooseError(err)

    def node_config(self, uri):
        """
        Retrieves the configuration of a given node in JSON format.

        :param uri: The connection string
        :type uri: str

        :returns: dict -- The requested configuration

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        res = impl.node_config(self.handle, uri, err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return json.loads(res)

    def node_status(self, uri):
        """
        Retrieves the status of a given node in JSON format.

        :param uri: The connection string
        :type uri: str

        :returns: dict -- The requested status

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        res = impl.node_status(self.handle, uri, err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return json.loads(res)

    def node_topology(self, uri):
        """
        Retrieves the topology of a given node in JSON format.

        :param uri: The connection string
        :type uri: str

        :returns: dict -- The requested topology

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        res = impl.node_topology(self.handle, uri, err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return json.loads(res)

    def query_find(self, q):
        """
        Retrieves all entries' aliases that match the specified query.

        :param q: The query to run
        :type q: str

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        result = impl.run_query_find(self.handle, q, err)

        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result

    def query_exp(self, q):
        """
        Retrieves all entries' aliases that match the specified query expression.
        :param q: the query expression to run
        :type q: str

        :returns: A list of tables which matched the query expression.
        If no match is found, returns an empty list.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        cpp_object = impl.run_query_exp(self.handle, q, err)
        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return QueryExpResult(cpp_object)

    def prefix_get(self, prefix, max_count):
        """
        Retrieves the list of all entries matching the provided prefix.

        :param prefix: The prefix to use for the search
        :type prefix: str

        :param max_count: The maximum number of entries to return
        :type max_count: long

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        result = impl.prefix_get(self.handle, prefix, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result

    def prefix_count(self, prefix):
        """
        Returns the count of all entries matching the provided prefix.

        :param prefix: The prefix to use for the count
        :type prefix: str

        :returns: The count of entries matching the provided prefix. 0 if there is no match.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        count = impl.prefix_count(self.handle, prefix, err)

        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return count

    def suffix_get(self, suffix, max_count):
        """
        Retrieves the list of all entries matching the provided suffix.

        :param suffix: The suffix to use for the search
        :type suffix: str

        :param max_count: The maximum number of entries to return
        :type max_count: long

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: Error

        """
        err = qdb_convert.make_error_carrier()
        result = impl.suffix_get(self.handle, suffix, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result

    def suffix_count(self, suffix):
        """
        Returns the count of all entries matching the provided suffix.

        :param suffix: The suffix to use for the count
        :type suffix: str

        :returns: The count of entries matching the provided suffix. 0 if there is no match.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        count = impl.suffix_count(self.handle, suffix, err)

        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return count

    def blob_scan(self, pattern, max_count):
        """
        Scans all blobs and returns the list of entries
        whose content matches the provided sub-string.

        :param pattern: The substring to look for
        :type pattern: str

        :param max_count: The maximum number of entries to return
        :type max_count: long

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        result = impl.blob_scan(self.handle, pattern, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result

    def blob_scan_regex(self, pattern, max_count):
        """
        Scans all blobs and returns the list of entries whose content that match the provided
        regular expression (ECMA-262).

        :param pattern: The regular expression to use for matching
        :type pattern: str

        :param max_count: The maximum number of entries to return
        :type max_count: long

        :returns: The list of matching aliases. If no match is found, returns an empty list.

        :raises: Error
        """
        err = qdb_convert.make_error_carrier()
        result = impl.blob_scan_regex(self.handle, pattern, max_count, err)
        if err.error == impl.error_alias_not_found:
            return []

        if err.error != impl.error_ok:
            raise chooseError(err.error)
        return result
