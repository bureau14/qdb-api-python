#
# Copyright (c) 2009-2015, quasardb SAS
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
.. module: qdb
    :platform: Unix, Windows
    :synopsis: quasardb official Python API

.. moduleauthor: quasardb SAS. All rights reserved
"""

import qdb.impl as impl
import cPickle as pickle
import json
import datetime
import calendar
import copy

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

def _api_buffer_to_string(buf):
    return None if buf is None else str(buf.data())[:buf.size()]

def _string_to_api_buffer(h, str):
    """
    Converts a string to an internal qdb::api_buffer_ptr
    """
    return None if str is None else impl.make_api_buffer_ptr_from_string(h, str)

class TimeZone(datetime.tzinfo):
    """The quasardb time zone is UTC. Please refer to the documentation for further information."""

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)

tz = TimeZone()

def _convert_expiry_time(expiry_time):
    return long(calendar.timegm(expiry_time.timetuple())) if expiry_time != None else long(0)

def make_error_carrier():
    err = impl.error_carrier()
    err.error = impl.error_uninitialized
    return err

class Element(object):

    def __init__(self, handle, alias):
        self.handle = handle
        self.__alias = alias

    def alias(self):
        """
        """
        return self.__alias

    def remove(self):
        """ Removes the given element from the repository. It is an error to remove a non-existing element.

            :raises: QuasardbException
        """
        err = self.handle.remove(self.alias())
        if err != impl.error_ok:
            raise QuasardbException(err)

class ExpirableElement(Element):

    def __init__(self, handle, alias, *args, **kwargs):
        super(ExpirableElement, self).__init__(handle, alias)

    def expires_at(self, expiry_time):
        """
            Sets the expiry time of an existing element. If the value is None, the element never expires.

            :param expiry_time: The expiry time, must be offset aware
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        err = self.handle.expires_at(super(Blob, self).alias(), _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def expires_from_now(self, expiry_delta):
        """
            Sets the expiry time of an existing element relative to the current time, in seconds.

            :param expiry_delta: The expiry delta in seconds
            :type expiry_delta: long

            :raises: QuasardbException
        """
        err = self.handle.expires_from_now(super(Blob, self).alias(), long(expiry_delta))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def get_expiry_time(self):
        """
            Returns the expiry time of the element.

            :returns: datetime.datetime -- The expiry time, offset aware
            :raises: QuasardbException
        """
        err = make_error_carrier()
        t = impl.get_expiry_time_wrapper(self.handle, super(Blob, self).alias(), err)

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return datetime.datetime.fromtimestamp(t, tz)

class Integer(ExpirableElement):
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
        if err != impl.error_ok:
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

class Queue(Element):
    """
    An unlimited, distributed, concurrent queue.
    """
    def __init__(self, handle, alias, *args, **kwargs):
        super(Queue, self).__init__(handle, alias)

    def push_front(self, data):
        """
            Appends a new element at the beginning of the queue. The queue will be created if it does not exist.

            :param data: The content for the element
            :type data: str

            :raises: QuasardbException
        """
        err = self.handle.queue_push_front(super(Queue, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def push_back(self, data):
        """
            Appends a new element at the end of the queue. The queue will be created if it does not exist.

            :param data: The content for the element
            :type data: str

            :raises: QuasardbException
        """
        err = self.handle.queue_push_back(super(Queue, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def __queue_getter(self, f):
        err = make_error_carrier()
        buf = f(self.handle, super(Queue, self).alias(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return _api_buffer_to_string(buf)

    def pop_front(self):
        """
            Atomically returns and remove the first element of the queue. The queue must exist and must not be empty.

            :raises: QuasardbException
            :returns: The first element of the queue
        """
        return self.__queue_getter(impl.queue_pop_front)

    def pop_back(self):
        """
            Atomically returns and remove the last element of the queue. The queue must exist and must not be empty.

            :raises: QuasardbException
            :returns: The last element of the queue
        """
        return self.__queue_getter(impl.queue_pop_back)

    def front(self):
        """
            Returns the first element of the queue. The queue must exist and must not be empty.

            :raises: QuasardbException
            :returns: The first element of the queue
        """
        return self.__queue_getter(impl.queue_front)

    def back(self):
        """
            Returns the last element of the queue. The queue must exist and must not be empty.

            :raises: QuasardbException
            :returns: The last element of the queue
        """
        return self.__queue_getter(impl.queue_back)

    def size(self):
        """
            Returns the current size of the queue.

            :raises: QuasardbException
            :returns: The current size of the queue.
        """
        raise QuasardbException(impl.error_not_implemented)

class HSet(Element):
    """
    An unlimited, distributed, concurrent hash set.
    """
    def __init__(self, handle, alias, *args, **kwargs):
        super(HSet, self).__init__(handle, alias)

    def insert(self, data):
        """
            Inserts a new element into the hash set. If the hash set does not exist, it will be created.

            :raises: QuasardbException
        """
        err = self.handle.hset_insert(super(HSet, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def erase(self, data):
        """
            Erases an existing an element from an existing hash set.

            :raises: QuasardbException
        """
        err = self.handle.hset_erase(super(HSet, self).alias(), data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def contains(self, data):
        """
            Tests if the element exists in the hash set. The hash set must exist.

            :raises: QuasardbException
            :returns: True if the element exists, false otherwise
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

class Blob(ExpirableElement):

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
        err = self.handle.put(super(Blob, self).alias(), data, _convert_expiry_time(expiry_time))
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
        err = self.handle.update(super(Blob, self).alias(), data, _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def get(self):
        """ Gets the data for the given alias.
        It is an error to request a non-existing blob.

            :returns: str -- The associated content
            :raises: QuasardbException
        """
        err = make_error_carrier()
        buf = impl.get(self.handle, super(Blob, self).alias(), err)
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
        buf = impl.get_and_update(self.handle, super(Blob, self).alias(), data, _convert_expiry_time(expiry_time), err)
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
        buf = impl.get_and_remove(self.handle, super(Blob, self).alias(), err)
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

            :returns: str -- The original content
            :raises: QuasardbException
        """
        err = make_error_carrier()
        buf = impl.compare_and_swap(self.handle, super(Blob, self).alias(), new_data, comparand, _convert_expiry_time(expiry_time), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def remove_if(self, comparand):
        """ Removes the blob from the repository if it matches comparand. The operation is atomic.
            It is an error to attempt to remove a non-existing blob.

            :param comparand: The content to compare to
            :type comparand: str

            :raises: QuasardbException
        """
        err = self.handle.remove_if(super(Blob, self).alias(), comparand)
        if err != impl.error_ok:
            raise QuasardbException(err)

class Cluster(object):
    """
    """

    def __init__(self, uri, *args, **kwargs):
        """ Creates the raw client.
        Connection is delayed until the client is actually used.
        If the client is not used for some time, the connection is dropped and reestablished at need.

            :param uri: The connection string
            :type uri: str

            :raises: QuasardbException
        """
        err = make_error_carrier()
        self.handle = None
        self.handle = impl.connect(uri, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

    def __del__(self):
        """ On delete, the connection is closed.
        """
        if self.handle != None:
            self.handle.close()
            self.handle = None

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

    def queue(self, alias):
        """
        Returns an object representing a queue with the provided alias. The queue may or may not exist yet.

        :param alias: The alias of the queue to work on
        :type alias: str

        :returns: The queue named alias
        """
        return Queue(self.handle, alias)

    def hset(self, alias):
        """
        Returns an object representing a hash set with the provided alias. The hash set may or may not exist yet.

        :param alias: The alias of the hash set to work on
        :type alias: str

        :returns: The hash set named alias
        """
        return HSet(self.handle, alias)

    def prefix_get(self, prefix):
        """ Returns the list of entries whose alias start with the given prefix. If no alias matches the given prefix,
            the function returns an empty list and raises no error.

            :param prefix: The prefix to use
            :type prefix: str

            :returns: a list of str -- The list of matching aliases
            :raises: QuasardbException
        """
        err = make_error_carrier()
        result = impl.prefix_get(self.handle, prefix, err)
        if err.error != impl.error_ok and err.error != impl.error_alias_not_found:
            raise QuasardbException(err.error)

        return result

    def purge_all(self):
        """ Removes all the entries from all nodes of the cluster.

            :raises: QuasardbException

            .. caution::
                This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.purge_all()
        if err != impl.error_ok:
            raise QuasardbException(err)

    def trim_all(self):
        """ Trims all entries of all nodes of the cluster.

            :raises: QuasardbException
        """
        err = self.handle.trim_all()
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
