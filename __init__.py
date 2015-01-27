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

class BatchRequest:
    """A request within a batch run."""
    def __init__(self, op_type, alias = None, content = None, comparand = None, expiry = None):
        """
        Initializes a BatchRequest

        :param op_type: The type of operation, from qdb.Operation
        :type op_type: int
        :param alias: The alias on which to operate, if it applies
        :type alias: str
        :param content: The content for the request, if it applies
        :type content: str
        :param comparand: The comparand for the request, if it applies
        :type comparand: str
        :param expiry: The expiry for the request, if it applies
        :type expiry: datetime.datetime
        """
        self.type = op_type
        self.alias = alias
        self.content = content
        self.comparand = comparand
        self.expiry = expiry

    def pickle(self):
        """
        "Pickles" the content and comparand members and returns a copy of the resulting object

        :returns: a copy of self with content and comparand "pickled"
        """
        br = BatchRequest(self.type, self.alias)
        br.content = None if self.content == None else pickle.dumps(self.content)
        br.comparand = None if self.comparand == None else pickle.dumps(self.comparand)
        br.expiry = self.expiry
        return br

    def cpp_type(self, handle):
        """
        Converts the BatchRequest into a qdb.impl.batch_request, a low level structure used for calls to the underlying C++ API.
        :param handle: The qdb handle used for internal allocation

        :returns: qdb.impl.batch_request - A converted copy of self
        """

        # _string_to_api_buffer is safe when the parameter is None
        content_buf = _string_to_api_buffer(handle, self.content)
        comparand_buf = _string_to_api_buffer(handle, self.comparand)
        expiry = _convert_expiry_time(self.expiry)

        return impl.batch_request(self.type, self.alias, content_buf, comparand_buf, expiry)

class BatchResult:
    """The result from a batch run."""
    def __init__(self, br):
        assert(isinstance(br, impl.batch_result))

        self.type = br.type
        self.error = br.error
        self.alias = br.alias
        self.result = _api_buffer_to_string(br.result)

    def unpickle(self):
        """
        "Unpickles" the result member and returns a copy of the resulting object.

        :returns: a copy of self with result "unpickled"
        """
        br = copy.copy(self)
        br.result = None if self.result == None else pickle.loads(self.result)
        return br

class RemoteNode:
    """ Convenience wrapper for the low level qdb.impl.qdb_remote_node_t structure"""
    def __init__(self, address, port = 2836):
        """Create a RemoteNode object.
        :param address: The address of the remote node (IP address or qualified name)
        :type address: str
        :param port: The port number of the remote node to connect to
        :type port: int
        """
        self.address = address
        self.port = port
        self.error = impl.error_uninitialized

    def __str__(self):
        return "quasardb remote node: " + self.address + ":" + str(self.port) + " - error status:" + make_error_string(self.error)

    def c_type(self):
        """
        Converts the RemoteNode into a qdb.impl.qdb_remote_node_t, a low-level structure used for calls to the underlying C API.
        :returns: qdb.impl.qdb_remote_node_t -- A converted copy of self
        """
        res = impl.qdb_remote_node_t()

        res.address = self.address
        res.port = self.port
        res.error = self.error

        return res

class RawForwardIterator(object):
    """
    A forward iterator that can be used to iterate on a whole cluster
    """
    def __init__(self, qdb_iter):
        self.__qdb_iter = qdb_iter

    def __iter__(self):
        return self

    def __del__(self):
        self.__qdb_iter.close()
        self.__qdb_iter = None

    def next(self):
        # next is safe to call even when we are at the end or beyond
        impl.iterator_next(self.__qdb_iter)

        if self.__qdb_iter.last_error() != impl.error_ok:
            raise StopIteration()

        return (impl.get_iterator_key(self.__qdb_iter), _api_buffer_to_string(impl.get_iterator_value(self.__qdb_iter)))

class RawClient(object):
    """ The raw client takes strings as arguments for both alias and data.
    If you want to put and get other objects, use :py:class:`Client` instead.
    """

    def __make_error_carrier(self):
        err = impl.error_carrier()
        err.error = impl.error_uninitialized
        return err

    def __init__(self, remote_node, *args, **kwargs):
        """ Creates the raw client.
        Connection is delayed until the client is actually used.
        If the client is not used for some time, the connection is dropped and reestablished at need.

            :param remote_node: The remote node to connect to
            :type remote_node: qdb.RemoteNode

            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        self.handle = None
        self.handle = impl.connect(remote_node.c_type(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

    def __del__(self):
        """ On delete, the connection is closed.
        """
        if self.handle != None:
            self.handle.close()
            self.handle = None

    def __iter__(self):
        """ Returns a forward iterator to iterate on all the cluster's entries
        """
        return RawForwardIterator(self.handle.begin())

    def put(self, alias, data, expiry_time=None):
        """ Puts a piece of data in the repository.
        It is an error to call this method on an entry that already exists. Use the :py:meth:`update` method to update an alias. If expiry_time is None, the entry never expires.

            :param alias: The alias to update
            :type alias: str
            :param data: The content for the alias
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        err = self.handle.put(alias, data, _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def update(self, alias, data, expiry_time=None):
        """ Updates the given alias.
        If the alias is not found in the repository, the entry is created. 
        If expiry_time is None, the entry never expires.

            :param alias: The alias to update
            :type alias: str
            :param data: The content for the alias
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        err = self.handle.update(alias, data, _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def prefix_get(self, prefix):
        """ Returns the list of entries whose alias start with the given prefix. If no alias matches the given prefix,
            the function returns an empty list and raises no error.

            :param prefix: The prefix to use
            :type prefix: str

            :returns: a list of str -- The list of matching aliases
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        result = impl.prefix_get(self.handle, prefix, err)
        if err.error != impl.error_ok and err.error != impl.error_alias_not_found:
            raise QuasardbException(err.error)

        return result

    def run_batch(self, requests):
        """
        Runs the provided requests (a collection of BatchRequest) and returns a collection of BatchResult.

        :param requests: The requests to run
        :type requests: a list of BatchRequest

        :returns: a list of BatchRequest -- The list of results
        :raises: QuasardbException
        """
        # transform the list of BatchRequest to a list impl.batch_request
        # we need to convert the list of impl.batch_result to a list of BatchResult
        br = impl.run_batch(self.handle, map(lambda x: x.cpp_type(self.handle), requests))
        return (br.successes, map(lambda x: BatchResult(x), br.results))

    def get(self, alias):
        """ Gets the data for the given alias.
        It is an error to request a non-existing entry.

            :param alias: The alias to get
            :type alias: str

            :returns: str -- The associated content
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        buf = impl.get(self.handle, alias, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def get_update(self, alias, data, expiry_time=None):
        """ Updates the given alias and returns the previous value.
        It is an error to call this method on a non-existing alias. 
        If expiry_time is None, the entry never expires.

            :param alias: The alias to get
            :type alias: str
            :param data: The new data to put in place
            :type data: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :returns: str -- The original content
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        buf = impl.get_update(self.handle, alias, data, _convert_expiry_time(expiry_time), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def get_remove(self, alias):
        """ Atomically gets the data from the given alias and removes it.
        It is an error to call this method on a non-existing alias.
        If expiry_time is None, the entry never expires.

            :param alias: The alias to get
            :type alias: str

            :returns: str -- The associated content
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        buf = impl.get_remove(self.handle, alias, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def compare_and_swap(self, alias, new_data, comparand, expiry_time=None):
        """ Atomically compares the alias with comparand and replaces it with new_data if it matches.
            If expiry_time is None, the entry never expires.

            :param alias: The alias to compare to
            :type alias: str
            :param new_data: The new content to put in place if the comparand matches
            :type new_data: str
            :param comparand: The content to compare to
            :type comparand: str
            :param expiry_time: The expiry time for the alias
            :type expiry_time: datetime.datetime

            :returns: str -- The original content
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        buf = impl.compare_and_swap(self.handle, alias, new_data, comparand, _convert_expiry_time(expiry_time), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return _api_buffer_to_string(buf)

    def remove(self, alias):
        """ Removes the given alias from the repository. It is an error to remove a non-existing alias.

            :param alias: The alias to remove
            :type alias: str

            :raises: QuasardbException
        """
        err = self.handle.remove(alias)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def remove_if(self, alias, comparand):
        """ Removes the given alias from the repository if it matches comparand. The operation is atomic. It is an error to attempt to remove a non-existing alias.

            :param alias: The alias to remove
            :type alias: str
            :param comparand: The content to compare to
            :type comparand: str

            :raises: QuasardbException
        """
        err = self.handle.remove_if(alias, comparand)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def expires_at(self, alias, expiry_time):
        """
            Sets the expiry time of an existing entry. If the value is None, the entry never expires.

            :param alias: The alias for which the expiry time will be set
            :type alias: str
            :param expiry_time: The expiry time, must be offset aware
            :type expiry_time: datetime.datetime

            :raises: QuasardbException
        """
        err = self.handle.expires_at(alias, _convert_expiry_time(expiry_time))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def expires_from_now(self, alias, expiry_delta):
        """
            Sets the expiry time of an existing entry relative to the current time, in seconds.

            :param alias: The alias for which the expiry time will be set
            :type alias: str
            :param expiry_delta: The expiry delta in seconds
            :type expiry_delta: long

            :raises: QuasardbException
        """
        err = self.handle.expires_from_now(alias, long(expiry_delta))
        if err != impl.error_ok:
            raise QuasardbException(err)

    def get_expiry_time(self, alias):
        """
            Returns the expiry time of an existing entry.

            :param alias: The alias to get the expiry time from
            :type alias: str

            :returns: datetime.datetime -- The expiry time, offset aware
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        t = impl.get_expiry_time_wrapper(self.handle, alias, err)

        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return datetime.datetime.fromtimestamp(t, tz)

    def purge_all(self):
        """ Removes all the entries from all nodes of the cluster.

            :raises: QuasardbException

            .. caution::
                This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.purge_all()
        if err != impl.error_ok:
            raise QuasardbException(err)

    def stop_node(self, remote_node, reason):
        """ Stops a node.

            :param remote_node: The node to stop
            :type remote_node: qdb.RemoteNode
            :param reason: A string describing the reason for the stop
            :type reason: str

            :raises: QuasardbException

            .. caution::
                This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.stop_node(remote_node.c_type(), reason)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def node_config(self, remote_node):
        """ Retrieves the configuration of a given node in JSON format.

            :param remote_node: The node to obtain the configuration from.
            :type remote_node: qdb.RemoteNode

            :returns: dict -- The requested configuration

            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        res = impl.node_config(self.handle, remote_node.c_type(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return json.loads(res)

    def node_status(self, remote_node):
        """ Retrieves the status of a given node in JSON format.

            :param remote_node: The node to obtain the status from.
            :type remote_node: qdb.RemoteNode

            :returns: dict -- The requested status

            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        res = impl.node_status(self.handle, remote_node.c_type(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return json.loads(res)

    def node_topology(self, remote_node):
        """ Retrieves the topology of a given node in JSON format.

            :param remote_node: The node to obtain the topology from.
            :type remote_node: qdb.RemoteNode

            :returns: dict -- The requested topology

            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        res = impl.node_topology(self.handle, remote_node.c_type(), err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)
        return json.loads(res)

class ForwardIterator(RawForwardIterator):
    """
    A forward iterator that can be used to iterate on a whole cluster with a pickle interface
    """
    def next(self):
        (k, v) = super(ForwardIterator, self).next()
        return (pickle.loads(k), pickle.loads(v))


class Client(RawClient):
    """ The client offers the same interface as the RawClient
    but accepts any kind of object as alias and data,
    provided that the object can be marshalled with the cPickle package.
    """
    def __iter__(self):
        """ Returns a forward iterator to iterate on all the cluster's entries
        """
        return ForwardIterator(self.handle.begin())

    def run_batch(self, requests):
        # we need to translate the requests buffers
        successes, res = super(Client, self).run_batch(map(lambda x: x.pickle(), requests))
        return (successes, map(lambda x: x.unpickle(), res))

    def put(self, alias, data, expiry_time=None):
        return super(Client, self).put(alias, pickle.dumps(data), expiry_time)

    def get(self, alias):
        return pickle.loads(super(Client, self).get(alias))

    def get_remove(self, alias):
        return pickle.loads(super(Client, self).get_remove(alias))

    def get_update(self, alias, data, expiry_time=None):
        return pickle.loads(super(Client, self).get_update(alias, pickle.dumps(data), expiry_time))

    def compare_and_swap(self, alias, new_value, comparand, expiry_time=None):
        return pickle.loads(super(Client, self).compare_and_swap(alias, pickle.dumps(new_value), pickle.dumps(comparand), expiry_time))

    def update(self, alias, data, expiry_time=None):
        return super(Client, self).update(alias, pickle.dumps(data), expiry_time)

    def remove(self, alias):
        return super(Client, self).remove(alias)

    def remove_if(self, alias, comparand):
        return super(Client, self).remove_if(alias, pickle.dumps(comparand))

    def expires_at(self, alias, expiry_time):
        return super(Client, self).expires_at(alias, expiry_time)

    def expires_from_now(self, alias, expiry_delta):
        return super(Client, self).expires_from_now(alias, expiry_delta)

    def get_expiry_time(self, alias):
        return super(Client, self).get_expiry_time(alias)
