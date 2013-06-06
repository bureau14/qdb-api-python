#
# Copyright (c) 2009-2013, Bureau 14 SARL
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
#    * Neither the name of Bureau 14 nor the names of its contributors may
#      be used to endorse or promote products derived from this software
#      without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
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

.. moduleauthor: Bureau 14 SARL, All rights reserved
"""

import qdb.impl as impl
import cPickle as pickle
import json

def make_error_string(error_code):
    """ Returns a meaningful error message corresponding to the quasardb error code.

    :param error_code: The error code to translate
    :returns: str -- An error string
    """
    return impl.make_error_string(error_code)

class QuasardbException(Exception):
    """The quasardb exception, based on the API error codes."""
    def __init__(self, error_code):
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

def api_buffer_to_string(buf):
    return str(buf.data())[:buf.size()]

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

        return (impl.get_iterator_key(self.__qdb_iter), api_buffer_to_string(impl.get_iterator_value(self.__qdb_iter)))

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

    def put(self, alias, data):
        """ Puts a piece of data in the repository.
        It is an error to call this method on an entry that already exists. Use the :py:meth:`update` method to update an alias.

            :param alias: The alias to update
            :type alias: str
            :param data: The content for the alias
            :type data: str

            :raises: QuasardbException
        """
        err = self.handle.put(alias, data)
        if err != impl.error_ok:
            raise QuasardbException(err)

    def update(self, alias, data):
        """ Updates the given alias.
        If the alias is not found in the repository, the entry is created.

            :param alias: The alias to update
            :type alias: str
            :param data: The content for the alias
            :type data: str

            :raises: QuasardbException
        """
        err = self.handle.update(alias, data)
        if err != impl.error_ok:
            raise QuasardbException(err)

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

        return api_buffer_to_string(buf)

    def get_update(self, alias, data):
        """ Updates the given alias and returns the previous value.
        It is an error to call this method on a non-existing alias.

            :param alias: The alias to get
            :type alias: str
            :param data: The new data to put in place
            :type data: str

            :returns: str -- The original content
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        buf = impl.get_update(self.handle, alias, data, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return api_buffer_to_string(buf)

    def get_remove(self, alias):
        """ Atomically gets the data from the given alias and removes it.
        It is an error to call this method on a non-existing alias.

            :param alias: The alias to get
            :type alias: str

            :returns: str -- The associated content
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        buf = impl.get_remove(self.handle, alias, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return api_buffer_to_string(buf)

    def compare_and_swap(self, alias, new_data, comparand):
        """ Atomically compares the alias with comparand and replaces it with new_data if it matches.

            :param alias: The alias to compare to
            :type alias: str
            :param new_data: The new content to put in place if the comparand matches
            :type new_data: str
            :param comparand: The content to compare to
            :type comparand: str

            :returns: str -- The original content
            :raises: QuasardbException
        """
        err = self.__make_error_carrier()
        buf = impl.compare_and_swap(self.handle, alias, new_data, comparand, err)
        if err.error != impl.error_ok:
            raise QuasardbException(err.error)

        return api_buffer_to_string(buf)

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

    def remove_all(self):
        """ Removes all the entries from all nodes of the cluster.

            :raises: QuasardbException

            .. caution::
                This method is intended for very specific usage scenarii. Use at your own risks.
        """
        err = self.handle.remove_all()
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

    def put(self, alias, data):
        return super(Client, self).put(pickle.dumps(alias), pickle.dumps(data))

    def get(self, alias):
        return pickle.loads(super(Client, self).get(pickle.dumps(alias)))

    def get_remove(self, alias):
        return pickle.loads(super(Client, self).get_remove(pickle.dumps(alias)))

    def get_update(self, alias, data):
        return pickle.loads(super(Client, self).get_update(pickle.dumps(alias), pickle.dumps(data)))

    def compare_and_swap(self, alias, new_value, comparand):
        return pickle.loads(super(Client, self).compare_and_swap(pickle.dumps(alias), pickle.dumps(new_value), pickle.dumps(comparand)))

    def update(self, alias, data):
        return super(Client, self).update(pickle.dumps(alias), pickle.dumps(data))

    def remove(self, alias):
        return super(Client, self).remove(pickle.dumps(alias))

    def remove_if(self, alias, comparand):
        return super(Client, self).remove_if(pickle.dumps(alias), pickle.dumps(comparand))
