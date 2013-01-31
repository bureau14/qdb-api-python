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

import qdb.impl as impl
import cPickle as pickle

""" Exceptions """
class QuasardbException(Exception)            : pass
class System(QuasardbException)               : pass
class Internal(QuasardbException)             : pass
class NoMemory(QuasardbException)             : pass
class InvalidProtocol(QuasardbException)      : pass
class HostNotFound(QuasardbException)         : pass
class InvalidOption(QuasardbException)        : pass
class AliasNotFound(QuasardbException)        : pass
class AliasTooLong(QuasardbException)         : pass
class AliasAlreadyExists(QuasardbException)   : pass
class Timeout(QuasardbException)              : pass
class BufferTooSmall(QuasardbException)       : pass
class InvalidCommand(QuasardbException)       : pass
class InvalidInput(QuasardbException)         : pass
class ConnectionRefused(QuasardbException)    : pass
class ConnectionReset(QuasardbException)      : pass
class UnexpectedReply(QuasardbException)      : pass
class NotImplemented(QuasardbException)       : pass
class UnstableHive(QuasardbException)         : pass
class ProtocolError(QuasardbException)        : pass
class OutdatedTopology(QuasardbException)     : pass
class WrongPeer(QuasardbException)            : pass
class InvalidVersion(QuasardbException)       : pass
class TryAgain(QuasardbException)             : pass
class InvalidArgument(QuasardbException)      : pass
class OutOfBounds(QuasardbException)          : pass
class Conflict(QuasardbException)             : pass
class NotConnected(QuasardbException)         : pass
class InvalidHandle(QuasardbException)        : pass

""" Internal - map qdb error codes to python exceptions"""
_errcode_to_exc = {
    impl.error_ok                    : None               ,
    impl.error_system                : System             ,
    impl.error_internal              : Internal           ,
    impl.error_no_memory             : NoMemory           ,
    impl.error_invalid_protocol      : InvalidProtocol    ,
    impl.error_host_not_found        : HostNotFound       ,
    impl.error_invalid_option        : InvalidOption      ,
    impl.error_alias_too_long        : AliasTooLong       ,
    impl.error_alias_not_found       : AliasNotFound      ,
    impl.error_alias_already_exists  : AliasAlreadyExists ,
    impl.error_timeout               : Timeout            ,
    impl.error_buffer_too_small      : BufferTooSmall     ,
    impl.error_invalid_command       : InvalidCommand     ,
    impl.error_invalid_input         : InvalidInput       ,
    impl.error_connection_refused    : ConnectionRefused  ,
    impl.error_connection_reset      : ConnectionReset    ,
    impl.error_unexpected_reply      : UnexpectedReply    ,
    impl.error_not_implemented       : NotImplemented     ,
    impl.error_unstable_hive         : UnstableHive       ,
    impl.error_protocol_error        : ProtocolError      ,
    impl.error_outdated_topology     : OutdatedTopology   ,
    impl.error_wrong_peer            : WrongPeer          ,
    impl.error_invalid_version       : InvalidVersion     ,
    impl.error_try_again             : TryAgain           ,
    impl.error_invalid_argument      : InvalidArgument    ,
    impl.error_out_of_bounds         : OutOfBounds        ,
    impl.error_conflict              : Conflict           ,
    impl.error_not_connected         : NotConnected       ,
    impl.error_invalid_handle        : InvalidHandle
}

def _safe_alias_output(alias):
    """ Remove EOL and crop alias to 128 chars for output in exceptions.
    """
    return str(alias)[:128].replace('\n', ' ').replace('\r', ' ')

def version():
    """ Returns the API's version number as a string
    """
    return impl.version()

def build():
    """ Returns the build tag and build date as a string
    """
    return impl.build()

def api_buffer_to_string(buf):
    return str(buf.data())[:buf.size()]

class RawClient(object):
    """ The raw client takes strings as arguments for both alias and data.
    If you want to put and get other objects, use the qdb.Client instead.
    """
    def __init__(self, hostname, port=2836, *args, **kwargs):
        """ Creates the raw client.
        Connection is delayed until the client is actually used.
        If the client is not used for some time, the connection is dropped and reestablished at need.
        Arguments:
            - hostname: The hostname, either a DNS name, an IPv4 or an IPv6 adress (e.g. "127.0.0.1", "::1", "myserver.mydomain")
            - port: The port, defaults to 2836
        """
        err = impl.error_carrier()
        err.error = impl.error_ok
        self.handle = impl.connect(hostname, port, err)
        if err.error != impl.error_ok:
            raise _errcode_to_exc[err.error]('connecting to %s:%s' % (hostname, port))

    def __del__(self):
        """ On delete, the connection is closed.
        """
        self.handle.close()
        self.handle = None

    def put(self, alias, data):
        """ Put a piece of data in the repository.
        If the alias is already in the repository, this method raises a qdb.AliasAlreadyExists exception.
        Use the update() method to update an alias.
        Arguments:
            - alias: str, Alias of the data.
            - data: str, The data.
        """
        err = self.handle.put(alias, data)
        if err: raise _errcode_to_exc[err]('putting "%s"' % _safe_alias_output(alias))

    def get(self, alias):
        """ Get the data for the given alias.
        If the alias is not found in the repository, this method raises a qdb.AliasNotFound exception.
        Arguments:
            - alias: str, The alias.
        Returns:
            The data
        """
        err = impl.error_carrier()
        err.error = impl.error_ok
        buf = impl.get(self.handle, alias, err)
        if err.error != impl.error_ok:
            raise _errcode_to_exc[err.error]('getting "%s"' % _safe_alias_output(alias))

        return api_buffer_to_string(buf)

    def update(self, alias, data):
        """ Update the given alias
        If the alias is not found in the repository, this method raises a qdb.AliasNotFound exception.
        Arguments:
            - alias: str, Alias of the data.
            - data: str, The data.
        """
        err = self.handle.update(alias, data)
        if err: raise _errcode_to_exc[err]('updating "%s"' % _safe_alias_output(alias))

    def get_update(self, alias, data):
        """ Update the given alias and return the previous value
        If the alias is not found in the repository, this method raises a qdb.AliasNotFound exception.
        Arguments:
            - alias: str, Alias of the data.
            - data: str, The data.
        """
        err = impl.error_carrier()
        err.error = impl.error_ok
        buf = impl.get_update(self.handle, alias, data, err)
        if err.error != impl.error_ok:
            raise _errcode_to_exc[err.error]('getting and updating "%s"' % _safe_alias_output(alias))

        return api_buffer_to_string(buf)

    def compare_and_swap(self, alias, new_data, comparand):
        """ Compare the alias with comparand and replace it with new_data if it matches
        Arguments:
            - alias: str, Alias of the data.
            - new_data: str, the new data to use in case of match
            - comparand: str, the data to compare the existing entry with
        """
        err = impl.error_carrier()
        err.error = impl.error_ok
        buf = impl.compare_and_swap(self.handle, alias, new_data, comparand, err)
        if err.error != impl.error_ok:
            raise _errcode_to_exc[err.error]('getting and updating "%s"' % _safe_alias_output(alias))

        return api_buffer_to_string(buf)

    def remove(self, alias):
        """ Remove the given alias from the repository.
        If the alias is not found in the repository, this method raises a qdb.AliasNotFound exception.
        Arguments:
            - alias: str, The alias
        """
        err = self.handle.remove(alias)
        if err: raise _errcode_to_exc[err]('removing "%s"' % _safe_alias_output(alias))

    def remove_all(self):
        """ Remove all the entries from the repository.
        """
        err = self.handle.remove_all()
        if err: raise _errcode_to_exc[err]('removing all "%s"' % _safe_alias_output(alias))

class Client(RawClient):
    """ The client offers the same interface as the RawClient
    but accepts any kind of object as alias an data,
    provided the object can be marshalled using the cPickle package.
    """
    def put(self, alias, data):
        return super(Client, self).put(pickle.dumps(alias), pickle.dumps(data))

    def get(self, alias):
        return pickle.loads(super(Client, self).get(pickle.dumps(alias)))

    def get_update(self, alias, data):
        return pickle.loads(super(Client, self).update(pickle.dumps(alias), pickle.dumps(data)))

    def compare_and_swap(self, alias, new_value, comparand):
        return pickle.loads(super(Client, self).compare_and_swap(pickle.dumps(alias), pickle.dumps(new_value), pickle.dumps(comparand)))

    def update(self, alias, data):
        return super(Client, self).update(pickle.dumps(alias), pickle.dumps(data))

    def remove(self, alias):
        return super(Client, self).remove(pickle.dumps(alias))
