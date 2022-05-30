# pylint: disable=C0103,C0111,C0302,R0903

# Copyright (c) 2009-2022, quasardb SAS. All rights reserved.
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

def generic_error_msg(msg, e):
    msg_str = "\n".join(msg)
    return """
**************************************************************************

{}

**************************************************************************

Original exception:

   Type:    {}
   Message: {}

**************************************************************************
""".format(msg_str, type(e), str(e))

def link_error_msg(e):
    msg = [
        "QuasarDB was unable to find all expected symbols in the compiled library.",
        "This is usually caused by running an incorrect version of the QuasarDB C",
        "API (libqdb_api) along the QuasarDB Python API (this module).",
        "",
        "Please ensure you do not have multiple versions of libqdb_api installed.",
        "If you believe this to be a bug, please reach out to QuasarDB at",
        "support@quasar.ai, and include the underlying error message:"]

    return generic_error_msg(msg, e)

def glibc_error_msg(e):
    msg = [
        "QuasarDB was unable to find the expected GLIBC version on this machine.",
        "This is usually caused by compiling the Python API on a different machine "
        "than you're currently using.",
        "",
        "Please ensure you're using the official version of the QuasarDB Python API, ",
        "and are using a machine that is compatible with the manylinux2014 Python ",
        "platform tag as defined in PEP 599. ",
        "",
        "If you believe this to be a bug, please reach out to QuasarDB at",
        "support@quasar.ai, and include the underlying error message:"]

    return generic_error_msg(msg, e)

try:
    from quasardb.quasardb import *
except BaseException as e:
    if "undefined symbol" in str(e):
        print(link_error_msg(e))
        raise e
    elif "GLIBC" in str(e):
        print(glibc_error_msg(e))
    else:
        from quasardb import *

from .extensions import extend_module
extend_module(quasardb)
