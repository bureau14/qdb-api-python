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

import quasardb.impl as impl  # pylint: disable=C0413,E0401
import numbers

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
        if isinstance(v, numbers.Integral):
            members[x[prefix_len:]] = v

    return type(type_name, (), members)

Compression = __make_enum('Compression', 'compression_')
Encryption = __make_enum('Encryption', 'encryption_')
ErrorCode = __make_enum('ErrorCode', 'error_')
Operation = __make_enum('Operation', 'operation_')
Options = __make_enum('Option', 'option_')
Protocol = __make_enum('Protocol', 'protocol_')
TSAggregation = __make_enum('Aggregation', 'aggregation_')
TSColumnType = __make_enum('ColumnType', 'column_')
TSFilter = __make_enum('Filter', 'filter_')
