# Copyright (c) 2009-2021, quasardb SAS. All rights reserved.
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

from __future__ import print_function

import sys
import traceback
import os
import numpy as np

import quasardb  # pylint: disable=C0413,E0401

def main(uri_src, uri_dst, ts_name):

    print("Connecting to: ", uri_src)
    c_src = quasardb.Cluster(uri=uri_src)
    ts_src = c_src.ts(ts_name)

    print("Connecting to: ", uri_dst)
    c_dst = quasardb.Cluster(uri=uri_dst)
    ts_dst = c_dst.ts(ts_name)

    try:
        ts_dst.remove()
    except:
        pass

    # create the time series on the destination cluster.
    ts_dst.create([quasardb.ColumnInfo(quasardb.ColumnType.Int64, "bid"),
                   quasardb.ColumnInfo(quasardb.ColumnType.Int64, "last")])

    # Note that we are loading all data points of this entire column in
    # memory here. A more scalable solution would split the ranges into
    # smaller ranges, or use the streaming bulk reader and bulk inserter.
    everything = [(np.datetime64('1970-01-01', 'ns'), np.datetime64('2035-01-01', 'ns'))]

    data = ts_src.int64_get_ranges("bid", everything)
    ts_dst.int64_insert("bid", data[0], data[1])

    data = ts_src.int64_get_ranges("last", everything)
    ts_dst.int64_insert("last", data[0], data[1])

if __name__ == "__main__":

    try:
        if len(sys.argv) != 4:
            print("usage: ", sys.argv[0], " cluster_from cluster_to tsname")
            sys.exit(1)

        main(sys.argv[1], sys.argv[2], sys.argv[3])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
