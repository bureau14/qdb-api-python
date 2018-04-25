# Copyright (c) 2009-2018, quasardb SAS
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
import time
import datetime
import random

import quasardb  # pylint: disable=C0413,E0401

def seed_ts(c, name):
    ts = c.ts(name)
    try:
        ts.remove()
    except:
        pass

    cols = ts.create([quasardb.TimeSeries.Int64ColumnInfo("bid"),
                      quasardb.TimeSeries.Int64ColumnInfo("last")])
    table = ts.local_table()

    # QuasarDB uses a high-performance, high-precision timestamp
    # structure because using datetime directly would be too
    # inefficient.
    #
    # Note that we are using the same timestamp for all data points
    # here.
    qdb_timestamp = quasardb.impl.qdb_timespec_t()
    qdb_timestamp.tv_sec = int(round(time.time()))
    qdb_timestamp.tv_usec = 0

    # Generate 20 random data points
    for values in zip(random.sample(range(0, 100), 20),
                      random.sample(range(0, 1000), 20)):
        table.fast_append_row(qdb_timestamp, *values)

    table.push()

    return ts, cols

def main(uri):

    print("Connecting to: ", uri)
    c = quasardb.Cluster(uri=uri)

    ts_name1 = "stocks.apple"
    ts_name2 = "stocks.google"

    ts1, cols1 = seed_ts(c, ts_name1)
    ts2, cols2 = seed_ts(c, ts_name2)

    # Attach a tag to both of them:
    ts1.attach_tag("nasdaq")
    ts2.attach_tag("nasdaq")

    res = c.query_exp("select sum(bid), count(bid) from find(tag='nasdaq') in range(2018, 2019)")
    for t in res.tables:
        # We expect two columns, one for sum(bid), other for count(bid)
        assert t.columns_count == 3
        assert t.rows_count == 1

        print(t.table_name, " sum =", t.get_payload(0, 1)[1],  ", count =", t.get_payload(0, 2)[1])



if __name__ == "__main__":

    try:
        if len(sys.argv) != 2:
            print("usage: ", sys.argv[0], " cluster")
            sys.exit(1)

        main(sys.argv[1])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
