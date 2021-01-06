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

def seed_ts(c, name):
    ts = c.ts(name)
    try:
        ts.remove()
    except:
        pass

    ts.create([quasardb.ColumnInfo(quasardb.ColumnType.Int64, "bid"),
                      quasardb.ColumnInfo(quasardb.ColumnType.Int64, "last")])

    # Generate 20 random data points
    date = np.arange(np.datetime64('2018-01-01'), np.datetime64('2018-01-21')).astype('datetime64[ns]')
    values = np.random.random_integers(0, 100, len(date))

    ts.int64_insert("bid", date, np.random.random_integers(0, 100, len(date)))
    ts.int64_insert("last", date, np.random.random_integers(0, 100, len(date)))

    return ts

def main(uri):

    print("Connecting to: ", uri)
    c = quasardb.Cluster(uri=uri)

    ts_name1 = "stocks.apple"
    ts_name2 = "stocks.google"

    ts1 = seed_ts(c, ts_name1)
    ts2 = seed_ts(c, ts_name2)

    # Attach a tag to both of them:
    ts1.attach_tag("nasdaq")
    ts2.attach_tag("nasdaq")

    q = c.query("select sum(bid), count(bid) from find(tag='nasdaq') in range(2018, 2019)")
    res = q.run()

    for k, table_result in res.tables.items():
        print("table:", k)
        for col in table_result:
            print(col.name, ":", col.data)

if __name__ == "__main__":

    try:
        if len(sys.argv) != 2:
            print("usage: ", sys.argv[0], " cluster")
            sys.exit(1)

        main(sys.argv[1])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
