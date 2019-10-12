# Copyright (c) 2009-2019, quasardb SAS
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
from builtins import int

import os
import sys
import traceback
import datetime

import quasardb  # pylint: disable=C0413,E0401
import numpy as np

def main(quasardb_uri, ts_name):

    print("Connecting to: ", quasardb_uri)
    c = quasardb.Cluster(uri=quasardb_uri)

    # create an instance of the time series object
    # which may or may not exist on the server
    ts = c.ts(ts_name)

    # create the time series with one column of doubles named "my_col"
    # you can specify multiple columns if needed
    # if the time series already exist it will throw an exception
    # the function returns objects enabling direct insertion to the columns
    # here an array of one
    ts.create([quasardb.ColumnInfo(quasardb.ColumnType.Double, "close"),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, "volume"),
        quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, "value_date")])

    # once you have a column, you can directly insert data into it
    # here we use the high level Python API which isn't the fastest
    # because of the date time conversions
    # in another example we showcase more efficient insertion methods
    # it's also possible to insert into multiple columns at once in a line by
    # line fashion with the bulk insert API

    dates = np.arange(np.datetime64('2017-01-01'), np.datetime64('2017-01-04')).astype('datetime64[ns]')

    ts.double_insert("close", dates, np.arange(1.0, 4.0))
    ts.int64_insert("volume", dates, np.arange(1, 4))
    ts.timestamp_insert("value_date", dates, np.arange(np.datetime64('2017-01-02'), np.datetime64('2017-01-05')).astype('datetime64[ns]'))

    # note that intervals are left inclusive, right exclusive
    year_2017 = (np.datetime64("2017-01-01", "ns"), np.datetime64("2018-01-01", "ns"))

    # from the column you can also get your points back directly
    # you can query multiple ranges at once if needed, in this example we only
    # ask for all the points of the year 2017
    points = ts.double_get_ranges("close", [year_2017])
    print("Inserted values: ", points[0], points[1])
    points = ts.int64_get_ranges("volume", [year_2017])
    print("Inserted values: ", points[0], points[1])
    points = ts.timestamp_get_ranges("value_date", [year_2017])
    print("Inserted values: ", points[0], points[1])

    # running queries is about creating a query, every time you run it, you get updated values
    q = c.query("select * from " + ts_name + " in range(2017, +10d)")

    res = q.run()

    for col in res.tables[ts_name]:
        print(col.name, ": ", col.data)

    # this is how we remove the time series
    # note that if it doesn't exist it will throw an exception
    ts.remove()

if __name__ == "__main__":

    try:
        if len(sys.argv) != 3:
            print("usage: ", sys.argv[0], " quasardb_uri ts_name")
            sys.exit(1)

        main(sys.argv[1], sys.argv[2])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
