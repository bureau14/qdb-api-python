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

from __future__ import print_function
from builtins import int

import os
import sys
import traceback
import datetime
import re

# you don't need the following, it's just added so it can be run from the git repo
# without installing the quasardb library
for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401


def main(quasardb_uri, ts_name):

    print("Connecting to: ", quasardb_uri)
    q = quasardb.Cluster(uri=quasardb_uri)

    # create an instance of the time series object
    # which may or may not exist on the server
    ts = q.ts(ts_name)

    # create the time series with one column of doubles named "my_col"
    # you can specify multiple columns if needed
    # if the time series already exist it will throw an exception
    # the function returns objects enabling direct insertion to the columns
    # here an array of one
    cols = ts.create([quasardb.TimeSeries.DoubleColumnInfo("close")])

    # you can also directly access the column, which is what you want to do
    # if the time series already exist
    # in our example we could have written my_col = cols[0]
    my_col = ts.column(quasardb.TimeSeries.DoubleColumnInfo("close"))

    # once you have a column, you can directly insert data into it
    # here we use the high level Python API which isn't the fastest
    # because of the date time conversions
    # in the other example we showcase more efficient insertion methods
    # it's also possible to insert into multiple columns at once in a line by
    # line fashion with the bulk insert API
    my_col.insert([(datetime.datetime(2017, 1, 1), 1.0),
                   (datetime.datetime(2017, 1, 2), 2.0),
                   (datetime.datetime(2017, 1, 3), 3.0)])

    # note that intervals are left inclusive, right exclusive
    year_2017 = (datetime.datetime(2017, 1, 1), datetime.datetime(2018, 1, 1))

    # from the column you can also get your points back directly
    # you can query multiple ranges at once if needed, in this example we only
    # ask for all the points of the year 2017
    points = my_col.get_ranges([year_2017])
    print("Inserted values: ", points)

    # and of course you can run aggregations, server-side which is very often several
    # orders of magnitude faster than what you can do client side thanks to the power
    # of distributed computing!
    # it will return a list of results matching the aggregation requests provided
    agg_request = quasardb.TimeSeries.DoubleAggregations()
    agg_request.append(quasardb.TimeSeries.Aggregation.arithmetic_mean, year_2017)

    my_average = my_col.aggregate(agg_request)
    print("The average value is: ", my_average[0].value)

    # this is how we remove the time series
    # note that if it doesn't exist it will throw an exception
#  ts.remove()

if __name__ == "__main__":

    try:
        if len(sys.argv) != 3:
            print("usage: ", sys.argv[0], " quasardb_uri ts_name")
            sys.exit(1)

        main(sys.argv[1], sys.argv[2])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
