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
from builtins import range as xrange, int

import os
from socket import gethostname
import sys
import traceback
import random
import time
import datetime
import locale

import numpy as np

import quasardb  # pylint: disable=C0413,E0401

STOCK_COLUMN = "stock_id"
PRICE_COLUMN = "price"

def time_execution(str, f, *args):
    print("     - ", str, end='')

    start_time = time.time()
    res = f(*args)
    end_time = time.time()

    print(" [duration: {}s]".format(end_time - start_time))

    return res

def gen_ts_name():
    return "test.{}.{}.{}".format(gethostname(), os.getpid(), random.randint(0, 100000))

def create_ts(q, name):
    ts = q.ts(name)
    ts.create([quasardb.ColumnInfo(quasardb.ColumnType.Int64, STOCK_COLUMN),
               quasardb.ColumnInfo(quasardb.ColumnType.Double, PRICE_COLUMN)])

    return ts

def create_many_ts(q, names):
    return [create_ts(q, x) for x in names]

def generate_points(points_count):
    start_time = np.datetime64('2017-01-01', 'ns')

    dates = np.array([(start_time + np.timedelta64(i, 's')) for i in range(points_count)]).astype('datetime64[ns]')
    stock_ids = np.random.randint(1, 5, size=points_count)
    prices = np.random.uniform(-100.0, 100.0, points_count)

    return (dates, stock_ids, prices)


def batch_ts_columns(ts_name, prealloc_size):
    return (quasardb.BatchColumnInfo(ts_name, STOCK_COLUMN, prealloc_size),
            quasardb.BatchColumnInfo(ts_name, PRICE_COLUMN, prealloc_size))

def bulk_insert(q, ts_names, dates, stock_ids, prices):
    # We generate a flattened list of columns for each timeseries; for example,
    # for 2 columns for 4 timeseries each, we have 8 columns.
    columns = [column for nested in (batch_ts_columns(ts_name, len(dates))
                                     for ts_name in ts_names)
               for column in nested]

    batch_inserter = q.ts_batch(columns)
    for i in range(len(stock_ids)):
        # We use the known layout of column (2 for each timeseries, alternating with
        # STOCK_COLUMN and PRICE_COLUMN) to set the values.
        for j in range(0, len(ts_names) * 2, 2):
            batch_inserter.start_row(dates[i])
            batch_inserter.set_int64(j, stock_ids[i]) # set stock_id
            batch_inserter.set_double(j + 1, prices[i]) # set price

    batch_inserter.push()

def make_it_so(q, points_count):
    ts_names = [gen_ts_name(), gen_ts_name()]

    ts = time_execution("Creating a time series with names {}".format(ts_names), create_many_ts, q, ts_names)
    (dates, stock_ids, prices) = time_execution("Generating {:,} points".format(points_count), generate_points, points_count)
    time_execution("Inserting {:,} points into timeseries with names {}".format(points_count, ts_names), bulk_insert, q, ts_names, dates, stock_ids, prices)


def main(quasardb_uri, points_count):
    print("Connecting to: ", quasardb_uri)
    q = quasardb.Cluster(uri=quasardb_uri)

    print(" *** Inserting {:,} into {}".format(points_count, quasardb_uri))
    make_it_so(q, points_count)

if __name__ == "__main__":

    try:
        if len(sys.argv) != 3:
            print("usage: ", sys.argv[0], " quasardb_uri points_count")
            sys.exit(1)

        main(sys.argv[1], int(sys.argv[2]))

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
