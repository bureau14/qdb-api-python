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

# you don't need the following, it's just added so it can be run from the git repo
# without installing the quasardb library
sys.path.append(os.path.join(os.path.split(__file__)[0], '..', 'bin', 'Release'))
sys.path.append(os.path.join(os.path.split(__file__)[0], '..', 'bin', 'release'))

import quasardb  # pylint: disable=C0413,E0401

COLUMN_NAME = "values"

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
    ts.create([quasardb.ColumnInfo(quasardb.ColumnType.Double, COLUMN_NAME)])

    return ts

def generate_points(points_count):
    start_time = np.datetime64('2017-01-01', 'ns')

    dates = np.array([(start_time + np.timedelta64(i, 's')) for i in range(points_count)]).astype('datetime64[ns]')
    doubles = np.random.uniform(-100.0, 100.0, points_count)

    return (dates, doubles)

def bulk_insert(q, ts_name, dates, values):
    columns = [quasardb.BatchColumnInfo(ts_name, COLUMN_NAME, len(dates))]
    batch_inserter = q.ts_batch(columns)

    # we could insert the whole column directly, but let's see how the row API works
    for i in range(len(values)):
        batch_inserter.append_double(values[i])
        batch_inserter.finalize_row(dates[i])

    batch_inserter.push()

def make_it_so(q, points_count):
    ts_name = gen_ts_name()

    ts = time_execution("Creating a time series of name {}".format(ts_name), create_ts, q, ts_name)
    (dates, values) = time_execution("Generating {:,} points".format(points_count), generate_points, points_count)
    time_execution("Inserting {:,} points into {}".format(points_count, ts_name), bulk_insert, q, ts_name, dates, values)
    time_execution("Removing time series {}".format(ts_name), ts.remove)

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
