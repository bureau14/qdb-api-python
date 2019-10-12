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
from builtins import range as xrange, int

import os
from socket import gethostname
import sys
import traceback
import random
import numpy as np
import time
import locale

import quasardb  # pylint: disable=C0413,E0401

COLUMN1_NAME = "value"
COLUMN2_NAME = "location"

cities = ["Tokyo",
          "New-York",
          "Sao Paulo",
          "Seoul",
          "Mexico",
          "Osaka",
          "Manila",
          "Mumbai",
          "Delhi",
          "Jakarta",
          "Lagos",
          "Kolkata",
          "Cairo",
          "Los Angeles",
          "Buenos Aires",
          "Rio de Janeiro",
          "Moscow",
          "Shanghai",
          "Karachi",
          "Paris"]

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

    try:
        ts.remove()

    except quasardb.Error:
        pass

    ts.create([quasardb.ColumnInfo(quasardb.ColumnType.Double, COLUMN1_NAME), quasardb.ColumnInfo(quasardb.ColumnType.Blob, COLUMN2_NAME)])

    return ts

def generate_points(points_count):
    dates = np.arange(np.datetime64('2017-01-01'), np.datetime64('2017-01-01') + np.timedelta64(points_count, 'm')).astype('datetime64[ns]')
    values = np.random.uniform(-10.0, 40.0, len(dates))

    return (dates, values)

def generate_cities(dates, values):
    res = []

    for i in range(0, len(values)):
      res.append(random.choice(cities))

    return np.array(res)

def insert(q, ts_name, points_count):
    ts = time_execution("Creating a time series of name {}".format(ts_name), create_ts, q, ts_name)
    (dates, values) = time_execution("Generating {:,} points".format(points_count), generate_points, points_count)
    cities = generate_cities(dates, values)

    time_execution("Inserting {:,} temperature points into {}".format(points_count, ts_name), ts.double_insert, COLUMN1_NAME, dates, values)
    time_execution("Inserting {:,} locations into {}".format(points_count, ts_name), ts.blob_insert, COLUMN2_NAME, dates, cities)

def main(quasardb_uri, ts_name, points_count):

    print("Connecting to: ", quasardb_uri)
    q = quasardb.Cluster(uri=quasardb_uri)

    print(" *** Inserting {:,} into {}".format(points_count, quasardb_uri))
    insert(q, ts_name, points_count)

if __name__ == "__main__":

    try:
        if len(sys.argv) != 4:
            print("usage: ", sys.argv[0], " quasardb_uri ts_name points_count")
            sys.exit(1)

        main(sys.argv[1], sys.argv[2], int(sys.argv[3]))

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
