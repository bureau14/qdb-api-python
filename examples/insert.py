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
from socket import gethostname
import sys
import traceback
import random
import time
import datetime
import locale
import re

# you don't need the following, it's just added so it can be run from the git repo
# without installing the quasardb library
for root, dirnames, filenames in os.walk(os.path.join(os.path.split(__file__)[0], '..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

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

    try:
        ts.remove()

    except quasardb.Error:
        pass

    cols = ts.create([quasardb.TimeSeries.DoubleColumnInfo(COLUMN_NAME)])

    return (ts, cols[0])

def generate_points(points_count):
    result = []

    time_index = datetime.datetime(2017, 1, 1)
    time_step = datetime.timedelta(microseconds=1)

    for _ in xrange(points_count):
        result.append((time_index, 1.0 + random.random() * 10.0))
        time_index += time_step

    return result

def vanilla_insert(q, points_count):

    ts_name = gen_ts_name()

    (ts, col) = time_execution("Creating a time series of name {}".format(ts_name), create_ts, q, ts_name)
    points = time_execution("Generating {:,} points".format(points_count), generate_points, points_count)
    time_execution("Inserting {:,} points into {}".format(points_count, ts_name), col.insert, points)
    time_execution("Removing time series {}".format(ts_name), ts.remove)

def fast_generate(points_count):
    vec = quasardb.DoublePointsVector()
    vec.resize(points_count)

    tv_sec = quasardb.qdb_convert.time_to_unix_timestamp(datetime.datetime(2017, 1, 1))
    tv_nsec = 0

    for i in xrange(points_count):
        vec[i].timestamp.tv_sec = tv_sec
        vec[i].timestamp.tv_nsec = tv_nsec
        vec[i].value = 1.0 + random.random() * 10.0

        tv_nsec += 1000
        if tv_nsec >= 1000000000:
            tv_sec += 1
            tv_nsec = 0

    return vec

def fast_insert(q, points_count):
    ts_name = gen_ts_name()

    (ts, col) = time_execution("Creating a time series of name {}".format(ts_name), create_ts, q, ts_name)
    points = time_execution("Generating {:,} points".format(points_count), fast_generate, points_count)
    time_execution("Inserting {:,} points into {}".format(points_count, ts_name), col.fast_insert, points)
    time_execution("Removing time series {}".format(ts_name), ts.remove)

def main(quasardb_uri, points_count):

    print("Connecting to: ", quasardb_uri)
    q = quasardb.Cluster(uri=quasardb_uri)

    print(" *** Inserting {:,} into {} the 'vanilla' way".format(points_count, quasardb_uri))
    vanilla_insert(q, points_count)

    print(" *** Inserting {:,} into {} the 'low_level' way".format(points_count, quasardb_uri))
    fast_insert(q, points_count)

if __name__ == "__main__":

    try:
        if len(sys.argv) != 3:
            print("usage: ", sys.argv[0], " quasardb_uri points_count")
            sys.exit(1)

        main(sys.argv[1], int(sys.argv[2]))

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
