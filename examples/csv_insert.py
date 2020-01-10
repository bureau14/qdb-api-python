# Copyright (c) 2009-2020, quasardb SAS. All rights reserved.
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

import numpy as np
import pandas as pd

import quasardb  # pylint: disable=C0413,E0401

def create_ts(q, name, columns):

    ts = q.ts(name)

    try:
        ts.remove()

    except quasardb.Error:
        pass

    ts.create([quasardb.ColumnInfo(quasardb.ColumnType.Double, x) for x in columns])

    return ts

def display_pts(start_time, end_time, points_count):
    elapsed = end_time - start_time
    pts_sec = points_count /elapsed

    print("...loaded {} points in {}s ({} points/sec)".format(points_count, elapsed, pts_sec))

def main(quasardb_uri, ts_name, csv_file):

    print("Loading CSV: ", csv_file)
    start_time = time.time()
    series = pd.read_csv(csv_file, header=0, parse_dates = {'Timestamp' : ['Date', 'Time']}, index_col = 'Timestamp')

    display_pts(start_time, time.time(), series.size)

    print("Uploading to server: ", quasardb_uri)
    q = quasardb.Cluster(quasardb_uri)
    start_time = time.time()
    ts = create_ts(q, ts_name, series.columns.tolist())

    # insert the columns
    for i in range(0, len(series.columns)):
        ts.double_insert(series.columns[i], series.index.values, np.squeeze(series.values[:,i:i+1]))

    display_pts(start_time, time.time(), series.size)

if __name__ == "__main__":

    try:
        if len(sys.argv) != 4:
            print("usage: ", sys.argv[0], " quasardb_uri ts_name csv_file")
            sys.exit(1)

        main(sys.argv[1], sys.argv[2], sys.argv[3])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
