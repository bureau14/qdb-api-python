# pylint: disable=C0103,C0111,C0302,R0903

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

import numpy as np
import quasardb

class PandasRequired(ImportError):
    pass

try:
    from pandas.core.api import DataFrame, Series
    from pandas.core.base import PandasObject
except ImportError:
    raise PandasRequired("The pandas library is required to handle pandas data formats")

def as_series(table, col_name, ranges):
    """
    Read a Pandas Timeseries from a single column.

    Parameters:
    table : str
      QuasarDB Timeseries table object, e.g. qdb_cluster.ts('my_table')

    col_name : str
      Name of the column to read.

    ranges : list
      A list of ranges to read, represented as tuples of Numpy datetime64[ns] objects.
    """
    read_with = {
        quasardb.ColumnType.Double: table.double_get_ranges,
        quasardb.ColumnType.Blob: table.blob_get_ranges,
        quasardb.ColumnType.Int64: table.int64_get_ranges,
        quasardb.ColumnType.Timestamp: table.timestamp_get_ranges,
    }

    # Dispatch based on column type
    t = table.column_type_by_id(col_name)
    res = (read_with[t])(col_name, ranges)

    return Series(res[1], index=res[0])

class QDBTable(PandasObject):
    """
    For mapping Pandas tables to QuasarDB tables.
    """
    def __init__(self, table):
        self.table = table
        self.df = None


def as_dataframe(table, columns=None, ranges=None, index='$timestamp'):
    """
    Read a Pandas Dataframe from a QuasarDB Timeseries table.

    Parameters:
    table : str
      QuasarDB Timeseries table object, e.g. qdb_cluster.ts('my_table')
    """

    if columns == None:
        columns = list(c.name for c in table.list_columns())

    reader = table.reader(columns=columns,
                          ranges=ranges) if ranges else table.reader(columns=columns)

    rows = []
    for row in reader:
        rows.append(row.copy())

    columns.insert(0, '$timestamp')

    return DataFrame.from_records(rows, columns=columns, index=index)
