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

import quasardb

class PandasRequired(ImportError):
    pass

try:
    import pandas as pd
    import numpy as np
    from pandas.core.api import DataFrame, Series
    from pandas.core.base import PandasObject
except ImportError:
    raise PandasRequired("The pandas library is required to handle pandas data formats")

def read_series(table, col_name, ranges):
    """
    Read a Pandas Timeseries from a single column.

    Parameters:
    table : quasardb.Timeseries
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

def read_dataframe(table, index='$timestamp', columns=None, ranges=None):
    """
    Read a Pandas Dataframe from a QuasarDB Timeseries table.

    Parameters:
    table : quasardb.Timeseries
      QuasarDB Timeseries table object, e.g. qdb_cluster.ts('my_table')

    columns : optional list
      List of columns to read in dataframe. The timestamp column '$timestamp' is
      always read.

      Defaults to all columns.

    index: optional str
      Column name for dataframe index. Defaults to '$timestamp'

    ranges: optional list
      A list of time ranges to read, represented as tuples of Numpy datetime64[ns] objects.
      Defaults to the entire table.

    """

    if columns == None:
        columns = list(c.name for c in table.list_columns())

    kwargs = {'columns': columns}
    if ranges != None:
        kwargs['ranges'] = ranges

    xs = dict((c, (read_series(table, c, ranges))) for c in columns)
    return DataFrame(data=xs)

def write_dataframe(df, cluster, table, create=False, chunk_size=50000):
    """
    Store a dataframe into a table.

    Parameters:
    df: pandas.DataFrame
      The pandas dataframe to store.

    cluster: quasardb.Cluster
      Active connection to the QuasarDB cluster

    table: quasardb.Timeseries or str
      Either a string or a reference to a QuasarDB Timeseries table object.
      For example, 'my_table' or cluster.ts('my_table') are both valid values.

    create: optional bool
      Whether to create the table. Defaults to false.
    """

    # Acquire reference to table if string is provided
    if isinstance(table, str):
        table = cluster.table(table)

    # Create batch column info from dataframe
    col_info = list(quasardb.BatchColumnInfo(table.get_name(), c, chunk_size) for c in df.columns)
    batch = cluster.ts_batch(col_info)

    write_with = [None]*len(df.columns)
    for i in range(len(df.columns)):
        c = df.columns[i]
        dtype = df[c].dtype
        if dtype == np.int64:
            write_with[i] = batch.set_int64
        elif dtype == np.int32:
            write_with[i] = batch.set_int64
        elif dtype == np.float64:
            write_with[i] = batch.set_double
        elif dtype == np.object:
            write_with[i] = batch.set_blob
        # Timestamps need to be converted so are a bit trickier
        elif dtype == np.dtype('M8[ns]'):
            write_with[i] = lambda i, x: batch.set_timestamp(i, np.datetime64(x, 'ns'))
        else:
            raise ValueError("Incompatible data type for column " + c + ": ", dtype)

    # Split the dataframe in chunks that equal our batch size
    dfs = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]

    for df in dfs:
        for row in df.itertuples(index=True):
            batch.start_row(np.datetime64(row[0], 'ns'))

            for i in range(len(df.columns)):
                fn = write_with[i]
                v = row[i + 1]
                fn(i, v)

        batch.push()
