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



# Constant mapping of numpy dtype to QuasarDB column type
# TODO(leon): support this natively in qdb C api ? we have everything we need
#             to understand dtypes.
_dtype_map = {
    np.dtype('int64'): quasardb.ColumnType.Int64,
    np.dtype('int32'): quasardb.ColumnType.Int64,
    np.dtype('float64'): quasardb.ColumnType.Double,
    np.dtype('object'): quasardb.ColumnType.Blob,
    np.dtype('M8[ns]'): quasardb.ColumnType.Timestamp
}

def read_series(table, col_name, ranges=None):
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

    kwargs = {
        'column': col_name
    }

    if ranges:
        kwargs['ranges'] = ranges

    # Dispatch based on column type
    t = table.column_type_by_id(col_name)
    res = (read_with[t])(**kwargs)

    return Series(res[1], index=res[0])

def write_series(series, table, col_name):
    """
    Writes a Pandas Timeseries to a single column.

    Parameters:
    series : pandas.Series
      Pandas Series, with a numpy.datetime64[ns] as index. Underlying data will be attempted
      to be transformed to appropriate QuasarDB type.

    table : quasardb.Timeseries
      QuasarDB Timeseries table object, e.g. qdb_cluster.ts('my_table')

    col_name : str
      Column name to store data in.
    """
    write_with = {
        quasardb.ColumnType.Double: table.double_insert,
        quasardb.ColumnType.Blob: table.blob_insert,
        quasardb.ColumnType.Int64: table.int64_insert,
        quasardb.ColumnType.Timestamp: table.timestamp_insert
    }

    t = table.column_type_by_id(col_name)
    res = (write_with[t])(col_name, series.index.to_numpy(), series.to_numpy())


def read_dataframe(table, row_index=False, columns=None, ranges=None):
    """
    Read a Pandas Dataframe from a QuasarDB Timeseries table.

    Parameters:
    table : quasardb.Timeseries
      QuasarDB Timeseries table object, e.g. qdb_cluster.ts('my_table')

    columns : optional list
      List of columns to read in dataframe. The timestamp column '$timestamp' is
      always read.

      Defaults to all columns.

    row_index: boolean
      Whether or not to index by rows rather than timestamps. Set to true if your
      dataset may contains null values and multiple rows may use the same timestamps.
      Note: using row_index is significantly slower.
      Defaults to false.

    ranges: optional list
      A list of time ranges to read, represented as tuples of Numpy datetime64[ns] objects.
      Defaults to the entire table.

    """

    if columns == None:
        columns = list(c.name for c in table.list_columns())

    if row_index == False:
        xs = dict((c, (read_series(table, c, ranges))) for c in columns)
        return DataFrame(data=xs)
    else:
        kwargs = {
            'columns': columns
        }
        if ranges:
            kwargs['ranges'] = ranges

        reader = table.reader(**kwargs)
        xs = []
        for row in reader:
            xs.append(row.copy())

        columns.insert(0, '$timestamp')
        return DataFrame(data=xs, columns=columns)

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
        table = cluster.ts(table)

    if create:
        _create_table_from_df(df, table)

    # Create batch column info from dataframe
    col_info = list(quasardb.BatchColumnInfo(table.get_name(), c, chunk_size) for c in df.columns)
    batch = cluster.ts_batch(col_info)

    write_with = {
        quasardb.ColumnType.Double: batch.set_double,
        quasardb.ColumnType.Blob: batch.set_blob,
        quasardb.ColumnType.Int64: batch.set_int64,
        quasardb.ColumnType.Timestamp: lambda i, x: batch.set_timestamp(i, np.datetime64(x, 'ns'))
    }

    # Split the dataframe in chunks that equal our batch size
    dfs = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]

    for df in dfs:
        # TODO(leon): use pinned columns so we can write entire numpy arrays
        for row in df.itertuples(index=True):
            batch.start_row(np.datetime64(row[0], 'ns'))

            for i in range(len(df.columns)):
                v = row[i + 1]

                if not pd.isnull(v):
                    ct = _dtype_to_column_type(df[df.columns[i]].dtype)
                    fn = write_with[ct]
                    fn(i, v)

        batch.push()

def _create_table_from_df(df, table):
    cols = list(quasardb.ColumnInfo(_dtype_to_column_type(df[c].dtype), c) for c in df.columns)
    table.create(cols)
    return table

def _dtype_to_column_type(dt):
    res = _dtype_map.get(dt, None)
    if res == None:
        raise ValueError("Incompatible data type: ", dt)

    return res
