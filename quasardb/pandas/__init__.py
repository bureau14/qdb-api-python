# pylint: disable=C0103,C0111,C0302,R0903

# Copyright (c) 2009-2021, quasardb SAS. All rights reserved.
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
import logging
import time
from datetime import datetime
from functools import partial


logger = logging.getLogger('quasardb.pandas')


class PandasRequired(ImportError):
    """
    Exception raised when trying to use QuasarDB pandas integration, but
    pandas has not been installed.
    """
    pass


try:
    import numpy as np
    import pandas as pd
    from pandas.core.api import DataFrame, Series
    from pandas.core.base import PandasObject
except ImportError:
    raise PandasRequired(
        "The pandas library is required to handle pandas data formats")


# Constant mapping of numpy dtype to QuasarDB column type
# TODO(leon): support this natively in qdb C api ? we have everything we need
#             to understand dtypes.
_dtype_map = {
    np.dtype('int64'): quasardb.ColumnType.Int64,
    np.dtype('int32'): quasardb.ColumnType.Int64,
    np.dtype('float64'): quasardb.ColumnType.Double,
    np.dtype('object'): quasardb.ColumnType.String,
    np.dtype('M8[ns]'): quasardb.ColumnType.Timestamp,
    np.dtype('datetime64[ns]'): quasardb.ColumnType.Timestamp,

    'int64': quasardb.ColumnType.Int64,
    'int32': quasardb.ColumnType.Int64,
    'float32': quasardb.ColumnType.Double,
    'float64': quasardb.ColumnType.Double,
    'timestamp': quasardb.ColumnType.Timestamp,
    'string': quasardb.ColumnType.String,
    'bytes': quasardb.ColumnType.Blob,

    'floating': quasardb.ColumnType.Double,
    'integer': quasardb.ColumnType.Int64,
    'bytes': quasardb.ColumnType.Blob,
    'string': quasardb.ColumnType.String,
    'datetime64':  quasardb.ColumnType.Timestamp
}

# Based on QuasarDB column types, which dtype do we want?
_dtypes_map_flip = {
    quasardb.ColumnType.String: np.dtype('unicode'),
    quasardb.ColumnType.Int64: np.dtype('int64'),
    quasardb.ColumnType.Double: np.dtype('float64'),
    quasardb.ColumnType.Blob: np.dtype('object'),
    quasardb.ColumnType.Timestamp: np.dtype('datetime64[ns]')
}


def read_series(table, col_name, ranges=None):
    """
    Read a Pandas Timeseries from a single column.

    Parameters:
    -----------

    table : quasardb.Timeseries
      QuasarDB Timeseries table object, e.g. qdb_cluster.table('my_table')

    col_name : str
      Name of the column to read.

    ranges : list
      A list of ranges to read, represented as tuples of Numpy datetime64[ns] objects.
    """
    read_with = {
        quasardb.ColumnType.Double: table.double_get_ranges,
        quasardb.ColumnType.Blob: table.blob_get_ranges,
        quasardb.ColumnType.String: table.string_get_ranges,
        quasardb.ColumnType.Int64: table.int64_get_ranges,
        quasardb.ColumnType.Timestamp: table.timestamp_get_ranges,
    }

    kwargs = {
        'column': col_name
    }

    if ranges is not None:
        kwargs['ranges'] = ranges

    # Dispatch based on column type
    t = table.column_type_by_id(col_name)

    logger.debug(
        "reading Series from column %s.%s with type %s",
        table.get_name(),
        col_name,
        t)

    res = (read_with[t])(**kwargs)

    return Series(res[1], index=res[0])


def write_series(series, table, col_name):
    """
    Writes a Pandas Timeseries to a single column.

    Parameters:
    -----------

    series : pandas.Series
      Pandas Series, with a numpy.datetime64[ns] as index. Underlying data will be attempted
      to be transformed to appropriate QuasarDB type.

    table : quasardb.Timeseries
      QuasarDB Timeseries table object, e.g. qdb_cluster.table('my_table')

    col_name : str
      Column name to store data in.
    """
    write_with = {
        quasardb.ColumnType.Double: table.double_insert,
        quasardb.ColumnType.Blob: table.blob_insert,
        quasardb.ColumnType.String: table.string_insert,
        quasardb.ColumnType.Int64: table.int64_insert,
        quasardb.ColumnType.Timestamp: table.timestamp_insert
    }

    t = table.column_type_by_id(col_name)

    xs = series.to_numpy(_dtypes_map_flip[t])
    logger.debug(
        "writing Series to column %s.%s with type %s",
        table.get_name(),
        col_name,
        t)

    (write_with[t])(col_name, series.index.to_numpy(), xs)


def query(cluster, query, blobs=False):
    """
    Execute a query and return the results as DataFrames. Returns a dict of
    tablename / DataFrame pairs.

    Parameters:
    -----------

    cluster : quasardb.Cluster
      Active connection to the QuasarDB cluster

    query : str
      The query to execute.

    blobs : bool or list
      Determines which QuasarDB blob-columns should be returned as bytearrays; otherwise
      they are returned as UTF-8 strings.

      True means every blob column should be returned as byte-array, or a list will
      specify which specific columns. Defaults to false, meaning all blobs are returned
      as strings.

    """
    logger.debug("querying and returning as DataFrame: %s", query)

    return DataFrame(cluster.query(query, blobs=blobs))


def read_dataframe(table, row_index=False, columns=None, ranges=None):
    """
    Read a Pandas Dataframe from a QuasarDB Timeseries table.

    Parameters:
    -----------

    table : quasardb.Timeseries
      QuasarDB Timeseries table object, e.g. qdb_cluster.table('my_table')

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

    if columns is None:
        columns = list(c.name for c in table.list_columns())

    if not row_index:
        logger.debug("reading DataFrame from %d series", len(columns))
        xs = dict((c, (read_series(table, c, ranges))) for c in columns)
        return DataFrame(data=xs)

    kwargs = {
        'columns': columns
    }
    if ranges:
        kwargs['ranges'] = ranges

    logger.debug("reading DataFrame from bulk reader")

    reader = table.reader(**kwargs)
    xs = []
    for row in reader:
        xs.append(row.copy())

    columns.insert(0, '$timestamp')

    logger.debug(
        "read %d rows, returning as DataFrame with %d columns",
        len(xs),
        len(columns))

    return DataFrame(data=xs, columns=columns)


def string_to_timestamp(x):
    return np.datetime64(datetime.utcfromtimestamp(int(float(x))), 'ns')


def fnil(f, x):
    "Utility function, only apply f to x if x is not nillable"

    if pd.isnull(x):
        return None

    if x == b'nan' or x == 'nan':
        return None

    return f(x)


_infer_with = {
    quasardb.ColumnType.Int64: {
        'floating': np.int64,
        'integer': np.int64,
        'string': lambda x: np.float64(x).astype(np.int64),
        'bytes': lambda x: np.float64(x.decode("utf-8")).astype(np.int64),
        'datetime64': lambda x: np.int64(x.nanosecond),
        'datetime': lambda x: np.int64(x.timestamp() * 1000000000),
        '_': lambda x: np.float64(x).astype(np.int64)
    },
    quasardb.ColumnType.Double: {
        'floating': lambda x: x,
        'integer': np.float64,
        'string': np.float64,
        'bytes': lambda x: np.float64(x.decode("utf-8")),
        'datetime64': lambda x: np.float64(x.nanosecond),
        '_': np.float64
    },
    quasardb.ColumnType.Blob: {
        'floating': lambda x: str(x).encode("utf-8"),
        'integer': lambda x: str(x).encode("utf-8"),
        'string': lambda x: str(x).encode("utf-8"),
        'bytes': lambda x: x,
        '_': lambda x: str(x).encode("utf-8"),
    },
    quasardb.ColumnType.String: {
        'floating': str,
        'integer': str,
        'string': str,
        'bytes': lambda x: x.decode("utf-8"),
        '_': str
    },
    quasardb.ColumnType.Timestamp: {
        'floating': lambda x: np.datetime64(datetime.utcfromtimestamp(x), 'ns'),
        'integer': lambda x: np.datetime64(datetime.utcfromtimestamp(x), 'ns'),
        'string': string_to_timestamp,
        'bytes': lambda x: string_to_timestamp(x.decode("utf-8")),
        'datetime64': lambda x: np.datetime64(x, 'ns'),
        '_': lambda x: np.datetime64(datetime.utcfromtimestamp(np.float64(x)), 'ns')
    }
}


# Update all function pointers to wrap them in _fnil, so that we don't
# accidentally convert e.g. None into string 'None'
for ct in _infer_with:
    for dt in _infer_with[ct]:
        f = _infer_with[ct][dt]
        _infer_with[ct][dt] = partial(fnil, f)


def write_dataframe(
        df,
        cluster,
        table,
        create=False,
        _async=False,
        fast=False,
        truncate=False,
        infer_types=True):
    """
    Store a dataframe into a table.

    Parameters:
    df: pandas.DataFrame
      The pandas dataframe to store.

    cluster: quasardb.Cluster
      Active connection to the QuasarDB cluster

    table: quasardb.Timeseries or str
      Either a string or a reference to a QuasarDB Timeseries table object.
      For example, 'my_table' or cluster.table('my_table') are both valid values.

    create: optional bool
      Whether to create the table. Defaults to false.

    infer_types: optional bool
      If true, will attemp to convert types from Python to QuasarDB natives types if
      the provided dataframe has incompatible types. For example, a dataframe with integers
      will automatically convert these to doubles if the QuasarDB table expects it.

      Defaults to True. For production use cases where you want to avoid implicit conversions,
      we recommend setting this to False.

    truncate: optional bool
      Truncate (also referred to as upsert) the data in-place. Will detect time range to truncate
      from the time range inside the dataframe.

      Defaults to False.

    _async: optional bool
      If true, uses asynchronous insertion API where commits are buffered server-side and
      acknowledged before they are written to disk. If you insert to the same table from
      multiple processes, setting this to True may improve performance.

      Defaults to False.

    fast: optional bool
      Whether to use 'fast push'. If you incrementally add small batches of data to table,
      you may see better performance if you set this to True.

      Defaults to False.

    """

    # Acquire reference to table if string is provided
    if isinstance(table, str):
        table = cluster.table(table)

    if create:
        _create_table_from_df(df, table)

    # Create batch column info from dataframe
    col_info = list(quasardb.BatchColumnInfo(
        table.get_name(), c, df.shape[0]) for c in df.columns)
    batch = cluster.inserter(col_info)

    write_with = {
        quasardb.ColumnType.Double: batch.set_double,
        quasardb.ColumnType.Blob: batch.set_blob,
        quasardb.ColumnType.String: batch.set_string,
        quasardb.ColumnType.Int64: batch.set_int64,
        quasardb.ColumnType.Timestamp: lambda i,
        x: batch.set_timestamp(
            i,
            np.datetime64(
                x,
                'ns'))}

    # We derive our column types from our table.
    ctypes = dict()
    for c in table.list_columns():
        ctypes[c.name] = c.type
    # Performance improvement: avoid a expensive dict lookups by indexing
    # the column types by relative offset within the df.
    ctypes_indexed = list(ctypes[c] for c in df.columns)

    dtypes = dict()
    dtypes_indexed = list()
    if infer_types is True:
        for i in range(len(df.columns)):
            c = df.columns[i]
            dt = pd.api.types.infer_dtype(df[c].values)
            logger.debug("Determined dtype of column %s to be %s", c, dt)
            dtypes[c] = dt

        # Performance improvement: avoid a expensive dict lookups by indexing
        # the column types by relative offset within the df.
        dtypes_indexed = list(dtypes[c] for c in df.columns)

    # TODO(leon): use pinned columns so we can write entire numpy arrays
    for row in df.itertuples(index=True):
        if pd.isnull(row[0]):
            raise RuntimeError(
                "Index must be a valid timestamp, found: " + str(row[0]))

        batch.start_row(np.datetime64(row[0], 'ns'))

        for i in range(len(df.columns)):
            v = row[i + 1]
            ct = ctypes_indexed[i]

            if infer_types is True:
                dt = dtypes_indexed[i]
                try:
                    fn = _infer_with[ct][dt]
                except KeyError:
                    # Fallback default
                    fn = _infer_with[ct]['_']

                v = fn(v)

            if not pd.isnull(v) and not pd.isna(v):

                fn = write_with[ct]

                try:
                    fn(i, v)
                except TypeError:
                    logger.exception(
                        "An error occured while setting column value: %s = %s", df.columns[i], v)
                    raise
                except ValueError:
                    logger.exception(
                        "An error occured while setting column value: %s = %s", df.columns[i], v)
                    raise

    start = time.time()

    logger.debug(
        "push chunk of %d rows, fast?=%s, async?=%s", len(
            df.index), fast, _async)

    if fast is True:
        batch.push_fast()
    elif truncate is True:
        batch.push_truncate()
    elif isinstance(truncate, tuple):
        batch.push_truncate(range=truncate)
    elif _async is True:
        batch.push_async()
    else:
        batch.push()

    logger.debug(
        "pushed %d rows in %s seconds", len(
            df.index), (time.time() - start))


def write_pinned_dataframe(
        df,
        cluster,
        table,
        create=False,
        _async=False,
        fast=False,
        truncate=False,
        infer_types=True):
    """
    Store a dataframe into a table with the pin column API.
    Parameters:
    -----------

    df: pandas.DataFrame
      The pandas dataframe to store.
    cluster: quasardb.Cluster
      Active connection to the QuasarDB cluster
    table: quasardb.Timeseries or str
      Either a string or a reference to a QuasarDB Timeseries table object.
      For example, 'my_table' or cluster.table('my_table') are both valid values.
    create: optional bool
      Whether to create the table. Defaults to false.
    infer_types: optional bool
      If true, will attemp to convert types from Python to QuasarDB natives types if
      the provided dataframe has incompatible types. For example, a dataframe with integers
      will automatically convert these to doubles if the QuasarDB table expects it.
      Defaults to True. For production use cases where you want to avoid implicit conversions,
      we recommend setting this to False.
    truncate: optional bool
      Truncate (also referred to as upsert) the data in-place. Will detect time range to truncate
      from the time range inside the dataframe.
      Defaults to False.
    _async: optional bool
      If true, uses asynchronous insertion API where commits are buffered server-side and
      acknowledged before they are written to disk. If you insert to the same table from
      multiple processes, setting this to True may improve performance.
      Defaults to False.
    fast: optional bool
      Whether to use 'fast push'. If you incrementally add small batches of data to table,
      you may see better performance if you set this to True.
      Defaults to False.
    """

    # Acquire reference to table if string is provided
    if isinstance(table, str):
        table = cluster.table(table)

    if create:
        _create_table_from_df(df, table)

    # Create batch column info from dataframe
    writer = cluster.pinned_writer([table])

    write_with = {
        quasardb.ColumnType.Double: writer.set_double_column,
        quasardb.ColumnType.Blob: writer.set_blob_column,
        quasardb.ColumnType.String: writer.set_string_column,
        quasardb.ColumnType.Int64: writer.set_int64_column,
        quasardb.ColumnType.Timestamp: writer.set_timestamp_column,
    }

    pin_dtypes_map_flip = {
        quasardb.ColumnType.String: np.dtype('unicode'),
        quasardb.ColumnType.Int64: np.dtype('int64'),
        quasardb.ColumnType.Double: np.dtype('float64'),
        quasardb.ColumnType.Blob: np.dtype('object'),
        quasardb.ColumnType.Timestamp: np.dtype('datetime64[ns]')
    }

    # We derive our column types from our table.
    batch_columns = writer.batch_column_infos()
    column_types = writer.column_types()
    table_name = table.get_name()

    # Reorder & introduce new columns
    # https://stackoverflow.com/questions/13148429/how-to-change-the-order-of-dataframe-columns
    cnames = []
    ctypes = []
    for i in range(len(batch_columns)):
        bc = batch_columns[i]
        cnames.append(bc.column)
        ctypes.append(column_types[i])
        # Introduce new column
        found = False
        for c in df.columns:
            if table_name == bc.timeseries and c == bc.column:
                found = True
        if found == False:
            df.insert(i, bc.column, None)
    # Reorder columns
    df = df[cnames]

    dtypes = _get_inferred_dtypes_indexed(df) if infer_types is True else list()

    for i in range(len(df.columns)):
        ct = ctypes[i]
        tmp = df.iloc[:, i]
        timestamps = tmp.index.astype(np.dtype('int64')).tolist()
        values = []
        if infer_types is True:
            dt = dtypes[i]
            fn = None
            try:
                fn = _infer_with[ct][dt]
            except KeyError:
                # Fallback default
                fn = _infer_with[ct]['_']
            values = tmp.apply(lambda v: fn(v) if not pd.isnull(v) and not pd.isna(v) else None).tolist()
        else:
            dt = pin_dtypes_map_flip[ct]
            if (ct == quasardb.ColumnType.Int64):
                # you need to fill with 0x8000000000000000 (which is quasardb value for null)
                # astype(np.dtype('int64')) cannot convert None to int64
                values = tmp.fillna(0x8000000000000000).astype(dt).tolist()
            elif (ct == quasardb.ColumnType.Blob):
                values = tmp.fillna(b'').astype(dt).tolist()
            elif (ct == quasardb.ColumnType.String):
                values = tmp.fillna('').astype(dt).tolist()
            else:
                values = tmp.astype(dt).tolist()
        write_with[ct](i, timestamps, values)

    start = time.time()

    logger.debug("push chunk of %d rows, fast?=%s, async?=%s",
                 len(df.index), fast, _async)

    if fast is True:
        writer.push_fast()
    elif truncate is True:
        writer.push_truncate()
    elif isinstance(truncate, tuple):
        writer.push_truncate(range=truncate)
    elif _async is True:
        writer.push_async()
    else:
        writer.push()

    logger.debug(
        "pushed %d rows in %s seconds", len(
            df.index), (time.time() - start))

    return table


def _create_table_from_df(df, table):
    cols = list()

    dtypes = _get_inferred_dtypes(df)
    for c in df.columns:
        dt = dtypes[c]
        ct = _dtype_to_column_type(df[c].dtype, dt)
        logger.debug("probed pandas dtype %s to inferred dtype %s and map to quasardb column type %s", df[c].dtype, dt, ct)
        cols.append(quasardb.ColumnInfo(ct, c))


    try:
        table.create(cols)
    except quasardb.quasardb.AliasAlreadyExistsError:
        # TODO(leon): warn? how?
        pass

    return table

def _dtype_to_column_type(dt, inferred):
    res = _dtype_map.get(inferred, None)
    if res is None:
        res = _dtype_map.get(dt, None)

    if res is None:
        raise ValueError("Incompatible data type: ", dt)

    return res

def _get_inferred_dtypes(df):
    dtypes = dict()
    for i in range(len(df.columns)):
        c = df.columns[i]
        dt = pd.api.types.infer_dtype(df[c].values)
        logger.debug("Determined dtype of column %s to be %s", c, dt)
        dtypes[c] = dt
    return dtypes

def _get_inferred_dtypes_indexed(df):
    dtypes = _get_inferred_dtypes(df)
    # Performance improvement: avoid a expensive dict lookups by indexing
    # the column types by relative offset within the df.
    return list(dtypes[c] for c in df.columns)
