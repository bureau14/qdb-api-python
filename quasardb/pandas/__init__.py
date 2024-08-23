# pylint: disable=C0103,C0111,C0302,R0903

# Copyright (c) 2009-2024, quasardb SAS. All rights reserved.
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

import logging
from datetime import datetime
from functools import partial

import quasardb
import quasardb.table_cache as table_cache
import quasardb.numpy as qdbnp


logger = logging.getLogger('quasardb.pandas')

class PandasRequired(ImportError):
    """
    Exception raised when trying to use QuasarDB pandas integration, but
    pandas has not been installed.
    """
    pass

try:
    import numpy as np
    import numpy.ma as ma
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
        quasardb.ColumnType.Symbol: table.string_get_ranges,
    }

    kwargs = {
        'column': col_name
    }

    if ranges is not None:
        kwargs['ranges'] = ranges

    # Dispatch based on column type
    t = table.column_type_by_id(col_name)

    logger.info("reading Series from column %s.%s with type %s",
                 table.get_name(), col_name, t)

    res = (read_with[t])(**kwargs)

    return Series(res[1], index=res[0])


def write_series(series,
                 table,
                 col_name,
                 infer_types=True,
                 dtype=None):
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

    logger.debug("write_series, table=%s, col_name=%s, infer_types=%s, dtype=%s", table.get_name(), col_name, infer_types, dtype)

    data = None
    index = None

    data = ma.masked_array(series.to_numpy(copy=False),
                           mask=series.isna())

    if infer_types is True:
        index = series.index.to_numpy('datetime64[ns]', copy=False)
    else:
        index = series.index.to_numpy(copy=False)

    assert data is not None
    assert index is not None

    return qdbnp.write_array(data=data,
                             index=index,
                             table=table,
                             column=col_name,
                             dtype=dtype,
                             infer_types=infer_types)

def query(cluster: quasardb.Cluster,
          query,
          index=None,
          blobs=False,
          numpy=True):
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
    (index, m) = qdbnp.query(cluster, query, index=index, dict=True)
    df = pd.DataFrame(m)

    df.set_index(index, inplace=True)
    return df

def stream_dataframe(table : quasardb.Table, *, batch_size : int = 0, column_names : list = None, ranges : list = None):
    """
    Read a Pandas Dataframe from a QuasarDB Timeseries table. Returns a generator with dataframes of size `batch_size`, which is useful
    when traversing a large dataset which does not fit into memory.

    Parameters:
    -----------

    table : quasardb.Timeseries
      QuasarDB Timeseries table object, e.g. qdb_cluster.table('my_table')

    batch_size : int
      The amount of rows to fetch in a single read operation. If unset, or 0, will
      return all data as an entire dataframe. Otherwise, returns a generator which
      yields on dataframe at a time.

    column_names : optional list
      List of columns to read in dataframe. The timestamp column '$timestamp' is
      always read.

      Defaults to all columns.

    ranges: optional list
      A list of time ranges to read, represented as tuples of Numpy datetime64[ns] objects.
      Defaults to the entire table.

    """
    # Sanitize batch_size
    if batch_size == None:
        batch_size = 0
    elif not isinstance(batch_size, int):
        raise TypeError("batch_size should be an integer, but got: {} with value {}".format(type(batch_size), str(batch_size)))

    kwargs = {}

    if column_names:
        kwargs['column_names'] = column_names

    if ranges:
        kwargs['ranges'] = ranges

    with table.reader(**kwargs) as reader:
        for batch in reader:
            # We always expect the timestamp column, and set this as the index
            assert '$timestamp' in batch

            idx = pd.Index(batch.pop('$timestamp'), copy=False, name='$timestamp')
            df = pd.DataFrame(batch, index=idx)

            yield df


def read_dataframe(table, **kwargs):
    """
    Read a Pandas Dataframe from a QuasarDB Timeseries table. Wraps around stream_dataframe(), and
    returns everything as a single dataframe. batch_size is always explicitly set to 0.
    """

    if 'batch_size' in kwargs and kwargs['batch_size'] != 0 and kwargs['batch_size'] != None:
        logger.warn("Providing a batch size with read_dataframe is unsupported, overriding batch_size to 0.")
        logger.warn("If you wish to traverse the data in smaller batches, please use: stream_dataframe().")
        kwargs['batch_size'] = 0

    # Note that this is *lazy*, dfs is a generator, not a list -- as such, dataframes will be
    # fetched on-demand, which means that an error could occur in the middle of processing
    # dataframes.
    dfs = stream_dataframe(table, **kwargs)

    return pd.concat(dfs)


def _extract_columns(df, cinfos):
    """
    Converts dataframe to a number of numpy arrays, one for each column.

    Arrays will be indexed by relative offset, in the same order as the table's columns.
    If a table column is not present in the dataframe, it it have a None entry.
    If a dataframe column is not present in the table, it will be ommitted.
    """
    ret = {}

    # Grab all columns from the DataFrame in the order of table columns,
    # put None if not present in df.
    for i in range(len(cinfos)):
        (cname, ctype) = cinfos[i]
        xs = None

        if cname in df.columns:
            arr = df[cname].array
            ret[cname] = ma.masked_array(arr.to_numpy(copy=False),
                                         mask=arr.isna())

    return ret

def write_dataframes(
        dfs,
        cluster,
        *,
        create = False,
        shard_size = None,
        **kwargs):
    """
    Store dataframes into a table. Any additional parameters not documented here
    are passed to numpy.write_arrays(). Please consult the pydoc of that function
    for additional accepted parameters.

    Parameters:
    -----------

    dfs: dict[str | quasardb.Table, pd.DataFrame] | list[tuple[str | quasardb.Table, pd.DataFrame]]
      This can be either a dict that maps table (either objects or names) to a dataframe, or a list
      of table<>dataframe tuples.

    cluster: quasardb.Cluster
      Active connection to the QuasarDB cluster

    create: optional bool
      Whether to create the table. Defaults to False.

    shard_size: optional datetime.timedelta
      The shard size of the timeseries you wish to create when `create` is True.
    """

    # If dfs is a dict, we convert it to a list of tuples.
    if isinstance(dfs, dict):
        dfs = dfs.items()

    if shard_size is not None and create == False:
        raise ValueError("Invalid argument: shard size provided while create is False")

    # If the tables are provided as strings, we look them up.
    dfs_ = []
    for (table, df) in dfs:
        if isinstance(table, str):
            table = table_cache.lookup(table, cluster)

        dfs_.append((table, df))

    data_by_table = []

    for table, df in dfs_:
        logger.debug("quasardb.pandas.write_dataframe, create = %s", create)
        assert isinstance(df, pd.DataFrame)

        # Create table if requested
        if create:
            _create_table_from_df(df, table, shard_size)

        cinfos = [(x.name, x.type) for x in table.list_columns()]

        if not df.index.is_monotonic_increasing:
            logger.warn("dataframe index is unsorted, resorting dataframe based on index")
            df = df.sort_index().reindex()

        # We pass everything else to our qdbnp.write_arrays function, as generally speaking
        # it is (much) more sensible to deal with numpy arrays than Pandas dataframes:
        # pandas has the bad habit of wanting to cast data to different types if your data
        # is sparse, most notably forcing sparse integer arrays to floating points.

        data = _extract_columns(df, cinfos)
        data['$timestamp'] = df.index.to_numpy(copy=False,
                                               dtype='datetime64[ns]')

        data_by_table.append((table, data))

    return qdbnp.write_arrays(data_by_table, cluster,
                              table=None,
                              index=None,
                              **kwargs)

def write_dataframe(
        df,
        cluster,
        table,
        **kwargs):
    """
    Store a single dataframe into a table. Takes the same arguments as `write_dataframes`, except only
    a single df/table combination.
    """
    write_dataframes([(table, df)], cluster, **kwargs)


def write_pinned_dataframe(*args, **kwargs):
    """
    Legacy wrapper around write_dataframe()
    """
    logger.warn("write_pinned_dataframe is deprecated and will be removed in a future release.")
    logger.warn("Please use write_dataframe directly instead")
    return write_dataframe(*args, **kwargs)


def _create_table_from_df(df, table, shard_size=None):
    cols = list()

    dtypes = _get_inferred_dtypes(df)

    logger.info("got inferred dtypes: %s", dtypes)
    for c in df.columns:
        dt = dtypes[c]
        ct = _dtype_to_column_type(df[c].dtype, dt)
        logger.debug("probed pandas dtype %s to inferred dtype %s and map to quasardb column type %s", df[c].dtype, dt, ct)
        cols.append(quasardb.ColumnInfo(ct, c))

    try:
        if not shard_size:
            table.create(cols)
        else:
            table.create(cols, shard_size)
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
