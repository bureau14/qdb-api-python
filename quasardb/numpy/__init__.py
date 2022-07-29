# pylint: disable=C0103,C0111,C0302,R0903

# Copyright (c) 2009-2022, quasardb SAS. All rights reserved.
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

logger = logging.getLogger('quasardb.numpy')


class NumpyRequired(ImportError):
    """
    Exception raised when trying to use QuasarDB pandas integration, but
    pandas has not been installed.
    """
    pass


try:
    import numpy as np
    import numpy.ma as ma

except ImportError as err:
    logger.exception(err)
    raise NumpyRequired(
        "The numpy library is required to handle numpy arrays formats"
        ) from err


class IncompatibleDtypeError(TypeError):
    """
    Exception raised when a provided dtype is not the expected dtype.
    """

    def __init__(self, cname=None, ctype=None, expected=None, provided=None):
        self.cname = cname
        self.ctype = ctype
        self.expected = expected
        self.provided = provided
        super().__init__(self.msg())

    def msg(self):
        return "Data for column '{}' with type '{}' was provided in dtype '{}' but need '{}'.".format(self.cname, self.ctype, self.provided, self.expected)


class IncompatibleDtypeErrors(TypeError):
    """
    Wraps multiple dtype errors
    """
    def __init__(self, xs):
        self.xs = xs
        super().__init__(self.msg())

    def msg(self):
        return "\n".join(x.msg() for x in self.xs)

# Based on QuasarDB column types, which dtype do we accept?
# First entry will always be the 'preferred' dtype, other ones
# those that we can natively convert in native code.
_ctype_to_dtype = {
    quasardb.ColumnType.String: [np.dtype(np.unicode_)],
    quasardb.ColumnType.Symbol: [np.dtype(np.unicode_)],
    quasardb.ColumnType.Int64: [np.dtype(np.int64), np.dtype(np.int32), np.dtype(np.int16)],
    quasardb.ColumnType.Double: [np.dtype(np.float64), np.dtype(np.float32)],
    quasardb.ColumnType.Blob: [np.dtype('S'), np.dtype('O')],
    quasardb.ColumnType.Timestamp: [np.dtype('datetime64[ns]')]
}


def _best_dtype_for_ctype(ctype: quasardb.quasardb.ColumnType):
    """
    Returns the 'best' DType for a certain column type. For example, for blobs, even
    though we accept py::bytes, prefer bytestrings (as they are faster to read in c++).
    """
    possible_dtypes = _ctype_to_dtype[ctype]
    assert len(possible_dtypes) > 0

    # By convention, the first entry is the preferred one
    return possible_dtypes[0]


def _coerce_dtype(dtype, columns):
    if dtype is None:
        dtype = [None] * len(columns)

    if isinstance(dtype, np.dtype):
        dtype = [dtype]

    if type(dtype) is dict:

        # Conveniently look up column index by label
        offsets = {}
        for i in range(len(columns)):
            (cname, ctype) = columns[i]
            offsets[cname] = i

        # Now convert the provided dtype dict to a list that matches
        # the relative offset within the table.
        #
        # Any columns not provided will have a 'None' dtype.
        dtype_ = [None] * len(columns)

        for (k, dt) in dtype.items():
            if not k in offsets:
                logger.warn("Forced dtype provided for column '%s' = %s, but that column is not found in the table. Skipping...", k, )

            i = offsets[k]
            dtype_[i] = dt

        dtype = dtype_

    if type(dtype) is not list:
        raise ValueError("Forced dtype argument provided, but the argument has an incompatible type. Expected: list-like or dict-like, got: {}".format(type(dtype)))

    if len(dtype) is not len(columns):
        raise ValueError("Expected exactly one dtype for each column, but %d dtypes were provided for %d columns".format(len(dtype), len(columns)))

    return dtype




def _add_desired_dtypes(dtype, columns):
    """
    When infer_types=True, this function sets the 'desired' dtype for each of the columns.
    `dtype` is expected to be the output of `_coerce_dtype`, that is, a list-like with an
    entry for each column.
    """
    assert len(dtype) == len(columns)

    for i in range(len(dtype)):
        # No dtype explicitly provided by the user, otherwise we don't touch it
        if dtype[i] is None:
            (cname, ctype) = columns[i]
            dtype_ = _best_dtype_for_ctype(ctype)
            logger.debug("using default dtype '%s' for column '%s' with type %s", dtype_, cname, ctype)
            dtype[i] = dtype_

    return dtype


def _is_all_masked(xs):
    if ma.isMA(xs):
        return ma.size(xs) == ma.count_masked(xs)

    if np.size(xs) == 0:
        # Empty lists are considered "masked"
        return True

    if xs.dtype == np.object_ and xs[0] is None:
        # Very likely that all is None at this point, we can try a more "expensive"
        # probing function
        #
        # We'll defer to the Python `all` function, as Numpy doesn't really have great
        # built-ins for object arrays
        return all(x is None for x in xs)


    logger.debug("{} is not a masked array, not convertible to requested type... ".format(type(xs)))

    # This array is *not* a masked array, it's *not* convertible to the type we want,
    # and it's *not* an object array.
    #
    # The best we can do at this point is emit a warning and defer to the (expensive)
    # python-based function as well.
    return all(x is None for x in xs)


def dtypes_equal(lhs, rhs):
    if lhs.kind == 'U' or lhs.kind == 'S':
        # Unicode and string data has variable length encoding, which means their itemsize
        # can be anything.
        #
        # We only care about dtype kind in this case.
        return lhs.kind == rhs.kind

    return lhs == rhs


def _dtype_found(needle, haystack):
    """
    Returns True if one of the dtypes in `haystack` matches that of `needle`.
    """
    for x in haystack:
        if dtypes_equal(needle, x) is True:
            return True

    return False


def _validate_dtypes(data, columns):
    errors = list()

    def _error_to_msg(e):
        (cname, ctype, provided_dtype, expected_dtype) = e
        return

    for (data_, (cname, ctype)) in zip(data, columns):
        expected_ = _ctype_to_dtype[ctype]

        logger.debug("data_.dtype = %s, expected_ = %s", data_.dtype, expected_)

        if _dtype_found(data_.dtype, expected_) == False:
            errors.append(IncompatibleDtypeError(cname=cname, ctype=ctype, provided=data_.dtype, expected=expected_))

    if len(errors) > 0:
        raise IncompatibleDtypeErrors(errors)


def _validate_drop_duplicates(drop_duplicates, columns):
    """
    Throws an error when 'drop_duplicates' refers to a column not present in the table.
    """
    if isinstance(drop_duplicates, bool):
        return True

    if not isinstance(drop_duplicates, list):
        raise TypeError("drop_duplicates should be either a bool or a list, got: " + type(drop_duplicates))

    cnames = [cname for (cname, ctype) in columns]

    for column_name in drop_duplicates:
        if not column_name in cnames:
            raise RuntimeError("Provided deduplication column name '{}' not found in table columns.".format(column_name))


    return True

def _coerce_data(data, dtype):
    """
    Coerces each numpy array of `data` to the dtype present in `dtype`.
    """

    assert len(data) == len(dtype)

    for i in range(len(data)):
        dtype_ = dtype[i]
        data_ = data[i]

        if dtype_ is not None and dtypes_equal(data_.dtype, dtype_) == False:
            logger.debug("data for column with offset %d was provided in dtype '%s', but need '%s': converting data...", i, data_.dtype, dtype_)

            logger.debug("type of data[%d] after: %s", i, type(data[i]))
            logger.debug("size of data[%d] after: %s", i, ma.size(data[i]))
            logger.debug("data of data[%d] after: %s", i, data[i])

            try:
                data[i] = data_.astype(dtype_)
            except TypeError as err:
                # One 'bug' is that, if everything is masked, the underlying data type can be
                # pretty much anything.
                if not _is_all_masked(data_):
                    logger.error("An error occured while coercing input data type from dtype '%s' to dtype '%s': ", data_.dtype, dtype_)
                    logger.exception(err)
                    raise err

                logger.debug("array completely empty, re-initializing to empty array of '%s'", dtype_)
                data[i] = ma.masked_all(ma.size(data_),
                                        dtype=dtype_)
            assert data[i].dtype.kind == dtype_.kind

            logger.debug("type of data[%d] after: %s", i, type(data[i]))
            logger.debug("size of data[%d] after: %s", i, ma.size(data[i]))
            logger.debug("data of data[%d] after: %s", i, data[i])
            assert ma.size(data[i]) == ma.size(data_)

    return data

def _probe_length(xs):
    """
    Returns the length of the first non-null array in `xs`, or None if all arrays
    are null.
    """
    if isinstance(xs, dict):
        return _probe_length(xs.values())

    for x in xs:
        if x is not None:
            return x.size

    return None

def _ensure_list(xs, cinfos):
    """
    If input data is a dict, ensures it's converted to a list with the correct
    offsets.
    """
    if isinstance(xs, list):
        return xs

    # As we only accept list-likes or dicts as input data, it *must* be a dict at this
    # point
    assert isinstance(xs, dict)

    logger.debug("data was provided as dict, coercing to list")

    # As we may have non-existing keys, we would like to initialize those as a masked
    # array with all elements masked. In those cases, though, we need to know the size
    # of the array.
    n = _probe_length(xs)

    ret = list()

    for i in range(len(cinfos)):
        (cname, ctype) = cinfos[i]

        xs_ = None
        if cname in xs:
            xs_ = xs[cname]
        else:
            xs_ = ma.masked_all(n, dtype=_best_dtype_for_ctype(ctype))

        ret.append(xs_)

    return ret


def ensure_ma(xs, dtype=None):
    if isinstance(dtype, list):
        assert(isinstance(xs, list) == True)
        return [ensure_ma(xs_, dtype_) for (xs_, dtype_) in zip(xs, dtype)]

    # Don't bother if we're already a masked array
    if ma.isMA(xs):
        return xs

    if not isinstance(xs, np.ndarray):
        logger.debug("Provided data is not a numpy array: %s", type(xs))
        xs = np.array(xs, dtype=dtype)

    logger.debug("coercing array with dtype: %s", xs.dtype)

    if xs.dtype.kind in ['O', 'U', 'S']:
        logger.debug("Data is object-like, masking None values")

        mask = xs == None
        return ma.masked_array(data=xs, mask=mask)
    else:
        logger.debug("Automatically masking invalid numbers")
        return ma.masked_invalid(xs, copy=False)


def read_array(table=None,
               column=None,
               ranges=None):
    if table is None:
        raise RuntimeError("A table is required.")

    if column is None:
        raise RuntimeError("A column is required.")

    kwargs = {
        'column': column
    }

    if ranges is not None:
        kwargs['ranges'] = ranges

    read_with = {
        quasardb.ColumnType.Double: table.double_get_ranges,
        quasardb.ColumnType.Blob: table.blob_get_ranges,
        quasardb.ColumnType.String: table.string_get_ranges,
        quasardb.ColumnType.Symbol: table.string_get_ranges,
        quasardb.ColumnType.Int64: table.int64_get_ranges,
        quasardb.ColumnType.Timestamp: table.timestamp_get_ranges,
    }

    ctype = table.column_type_by_id(column)

    fn = read_with[ctype]
    return fn(**kwargs)


def write_array(
        data=None,
        index=None,
        table=None,
        column=None,
        dtype=None,
        infer_types=True):
    """
    Write a Numpy array to a single column.

    Parameters:
    -----------

    data: np.array
      Numpy array with a dtype that is compatible with the column's type.

    index: np.array
      Numpy array with a datetime64[ns] dtype that will be used as the
      $timestamp axis for the data to be stored.

    dtype: optional np.dtype
      If provided, ensures the data array is converted to this dtype before
      insertion.

    infer_types: optional bool
      If true, when necessary will attempt to convert the data and index array
      to the best type for the column. For example, if you provide float64 data
      while the column's type is int64, it will automatically convert the data.

      Defaults to True. For production use cases where you want to avoid implicit
      conversions, we recommend always setting this to False.

    """

    if table is None:
        raise RuntimeError("A table is required.")

    if column is None:
        raise RuntimeError("A column is required.")

    if data is None:
        raise RuntimeError("A data numpy array is required.")

    if index is None:
        raise RuntimeError("An index numpy timestamp array is required.")


    data = ensure_ma(data, dtype=dtype)
    ctype =  table.column_type_by_id(column)

    # We try to reuse some of the other functions, which assume array-like
    # shapes for column info and data. It's a bit hackish, but actually works
    # well.
    #
    # We should probably generalize this block of code with the same found in
    # write_arrays().

    cinfos = [(column, ctype)]
    dtype_ = [dtype]

    dtype = _coerce_dtype(dtype_, cinfos)

    if infer_types is True:
        dtype = _add_desired_dtypes(dtype, cinfos)

    # data_ = an array of [data]
    data_ = [data]
    data_ = _coerce_data(data_, dtype)
    _validate_dtypes(data_, cinfos)

    # No functions that assume array-of-data anymore, let's put it back
    data = data_[0]

    # Dispatch to the correct function
    write_with = {
        quasardb.ColumnType.Double: table.double_insert,
        quasardb.ColumnType.Blob: table.blob_insert,
        quasardb.ColumnType.String: table.string_insert,
        quasardb.ColumnType.Symbol: table.string_insert,
        quasardb.ColumnType.Int64: table.int64_insert,
        quasardb.ColumnType.Timestamp: table.timestamp_insert,
    }

    logger.info("Writing array (%d rows of dtype %s) to columns %s.%s (type %s)", len(data), data.dtype, table.get_name(), column, ctype)
    write_with[ctype](column, index, data)


def write_arrays(
        data,
        cluster,
        table,
        dtype = None,
        index = None,
        _async = False,
        fast = False,
        truncate = False,
        drop_duplicates=False,
        infer_types = True,
        writer = None):
    """
    Write multiple aligned numpy arrays to a table.

    Parameters:
    -----------

    data: Iterable of np.array, or dict-like of str:np.array
      Numpy arrays to write into the database. Can either be a list of numpy arrays,
      in which case they are expected to be in the same order as table.list_columns(),
      and an array is provided for each of the columns. If `index` is None, the first
      array will be assumed to be an index with dtype `datetime64[ns]`.

      Alternatively, a dict of key/values may be provided, where the key is expected
      to be a table column label, and the value is expected to be a np.array. If present,
      a column with label '$timestamp' will be used as the index.

      In all cases, all numpy arrays are expected to be of exactly the same length as the
      index.

    cluster: quasardb.Cluster
      Active connection to the QuasarDB cluster

    table: quasardb.Table or str
      Either a string or a reference to a QuasarDB Timeseries table object.
      For example, 'my_table' or cluster.table('my_table') are both valid values.

      Defaults to False.

    index: optional np.array with dtype datetime64[ns]
      Optionally explicitly provide an array as the $timestamp index. If not provided,
      the first array provided to `data` will be used as the index.

    dtype: optional dtype, list of dtype, or dict of dtype
      Optional data type to force. If a single dtype, will force that dtype to all
      columns. If list-like, will map dtypes to dataframe columns by their offset.
      If dict-like, will map dtypes to dataframe columns by their label.

      If a dtype for a column is provided in this argument, and infer_types is also
      True, this argument takes precedence.

    drop_duplicates: bool or list[str]
      Enables server-side deduplication of data when it is written into the table.
      When True, automatically deduplicates rows when all values of a row are identical.
      When a list of strings is provided, deduplicates only based on the values of
      these columns.

    Defaults to False.

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

    writer: optional quasardb.PinnedWriter
      Allows you to explicitly provide a PinnedWriter to use, which is expected to be
      initialized with the `table`.

      Reuse of the PinnedWriter allows for some performance improvements.

    """

    # Acquire reference to table if string is provided
    if isinstance(table, str):
        table = cluster.table(table)

    # Create batch column info from dataframe
    if writer is None:
        writer = cluster.pinned_writer(table)

    cinfos = [(x.name, x.type) for x in writer.column_infos()]
    dtype = _coerce_dtype(dtype, cinfos)

    assert type(dtype) is list
    assert len(dtype) is len(cinfos)

    if infer_types is True:
        dtype = _add_desired_dtypes(dtype, cinfos)

    data = _ensure_list(data, cinfos)
    data = ensure_ma(data, dtype=dtype)
    data = _coerce_data(data, dtype)


    # Just some additional friendly information about incorrect dtypes, we'd
    # prefer to have this information thrown from Python instead of native
    # code as it generally makes for somewhat better error context.
    _validate_dtypes(data, cinfos)

    _validate_drop_duplicates(drop_duplicates, cinfos)

    write_with = {
        quasardb.ColumnType.Double: writer.set_double_column,
        quasardb.ColumnType.Blob: writer.set_blob_column,
        quasardb.ColumnType.String: writer.set_string_column,
        quasardb.ColumnType.Symbol: writer.set_string_column,
        quasardb.ColumnType.Int64: writer.set_int64_column,
        quasardb.ColumnType.Timestamp: writer.set_timestamp_column
    }

    assert len(data) == len(cinfos)

    writer.set_index(index)

    for i in range(len(data)):
        (cname, ctype) = cinfos[i]

        if data[i] is not None:
            write_with[ctype](i, data[i])


    logger.debug("pushing %d rows", len(index))
    start = time.time()

    if fast is True:
        writer.push_fast(drop_duplicates=drop_duplicates)
    elif truncate is True:
        writer.push_truncate(drop_duplicates=drop_duplicates)
    elif isinstance(truncate, tuple):
        writer.push_truncate(range=truncate, drop_duplicates=drop_duplicates)
    elif _async is True:
        writer.push_async(drop_duplicates=drop_duplicates)
    else:
        writer.push(drop_duplicates=drop_duplicates)

    logger.debug("pushed %d rows in %s seconds",
                 len(index), (time.time() - start))

    return table


def _xform_query_results(xs, index, dict):
    if len(xs) == 0:
        return (np.array([], np.dtype('datetime64[ns]')), np.array([]))

    n = None
    for x in xs:
        assert isinstance(x, tuple)

        if n is None:
            n = x[1].size
        else:
            assert x[1].size == n

    if index is None:
        # Generate a range, put it in the front of the result list,
        # recurse and tell the function to use that index.
        xs_ = [('$index', np.arange(n))] + xs
        return _xform_query_results(xs_, '$index', dict)

    if isinstance(index, str):
        for i in range(len(xs)):
            (cname, _) = xs[i]
            if cname == index:
                # Now we know that this column has offset `i`,
                # recurse with that offset
                return _xform_query_results(xs, i, dict)

        raise KeyError("Unable to resolve index column: column not found in results: {}".format(index))

    if not isinstance(index, int):
        raise TypeError("Unable to resolve index column: unrecognized type {}: {}".format(type(index), index))

    idx = xs[index][1]
    del xs[index]

    # Our index *may* be a masked array, but if it is, there should be no
    # masked items: we cannot not have an index for a certain row.
    if ma.isMA(idx):
        if ma.count_masked(idx) > 0:
            raise ValueError("Invalid index: null values detected. An index is never allowed to have null values.")

        assert isinstance(idx.data, np.ndarray)
        idx = idx.data

    xs_ = None

    if dict is True:
        xs_ = {x[0]: x[1] for x in xs}
    else:
        xs_ = [x[1] for x in xs]

    return (idx, xs_)


def query(cluster,
          query,
          index=None,
          dict=False):
    """
    Execute a query and return the results as numpy arrays. The shape of the return value
    is always:

      tuple[index, dict | list[np.array]]


    If `dict` is True, constructs a dict[str, np.array] where the key is the column name.
    Otherwise, it returns a list of all the individual data arrays.



    Parameters:
    -----------

    cluster : quasardb.Cluster
      Active connection to the QuasarDB cluster

    query : str
      The query to execute.

    index : optional[str | int]
      If provided, resolves column and uses that as the index. If string (e.g. `$timestamp`), uses
      that column as the index. If int (e.g. `1`), looks up the column based on that offset.

    dict : bool
      If true, returns data arrays as a dict, otherwise a list of np.arrays.
      Defaults to False.

    """

    m = {}
    xs = cluster.query_numpy(query)

    return _xform_query_results(xs, index, dict)
