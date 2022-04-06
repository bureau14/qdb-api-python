# pylint: disable=C0103,C0111,C0302,W0212
import json
import random
import string
import pytest
import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime

from functools import partial

import quasardb
import quasardb.pandas as qdbpd

def connect(uri):
    return quasardb.Cluster(uri)


def direct_connect(conn, node_uri):
    return conn.node(node_uri)


def config():
    return {"uri":
            {"insecure": "qdb://127.0.0.1:2836",
             "secure": "qdb://127.0.0.1:2838"}}

@pytest.fixture
def qdbd_settings(scope="module"):
    user_key = {}
    cluster_key = ""

    with open('user_private.key', 'r') as user_key_file:
        user_key = json.load(user_key_file)
    with open('cluster_public.key', 'r') as cluster_key_file:
        cluster_key = cluster_key_file.read()
    return {
        "uri": {
            "insecure": "qdb://127.0.0.1:2836",
            "secure": "qdb://127.0.0.1:2838"},
        "security": {
            "user_name": user_key['username'],
            "user_private_key": user_key['secret_key'],
            "cluster_public_key": cluster_key}}


@pytest.fixture(scope="function")
def qdbd_connection(qdbd_settings):
    conn = connect(qdbd_settings.get("uri").get("insecure"))
    yield conn
    conn.purge_all(datetime.timedelta(minutes=1))

@pytest.fixture(scope="function")
def qdbd_secure_connection(qdbd_settings):
    conn = quasardb.Cluster(
        uri=qdbd_settings.get("uri").get("secure"),
        user_name=qdbd_settings.get("security").get("user_name"),
        user_private_key=qdbd_settings.get("security").get("user_private_key"),
        cluster_public_key=qdbd_settings.get("security").get("cluster_public_key"))
    yield conn
    conn.purge_all(datetime.timedelta(minutes=1))



@pytest.fixture
def qdbd_direct_connection(qdbd_settings, qdbd_connection):
    return direct_connect(qdbd_connection, qdbd_settings.get(
        "uri").get("insecure").replace("qdb://", ""))


@pytest.fixture
def qdbd_direct_secure_connection(qdbd_settings, qdbd_secure_connection):
    return direct_connect(
        qdbd_secure_connection,
        qdbd_settings.get("uri").get("secure").replace(
            "qdb://",
            ""))


def _random_identifier():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(16))

def _random_string():
    return ''.join(
        random.choice(
            string.ascii_uppercase +
            string.digits) for _ in range(16))

@pytest.fixture
def random_identifier():
    return _random_identifier()

@pytest.fixture
def random_string():
    return _random_string()

@pytest.fixture
def column_name():
    return _random_identifier()

@pytest.fixture
def symbol_table_name():
    return _random_identifier()

@pytest.fixture
def tag_name():
    return _random_identifier()

@pytest.fixture
def tag_names():
    return sorted([''.join(random.choice(string.ascii_lowercase)
                           for _ in range(16)) for _ in range(10)])

@pytest.fixture
def entry_name(random_string):
    return _random_string()

@pytest.fixture
def random_blob(random_string):
    return np.random.bytes(16)

@pytest.fixture
def random_integer():
    return random.randint(-1000000000, 1000000000)


@pytest.fixture
def blob_entry(qdbd_connection, entry_name):
    return qdbd_connection.blob(entry_name)


@pytest.fixture
def integer_entry(qdbd_connection, entry_name):
    return qdbd_connection.integer(entry_name)

@pytest.fixture
def table_name(entry_name):
    return entry_name

def _create_table(c, table_name):
    t = c.table(table_name)
    double_col = quasardb.ColumnInfo(quasardb.ColumnType.Double, "the_double")
    blob_col = quasardb.ColumnInfo(quasardb.ColumnType.Blob, "the_blob")
    string_col = quasardb.ColumnInfo(quasardb.ColumnType.String, "the_string")
    int64_col = quasardb.ColumnInfo(quasardb.ColumnType.Int64, "the_int64")
    ts_col = quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, "the_ts")
    symbol_col = quasardb.ColumnInfo(
        quasardb.ColumnType.Symbol, "the_symbol", "symtable")

    t.create([double_col, blob_col, string_col, int64_col, ts_col, symbol_col])
    return t

@pytest.fixture
def table(qdbd_connection, entry_name):
    return _create_table(qdbd_connection, entry_name)


@pytest.fixture
def secure_table(qdbd_secure_connection, entry_name):
    return _create_table(qdbd_secure_connection, entry_name)


@pytest.fixture
def intervals():
    start_time = np.datetime64('2017-01-01', 'ns')
    return [(start_time, start_time + np.timedelta64(1, 's'))]


def create_many_intervals():
    start_time = np.datetime64('2017-01-01', 'ns')
    return np.array([(start_time + np.timedelta64(i, 's'))
                     for i in range(10000)]).astype('datetime64[ns]')


@pytest.fixture
def many_intervals():
    return create_many_intervals()

@pytest.fixture(params=[256])
def row_count(request):
    yield request.param

@pytest.fixture(params=[1, 2, 4, 8, 16])
def df_count(request):
    yield request.param

def _sparsify(perc, xs):
    # Randomly make a bunch of elements null, we make use of Numpy's masked
    # arrays for this: keep track of a separate boolean 'mask' array, which
    # determines whether or not an item in the array is None.
    #
    # `perc` denotes the percentage of "sparsity", where 100 means no None elements,
    # and 0 means everything is None
    assert perc <= 100
    assert perc >= 0
    assert type(perc) == int

    n = len(xs)

    if perc == 0:
        # When everything is 0, don't use a masked array; it's more "intrusive" if we
        # would just literally return an array with nulls, we don't need a mask here.
        return np.full(n, None)
    elif perc == 100:
        return xs

    choices = np.full(100, True)
    choices[:perc] = False

    mask = np.random.choice(a=choices, size=n)
    return ma.masked_array(data=xs,
                           mask=mask)

no_sparsify = [lambda x: x]

@pytest.fixture(params=[100, 50, 0])
def sparsify(request):
    return partial(_sparsify, request.param)

def _gen_floating(n, low=-100.0, high=100.0):
    return np.random.uniform(low, high, n)

def _gen_float64(n, low=-100.0, high=100.0):
    return _gen_floating(n, low=low, high=high).astype(np.float64)

def _gen_float32(n, low=-100.0, high=100.0):
    return _gen_floating(n, low=low, high=high).astype(np.float32)

def _gen_float16(n, low=-100.0, high=100.0):
    return _gen_floating(n, low=low, high=high).astype(np.float16)

def _gen_integer(n):
    return np.random.randint(-100, 100, n)

def _gen_int64(n):
    return _gen_integer(n).astype(np.int64)

def _gen_int32(n):
    return _gen_integer(n).astype(np.int32)

def _gen_int16(n):
    return _gen_integer(n).astype(np.int16)

def _gen_string(n):
    # Slightly hacks, but for testing purposes the contents of the strings
    # being floats allows us to always cast these strings to other types as
    # well.
    return list(str(x) for x in _gen_floating(n, low=0))


def _gen_blob(n):
    return list(x.encode("utf-8") for x in _gen_string(n))

def _gen_datetime64(n, precision):
    start_time = np.datetime64('2017-01-01', precision)
    return np.array([(start_time + np.timedelta64(i, 's'))
                     for i in range(n)]).astype('datetime64[{}]'.format(precision))

def _gen_datetime64_s(n):
    return _gen_datetime64(n, 's')

def _gen_datetime64_ms(n):
    return _gen_datetime64(n, 'ms')

def _gen_datetime64_ns(n):
    return _gen_datetime64(n, 'ns')

def _gen_timestamp(n):
    return _gen_datetime64_ns(n)

_dtype_to_generator = {np.float64: _gen_float64,
                       np.float32: _gen_float32,
                       np.float16: _gen_float16,
                       np.int64: _gen_int64,
                       np.int32: _gen_int32,
                       np.int16: _gen_int16,
                       np.unicode_: _gen_string,
                       np.bytes_: _gen_blob,
                       np.dtype('datetime64[ns]'): _gen_datetime64_ns,
                       np.dtype('datetime64[ms]'): _gen_datetime64_ms,
                       np.dtype('datetime64[s]'): _gen_datetime64_s}


_dtype_to_column_type = {np.int64: quasardb.ColumnType.Int64,
                         np.int32: quasardb.ColumnType.Int64,
                         np.int16: quasardb.ColumnType.Int64,
                         np.float64: quasardb.ColumnType.Double,
                         np.float32: quasardb.ColumnType.Double,
                         np.float16: quasardb.ColumnType.Double,
                         np.unicode_: quasardb.ColumnType.String,
                         np.bytes_: quasardb.ColumnType.Blob,
                         np.dtype('datetime64[ns]'): quasardb.ColumnType.Timestamp,
                         np.dtype('datetime64[ms]'): quasardb.ColumnType.Timestamp,
                         np.dtype('datetime64[s]'): quasardb.ColumnType.Timestamp}

# all dtypes that we support 'natively', that is, writing to qdb & reading them back
# is guaranteed to yield the exact same value.
native_dtypes = [np.float64,
                 np.int64,
                 np.unicode_,
                 np.bytes_,
                 np.dtype('datetime64[ns]')]

# dtypes which we support, but should not necessarily yield the exact same result type.
inferrable_dtypes = [np.float32,
                     np.float16,
                     np.int32,
                     np.int16,
                     np.dtype('datetime64[ms]'),
                     np.dtype('datetime64[s]')]

all_dtypes = [*native_dtypes, *inferrable_dtypes]

@pytest.fixture(params=all_dtypes)
def gen_dtype(request):
    yield request.param

gen_native_dtype = native_dtypes
gen_inferrable_dtype = inferrable_dtypes

def is_native_dtype(dt):
    return dt in native_dtypes

def is_inferrable_dtype(dt):
    return dt in inferrable_dtypes0

def _gen_array(dtype, sparsify, row_count):
    assert dtype in _dtype_to_generator
    fn = _dtype_to_generator[dtype]

    xs = fn(row_count)
    xs = sparsify(xs)
    return (dtype, xs)

@pytest.fixture(params=[np.datetime64('2017-01-01', 'ns')])
def start_date(request):
    yield request.param

@pytest.fixture
def gen_array(gen_dtype, sparsify, row_count):
    return _gen_array(gen_dtype, sparsify, row_count)

def _do_gen_df(gen_array, start_date, row_count, column_name):
    (dtype, xs) = gen_array

    idx = pd.Index(pd.date_range(start_date, periods=row_count, freq='S'),
                   name='$timestamp')
    return (dtype, pd.DataFrame(data={column_name: xs},
                                index=idx))

@pytest.fixture
def gen_df_fn(gen_array, start_date, row_count, column_name):
    return partial(_do_gen_df, start_date, row_count, column_name)

@pytest.fixture
def gen_df(gen_array, start_date, row_count, column_name):
    return _do_gen_df(gen_array, start_date, row_count, column_name)

@pytest.fixture
def df_with_table(qdbd_connection, table_name, column_name, gen_df):
    (dtype, df) = gen_df

    table = qdbd_connection.table(table_name)
    columnType = _dtype_to_column_type[dtype]

    columns = [quasardb.ColumnInfo(columnType, column_name)]
    table.create(columns)
    return (dtype, df, table)

@pytest.fixture(params=[quasardb.ColumnType.Int64,
                        quasardb.ColumnType.Double,
                        quasardb.ColumnType.Blob,
                        quasardb.ColumnType.String,
                        quasardb.ColumnType.Timestamp,
                        quasardb.ColumnType.Symbol], ids=['int64',
                                                          'double',
                                                          'blob',
                                                          'string',
                                                          'timestamp',
                                                          'symbol'])
def column_type(request):
    param = request.param
    yield param

@pytest.fixture(params=[qdbpd.write_dataframe])
def qdbpd_write_fn(request):
    yield request.param

def _query_style_numpy(conn, query):
    return qdbpd.query(conn, query, numpy=True)

def _query_style_regular(conn, query):
    return qdbpd.query(conn, query, numpy=False)

query_fns = {'query_numpy': _query_style_numpy,
             'query_regular': _query_style_regular}

@pytest.fixture(params=['query_numpy',
                        'query_regular'])
def qdbpd_query_fn(request):
    yield query_fns[request.param]

@pytest.fixture
def column_info(column_name, column_type, symbol_table_name):
    if column_type == quasardb.ColumnType.Symbol:
        return quasardb.ColumnInfo(column_type, column_name, symbol_table_name)

    return quasardb.ColumnInfo(column_type, column_name)

@pytest.fixture
def table_1col(qdbd_connection, table_name, column_info):
    t = qdbd_connection.ts(table_name)
    t.create([column_info])
    return t
