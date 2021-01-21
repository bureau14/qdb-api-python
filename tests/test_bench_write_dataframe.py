
import numpy as np
import pandas as pd
import tempfile
import datetime
import time
import quasardb
import quasardb.pandas as qdbpd
import pyarrow.parquet as pq
import multiprocessing as mp
import hashlib
import pytest

cols = None
shard_size = None

def _init_table_creator():
    global cols
    global shard_size
    cols = [
        quasardb.ColumnInfo(quasardb.ColumnType.String, 'node_id'),
        quasardb.ColumnInfo(quasardb.ColumnType.Timestamp, 'event_id'),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, 'x'),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, 'y'),
    ]
    shard_size = 3600000

def _create_table(conn, table_name):
    t = conn.table(table_name)
    try:
        t.create(cols, shard_size=datetime.timedelta(milliseconds=shard_size))
    except:
        pass


def _create_tables(conn, prefix):
    print("Creating tables with prefix " + prefix)
    table_names = ["{}/{:04x}".format(prefix, i) for i in range(65536)]
    _init_table_creator()
    for table_name in table_names:
        _create_table(conn, table_name)

def _sensor_to_table(x):
    h = hashlib.sha1()

    xs = bytes(x, 'utf-8')
    h.update(xs)
    xs_ = h.digest()[0:4]
    return "{:02x}{:02x}".format(xs_[0], xs_[1])

def _parse_timestamp(str):
    return datetime.datetime.strptime(
        str, "%m/%d/%Y %H:%M:%S")

def _get_timestamps(event_id, sample_rate, sample_value_len):
    start = np.datetime64(event_id, 'ns')
    step = np.timedelta64(int(round(1 / sample_rate * 1000000000)), 'ns')
    stop = start + step * sample_value_len
    return np.arange(start=start, stop=stop, step=step, dtype='datetime64[ns]')


def _parse_row(row, node_id, axis):

    size = len(row.sample_value)

    timestamps = _get_timestamps(row.event_id, row.sample_rate, size)
    node_ids = np.full(size, node_id)
    event_ids = np.full(size, row.event_id)
    values = row.sample_value * 10000
    nans = np.full(size, np.nan)
    x = values if axis == 'X' else nans
    y = values if axis == 'Y' else nans

    dct = {
        'timestamp': timestamps,
        'node_id': node_ids,
        'event_id': event_ids,
        'x': x,
        'y': y,
    }
    row_df = pd.DataFrame(dct)
    row_df.set_index('timestamp', inplace=True)

    return row_df

def _parse_df(node_df, node_id, axis):

    if type(node_df) == pd.Series:
        df = _parse_row(node_df, node_id, axis)
    else:
        dfs = []
        for _, row in node_df.iterrows():
            dfs.append(_parse_row(row, node_id, axis))
        df = pd.concat(dfs)

    return df

def _import_df(df, conn, table_name):

    attemps_left = 10
    while attemps_left >= 0:
        try:
            qdbpd.write_pinned_dataframe_with_none(df, conn, table_name, fast=True)
            break
        except Exception as e:
            print("Error:", e)
            attemps_left -= 1
            time.sleep(3)

@pytest.mark.skip(reason="Skip unless you're benching the pinned writer")
def test_process_file(qdbd_connection):
    prefix = 'test'
    create_table_start = time.time()
    _create_tables(qdbd_connection, prefix)
    create_table_time = time.time() - create_table_start

    read_from_parquet_start = time.time()
    file = "tests/pulp.parquet"
    table = pq.read_table(file)
    df = table.to_pandas()
    read_from_parquet_time = time.time() - read_from_parquet_start
    
    print(f'{__name__}:')
    print(f'  - table creation:     {create_table_time}s')
    print(f'  - parquet read:     {read_from_parquet_time}s')

    df['event_id'] = df['reading_datetime_utc'].map(_parse_timestamp)
    df = df[['axis', 'node_id', 'event_id',
                 'sample_rate', 'sample_value']]
    df.set_index(['axis', 'node_id'], inplace=True)

    parse_time = 0.0
    insert_time = 0.0
    total_start = time.time()
    for axis in ['X', 'Y']:
        try:
            axis_df = df.loc[axis]
        except:
            continue
        node_ids = axis_df.index.unique().tolist()
        print("node_ids count: {}".format(len(node_ids)))
        for node_id in node_ids:
            try:
                node_df = axis_df.loc[node_id]
            except:
                continue
            table_name = "{}/{}".format(prefix,
                                        _sensor_to_table(node_id))
            
            parse_start = time.time()
            table_df = _parse_df(node_df, node_id, axis)
            parse_time = parse_time + (time.time() - parse_start)
            insert_start = time.time()
            _import_df(table_df, qdbd_connection, table_name)
            one_insert_time = time.time() - insert_start
            insert_time = insert_time + one_insert_time

    total_time = time.time() - total_start

    print(f'Results:')
    print(f'  - parse:     {parse_time}s')
    print(f'  - insert:    {insert_time}s')
    print(f'  - total: {total_time}s')
