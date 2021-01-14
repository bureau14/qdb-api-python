# pylint: disable=C0103,C0111,C0302,W0212
from builtins import range as xrange, int as long  # pylint: disable=W0622
from functools import reduce  # pylint: disable=W0622
import datetime
import test_table as tslib
from time import sleep
import time
import random

import pytest
import quasardb
import numpy as np

table_count, column_count, row_factor = 100, 6, 10000
row_count = table_count * row_factor

def make_tables(qdbd_connection):
    table_creation_start = time.time()
    for i in range(table_count):
        name = 'table_%s' % (i,)
        cols = [quasardb.ColumnInfo(quasardb.ColumnType.Int64, 'col_{}'.format(col_idx)) for col_idx in range(column_count)]
        qdbd_connection.query("DROP TABLE IF EXISTS {}".format(name))
        t = qdbd_connection.table(name)
        t.create(columns=cols, shard_size=datetime.timedelta(seconds=60))
        t.attach_tag('test_tag')
    table_creation_time = time.time() - table_creation_start
    return table_creation_time

def make_batch_columns():
    batch_columns = [quasardb.BatchColumnInfo('table_{}'.format(tbl_idx), 'col_{}'.format(col_idx) , 1) for tbl_idx in range(table_count) for col_idx in range(column_count)]
    return batch_columns


def test_insert(qdbd_connection):
    table_creation_time = make_tables(qdbd_connection)

    inserter_creation_start = time.time()
    batch_columns = make_batch_columns()
    inserter = qdbd_connection.inserter(batch_columns)
    inserter_creation_time = time.time() - inserter_creation_start
    
    print(f'Testing insertion:')
    print(f'  - {table_count} table(s)')
    print(f'  - {column_count} column(s) per table')
    print(f'  - {row_count} row(s)')
    print(f'  - table creation:     {table_creation_time}s')
    print(f'  - inserters creation: {inserter_creation_time}s')

    total_insertion_start = time.time()
    bulk_writing_start = time.time()
    bulk_start_row_time = 0.0

    for row_idx in range(row_count):
        ts = int(time.time() * 1.0e9)
        bulk_start_row_start = time.time()
        inserter.start_row(ts)
        bulk_start_row_time = bulk_start_row_time + (time.time() - bulk_start_row_start)
        tbl_idx = row_idx % table_count
        for col_idx in range(column_count):
            inserter.set_int64(tbl_idx * column_count + col_idx, int(row_idx + random.random()))

    bulk_writing_time = time.time() - bulk_writing_start
    bulk_insert_start = time.time()
    inserter.push()
    bulk_insert_time = time.time() - bulk_insert_start
    total_insertion_time = time.time() - total_insertion_start
    
    res = qdbd_connection.query("SELECT count(col_0) FROM FIND(tag='test_tag')")
    print(f'Results:')
    print(f'  - batch start row:     {bulk_start_row_time}s')
    print(f'  - batch set values:    {bulk_writing_time}s')
    print(f'  - batch insert values: {bulk_insert_time}s')
    print(f'  - total insert time:   {total_insertion_time}s')
    print(f'  - rows inserted: {res[0]}')


def test_pinned_value_insert(qdbd_connection):

    print(f'Testing insertion:')
    print(f'  - {table_count} table(s)')
    print(f'  - {column_count} column(s) per table')
    print(f'  - {row_count} row(s)')
    
    table_creation_time = make_tables(qdbd_connection)

    inserter_creation_start = time.time()
    batch_columns = make_batch_columns()
    inserter = qdbd_connection.pinned_value_inserter(batch_columns)
    inserter_creation_time = time.time() - inserter_creation_start
    
    print(f'  - table creation:     {table_creation_time}s')
    print(f'  - inserters creation: {inserter_creation_time}s')

    total_insertion_start = time.time()
    bulk_writing_start = time.time()
    bulk_start_row_time = 0.0

    for row_index in range(row_count):
        ts = int(time.time() * 1.0e9)
        bulk_start_row_start = time.time()
        inserter.start_row(ts)
        bulk_start_row_time = bulk_start_row_time + (time.time() - bulk_start_row_start)
        tbl_idx = row_index % table_count
        for col_idx in range(column_count):
            inserter.set_int64(tbl_idx * column_count + col_idx, int(row_index + random.random()))

    bulk_writing_time = time.time() - bulk_writing_start
    bulk_insert_start = time.time()
    inserter.push()
    bulk_insert_time = time.time() - bulk_insert_start
    total_insertion_time = time.time() - total_insertion_start

    res = qdbd_connection.query("SELECT count(col_0) FROM FIND(tag='test_tag')")
    print(f'Results:')
    print(f'  - batch start row:     {bulk_start_row_time}s')
    print(f'  - batch set values:    {bulk_writing_time}s')
    print(f'  - batch insert values: {bulk_insert_time}s')
    print(f'  - total insert time:   {total_insertion_time}s')
    print(f'  - rows inserted: {res[0]}')

def test_pinned_column_insert(qdbd_connection):

    print(f'Testing insertion:')
    print(f'  - {table_count} table(s)')
    print(f'  - {column_count} column(s) per table')
    print(f'  - {row_count} row(s)')
    
    table_creation_time = make_tables(qdbd_connection)

    inserter_creation_start = time.time()
    batch_columns = make_batch_columns()
    inserter = qdbd_connection.pinned_inserter(batch_columns)
    inserter_creation_time = time.time() - inserter_creation_start
    
    print(f'  - table creation:     {table_creation_time}s')
    print(f'  - inserters creation: {inserter_creation_time}s')

    total_insertion_start = time.time()
    bulk_writing_start = time.time()
    bulk_start_row_time = 0.0
    timestamps = []
    values = []
    for tbl_idx in range(table_count):
        timestamps.append([])
        values.append([])
        for col_idx in range(column_count):
            timestamps[tbl_idx].append([])
            values[tbl_idx].append([])

    for row_idx in range(row_count):
        bulk_start_row_start = time.time()
        bulk_start_row_time = bulk_start_row_time + (time.time() - bulk_start_row_start)
        tbl_idx = row_idx % table_count
        for col_idx in range(column_count):
            timestamps[tbl_idx][col_idx].append(int(time.time() * 1.0e9))
            values[tbl_idx][col_idx].append(int(row_idx + random.random()))

    for tbl_idx in range(table_count):
        for col_idx in range(column_count):
            inserter.set_int64_column(tbl_idx * column_count + col_idx, timestamps[tbl_idx][col_idx], values[tbl_idx][col_idx])

    bulk_writing_time = time.time() - bulk_writing_start
    bulk_insert_start = time.time()
    inserter.push()
    bulk_insert_time = time.time() - bulk_insert_start
    total_insertion_time = time.time() - total_insertion_start
    
    res = qdbd_connection.query("SELECT count(col_0) FROM FIND(tag='test_tag')")
    print(f'Results:')
    print(f'  - batch start row:     {bulk_start_row_time}s')
    print(f'  - batch set values:    {bulk_writing_time}s')
    print(f'  - batch insert values: {bulk_insert_time}s')
    print(f'  - total insert time:   {total_insertion_time}s')
    print(f'  - rows inserted: {res[0]}')
