import quasardb
import logging
import numpy as np
import sys

class TimeseriesPinnedValueBatch:
    columns = []
    timestamp = 0
    column_count = 0
    column_types = []
    inserter = None
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    def __init__(self, inserter, column_types):
        self.column_count = len(column_types)
        self.column_types = column_types
        self.inserter = inserter
        self._reset_columns()
    
    def _reset_columns(self):
        self.columns = []
        for idx in range(0, self.column_count):
            self.columns.append([[], []])

    def _reset_batch(self):
        self.timestamp = 0
        self._reset_columns()

    def start_row(self, ts):
        self.timestamp = ts

    def set_int64(self, idx, value):
        self.columns[idx][0].append(self.timestamp)
        self.columns[idx][1].append(value)

    def set_double(self, idx, value):
        self.columns[idx][0].append(self.timestamp)
        self.columns[idx][1].append(value)

    def set_timestamp(self, idx, value):
        self.columns[idx][0].append(self.timestamp)
        self.columns[idx][1].append(value)

    def set_blob(self, idx, value):
        self.columns[idx][0].append(self.timestamp)
        self.columns[idx][1].append(value)

    def set_string(self, idx, value):
        self.columns[idx][0].append(self.timestamp)
        self.columns[idx][1].append(value)

    def push(self):
        insert_with = {
            quasardb.ColumnType.Double: self.inserter.set_pinned_double_column,
            quasardb.ColumnType.Blob: self.inserter.set_pinned_blob_column,
            quasardb.ColumnType.String: self.inserter.set_pinned_string_column,
            quasardb.ColumnType.Int64: self.inserter.set_pinned_int64_column,
            quasardb.ColumnType.Timestamp: self.inserter.set_pinned_timestamp_column,
            quasardb.ColumnType.Symbol: self.inserter.set_pinned_symbol_column,
        }
        for idx in range(0, self.column_count):
            insert_with[self.column_types[idx]](idx, self.columns[idx][0], self.columns[idx][1])
        self.inserter.pinned_push()
        self._reset_batch()

def make_pinned_writer(inserter, column_types):
    return TimeseriesPinnedValueBatch(inserter, column_types)