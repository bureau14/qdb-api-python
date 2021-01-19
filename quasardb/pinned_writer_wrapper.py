import quasardb
import logging
import numpy as np
import sys

class PinnedWriterWrapper:
    columns = []
    timestamp = 0
    column_count = 0
    column_types = []
    writer = None
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    def __init__(self, writer):
        self.column_types = writer.column_types()
        self.column_count = len(self.column_types)
        self.writer = writer
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

    # set value
    def _set_value(self, idx, value):
        self.columns[idx][0].append(self.timestamp)
        self.columns[idx][1].append(value)

    def set_int64(self, idx, value):
        self._set_value(idx, value)

    def set_double(self, idx, value):
        self._set_value(idx, value)

    def set_timestamp(self, idx, value):
        self._set_value(idx, value)

    def set_blob(self, idx, value):
        self._set_value(idx, value)

    def set_string(self, idx, value):
        self._set_value(idx, value)

    def set_symbol(self, idx, value):
        self._set_value(idx, value)

    # set column
    def _set_column(self, idx, timestamps, values):
        self.columns[idx][0].extend(timestamps)
        self.columns[idx][1].extend(values)

    def set_blob_column(self, idx, timestamps, values):
        self._set_column(idx, timestamps, values)

    def set_double_column(self, idx, timestamps, values):
        self._set_column(idx, timestamps, values)

    def set_int64_column(self, idx, timestamps, values):
        self._set_column(idx, timestamps, values)

    def set_string_column(self, idx, timestamps, values):
        self._set_column(idx, timestamps, values)

    def set_symbol_column(self, idx, timestamps, values):
        self._set_column(idx, timestamps, values)

    def set_timestamp_column(self, idx, timestamps, values):
        self._set_column(idx, timestamps, values)

    def _flush_columns(self):
        flush_with = {
            quasardb.ColumnType.Double: self.writer.set_double_column,
            quasardb.ColumnType.Blob: self.writer.set_blob_column,
            quasardb.ColumnType.String: self.writer.set_string_column,
            quasardb.ColumnType.Int64: self.writer.set_int64_column,
            quasardb.ColumnType.Timestamp: self.writer.set_timestamp_column,
            quasardb.ColumnType.Symbol: self.writer.set_symbol_column,
        }
        for idx in range(0, self.column_count):
            flush_with[self.column_types[idx]](idx, self.columns[idx][0], self.columns[idx][1])

    def push(self):
        self._flush_columns()
        self.writer.push()
        self._reset_batch()

    def push_async(self):
        self._flush_columns()
        self.writer.push_async()
        self._reset_batch()

    def push_fast(self):
        self._flush_columns()
        self.writer.push_fast()
        self._reset_batch()

def make_pinned_writer_wrapper(writer):
    return PinnedWriterWrapper(writer)
