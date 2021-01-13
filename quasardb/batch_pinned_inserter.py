import quasardb
import logging
import numpy as np
import sys

class TimeseriesPinnedBatch:
    row = []
    columns = []
    timestamp = 0
    column_count = 0
    inserter = None
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    def __init__(self, inserter, column_count):
        print("\n")
        print("__init__")
        self.column_count = column_count
        self.inserter = inserter
        self._reset_columns()

    def _reset_row(self):
        print("_reset_row")
        self.row = [None] * self.column_count
    
    def _reset_columns(self):
        print("_reset_columns")
        self.columns = []
        for idx in range(0, self.column_count):
            self.columns.append(([], []))

    def _reset_batch(self):
        print("_reset_batch")
        self._reset_row()
        self._reset_columns()
        self.timestamp = 0

    def _append_row(self):
        print("_append_row")
        print("{}".format(self.columns))
        for idx in range(0, self.column_count):
            if self.row[idx] != None:
                self.columns[idx][0].append(self.timestamp)
                self.columns[idx][1].append(self.row[idx])

    def start_row(self, ts):
        print("start_row")
        if len(self.row) > 0:
            self._append_row()
        self.timestamp = ts
        self._reset_row()

    def set_value(self, idx, value):
        print("set_value")
        # check for null value, use the corresponding qdb null value
        self.row[idx] = value

    def push(self):
        self._append_row()
        for idx in range(0, self.column_count):
            self.inserter.set_pinned_int64_column(idx, self.columns[idx][0], self.columns[idx][1])
        self.inserter.pinned_push()
        self._reset_batch()

def make_pinned_writer(inserter, column_count):
    return TimeseriesPinnedBatch(inserter, column_count)
