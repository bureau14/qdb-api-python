import quasardb
import logging
import numpy as np
import sys

class TimeseriesPinnedValueBatch:
    columns = []
    timestamp = 0
    column_count = 0
    inserter = None
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    def __init__(self, inserter, column_count):
        self.column_count = column_count
        self.inserter = inserter
        self._reset_columns()
    
    def _reset_columns(self):
        self.columns = []
        for idx in range(0, self.column_count):
            self.columns.append(([], []))

    def _reset_batch(self):
        self.timestamp = 0
        self._reset_columns()

    def start_row(self, ts):
        self.timestamp = ts

    def set_int64(self, idx, value):
        self.columns[idx][0].append(self.timestamp)
        self.columns[idx][1].append(value)

    def push(self):
        for idx in range(0, self.column_count):
            self.inserter.set_pinned_int64_column(idx, self.columns[idx][0], self.columns[idx][1])
        self.inserter.pinned_push()
        self._reset_batch()

def make_pinned_writer(inserter, column_count):
    return TimeseriesPinnedValueBatch(inserter, column_count)