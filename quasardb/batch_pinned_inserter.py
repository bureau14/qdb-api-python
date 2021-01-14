import quasardb
import logging
import numpy as np
import sys

class TimeseriesPinnedBatch:
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
        self._reset_columns()
        self.timestamp = 0

    def set_int64_column(self, idx, timestamps, values):
        self.inserter.set_pinned_int64_column(idx, timestamps, values)

    def push(self):
        self.inserter.pinned_push()
        self._reset_batch()

def make_pinned_writer(inserter, column_count):
    return TimeseriesPinnedBatch(inserter, column_count)
