"""

.. module: quasardb
    :platform: Unix, Windows
    :synopsis: quasardb official Python API

.. moduleauthor: quasardb SAS. All rights reserved
"""

from __future__ import annotations

import datetime

from quasardb.quasardb import BatchColumnInfo

# from quasardb.quasardb import (
#     AliasAlreadyExistsError,
#     AliasNotFoundError,
#     AsyncPipelineFullError,
#     Blob,
#     Cluster,
#     ColumnInfo,
#     ColumnType,
#     DirectBlob,
#     DirectInteger,
#     Double,
#     Entry,
#     Error,
#     ExpirableEntry,
#     FindQuery,
#     IncompatibleTypeError,
#     IndexedColumnInfo,
#     InputBufferTooSmallError,
#     Integer,
#     InternalLocalError,
#     InvalidArgumentError,
#     InvalidDatetimeError,
#     InvalidHandleError,
#     InvalidQueryError,
#     MaskedArray,
#     Node,
#     NotImplementedError,
#     Options,
#     OutOfBoundsError,
#     Perf,
#     Properties,
#     QueryContinuous,
#     Reader,
#     RetryOptions,
#     String,
#     Table,
#     Tag,
#     Timestamp,
#     TryAgainError,
#     UninitializedError,
#     Writer,
#     WriterData,
#     WriterPushMode,
#     build,
#     dict_query,
#     metrics,
#     version,
# )

__all__ = [
    # "AliasAlreadyExistsError",
    # "AliasNotFoundError",
    # "AsyncPipelineFullError",
    "BatchColumnInfo",
    # "Blob",
    # "Cluster",
    # "ColumnInfo",
    # "ColumnType",
    # "DirectBlob",
    # "DirectInteger",
    # "Double",
    # "Entry",
    # "Error",
    # "ExpirableEntry",
    # "FindQuery",
    # "IncompatibleTypeError",
    # "IndexedColumnInfo",
    # "InputBufferTooSmallError",
    # "Integer",
    # "InternalLocalError",
    # "InvalidArgumentError",
    # "InvalidDatetimeError",
    # "InvalidHandleError",
    # "InvalidQueryError",
    # "MaskedArray",
    # "Node",
    # "NotImplementedError",
    # "Options",
    # "OutOfBoundsError",
    # "Perf",
    # "Properties",
    # "QueryContinuous",
    # "Reader",
    # "RetryOptions",
    # "String",
    # "Table",
    # "Tag",
    # "Timestamp",
    # "TryAgainError",
    # "UninitializedError",
    # "Writer",
    # "WriterData",
    # "WriterPushMode",
    # "build",
    # "dict_query",
    # "extend_module",
    # "extensions",
    # "generic_error_msg",
    # "glibc_error_msg",
    # "link_error_msg",
    # "metrics",
    # "never_expires",
    # "quasardb",
    # "unknown_error_msg",
    # "version",
]

def generic_error_msg(msg, e=None): ...
def glibc_error_msg(e): ...
def link_error_msg(e): ...
def unknown_error_msg(): ...

never_expires: datetime.datetime  # value = datetime.datetime(1969, 12, 31, 19, 0)
