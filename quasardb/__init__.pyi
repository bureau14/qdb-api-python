"""

.. module: quasardb
    :platform: Unix, Windows
    :synopsis: quasardb official Python API

.. moduleauthor: quasardb SAS. All rights reserved
"""

from __future__ import annotations

from quasardb.quasardb import (
    AliasAlreadyExistsError,
    AliasNotFoundError,
    AsyncPipelineFullError,
    BatchColumnInfo,
    Cluster,
    ColumnInfo,
    ColumnType,
    Error,
    IncompatibleTypeError,
    IndexedColumnInfo,
    InputBufferTooSmallError,
    InternalLocalError,
    InvalidArgumentError,
    InvalidDatetimeError,
    InvalidHandleError,
    InvalidQueryError,
    Node,
    NotImplementedError,
    OutOfBoundsError,
    RetryOptions,
    TryAgainError,
    UninitializedError,
    WriterData,
    WriterPushMode,
    build,
    metrics,
    never_expires,
    version,
)

__all__ = [
    "AliasAlreadyExistsError",
    "AliasNotFoundError",
    "AsyncPipelineFullError",
    "BatchColumnInfo",
    "Cluster",
    "ColumnInfo",
    "ColumnType",
    "Error",
    "IncompatibleTypeError",
    "IndexedColumnInfo",
    "InputBufferTooSmallError",
    "InternalLocalError",
    "InvalidArgumentError",
    "InvalidDatetimeError",
    "InvalidHandleError",
    "InvalidQueryError",
    "Node",
    "NotImplementedError",
    "OutOfBoundsError",
    "RetryOptions",
    "TryAgainError",
    "UninitializedError",
    "WriterData",
    "WriterPushMode",
    "build",
    "metrics",
    "never_expires",
    "version",
]
