"""

.. module: quasardb
    :platform: Unix, Windows
    :synopsis: quasardb official Python API

.. moduleauthor: quasardb SAS. All rights reserved
"""

from __future__ import annotations

import datetime
from typing import Union
from typing_extensions import Self

from . import quasardb
from .rest import RestCluster as RestCluster

from quasardb.quasardb import (
    AliasAlreadyExistsError,
    AliasNotFoundError,
    AsyncPipelineFullError,
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

class Cluster(quasardb.Cluster):
    # A str annotation cannot distinguish URI schemes. An isinstance check
    # narrows the result for callers that use native-only methods.
    def __new__(  # type: ignore[misc]
        cls,
        uri: str,
        user_name: str = ...,
        user_private_key: str = ...,
        cluster_public_key: str = ...,
        *,
        user_security_file: str = ...,
        cluster_public_key_file: str = ...,
        timeout: datetime.timedelta = ...,
        do_version_check: bool = ...,
        enable_encryption: bool = ...,
        compression_mode: quasardb.Options.Compression = ...,
        client_max_parallelism: int = ...,
        verify: bool = ...,
    ) -> Union[Self, RestCluster]: ...
    def __enter__(self) -> Self: ...

__all__ = [
    "AliasAlreadyExistsError",
    "AliasNotFoundError",
    "AsyncPipelineFullError",
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
