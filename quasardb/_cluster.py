"""Shared connection interface and URI dispatch; loaded after extension initialization."""

from __future__ import annotations

import datetime
from abc import ABCMeta
from inspect import signature
from types import TracebackType
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union

from .quasardb import Options

if TYPE_CHECKING:
    from ._rest import RestQueryReader
    from .typing import MaskedArrayAny, RangeSet
    from .quasardb import (
        Blob,
        Double,
        Integer,
        Node,
        Perf,
        Properties,
        QueryContinuous,
        QueryReader,
        Reader,
        String,
        Table,
        Tag,
        Timestamp,
        Writer,
    )


class ClusterMeta(ABCMeta):
    """Choose the transport when a caller constructs Cluster(uri).

    ABCMeta supplies register(), so the native extension type can satisfy
    isinstance(conn, Cluster) while keeping its existing class hierarchy.
    """

    def __call__(cls, *args, **kwargs):
        if cls is Cluster:
            from .quasardb import InvalidArgumentError, NativeCluster

            # The native parser accepts multi-node addresses and raw byte strings.
            # Only inspect the scheme here so the binding receives its original URI.
            uri = args[0] if args else kwargs.get("uri")
            if isinstance(uri, str):
                scheme = uri.partition(":")[0].lower()
                if scheme in ("http", "https"):
                    from ._rest import RestCluster

                    return RestCluster(*args, **kwargs)
                if scheme != "qdb":
                    raise InvalidArgumentError(
                        "Unsupported connection URI scheme: " + scheme
                    )
            # verify controls REST certificate checks. Shared settings may
            # include its default even when the native binding handles the URI.
            verify = kwargs.pop("verify", True)
            if not isinstance(verify, bool):
                raise TypeError("verify must be a bool")
            if not verify:
                raise InvalidArgumentError(
                    "verify=False is only supported by REST connections"
                )
            return NativeCluster(*args, **kwargs)
        return super().__call__(*args, **kwargs)


class Cluster(metaclass=ClusterMeta):
    """Open a NativeCluster for qdb:// or a RestCluster for http:// and https://."""

    __slots__ = ()

    def __enter__(self) -> Cluster:
        return self

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __init__(
        self,
        uri: str,
        user_name: str = "",
        user_private_key: str = "",
        cluster_public_key: str = "",
        *,
        user_security_file: str = "",
        cluster_public_key_file: str = "",
        timeout: datetime.timedelta = datetime.timedelta(minutes=1),
        do_version_check: bool = False,
        enable_encryption: bool = False,
        compression_mode: Options.Compression = Options.Compression.Balanced,
        client_max_parallelism: int = 0,
        verify: bool = True,
    ) -> None:
        raise TypeError("Construct a concrete connection or use Cluster(uri)")

    def blob(self, __alias: str) -> Blob:
        raise NotImplementedError("blob is unavailable for this transport")

    def close(self) -> None:
        raise NotImplementedError("close is unavailable for this transport")

    def compact_abort(self) -> None:
        raise NotImplementedError("compact_abort is unavailable for this transport")

    def compact_full(self) -> None:
        raise NotImplementedError("compact_full is unavailable for this transport")

    def compact_progress(self) -> int:
        raise NotImplementedError("compact_progress is unavailable for this transport")

    def double(self, __alias: str) -> Double:
        raise NotImplementedError("double is unavailable for this transport")

    def endpoints(self) -> List[str]:
        raise NotImplementedError("endpoints is unavailable for this transport")

    def find(self, __query: str) -> List[str]:
        raise NotImplementedError("find is unavailable for this transport")

    def get_memory_info(self) -> str:
        raise NotImplementedError("get_memory_info is unavailable for this transport")

    def integer(self, __alias: str) -> Integer:
        raise NotImplementedError("integer is unavailable for this transport")

    def is_open(self) -> bool:
        raise NotImplementedError("is_open is unavailable for this transport")

    def node(self, __uri: str) -> Node:
        raise NotImplementedError("node is unavailable for this transport")

    def node_config(self, __uri: str) -> Dict[str, Any]:
        raise NotImplementedError("node_config is unavailable for this transport")

    def node_status(self, __uri: str) -> Dict[str, Any]:
        raise NotImplementedError("node_status is unavailable for this transport")

    def node_topology(self, __uri: str) -> Dict[str, Any]:
        raise NotImplementedError("node_topology is unavailable for this transport")

    def options(self) -> Options:
        raise NotImplementedError("options is unavailable for this transport")

    def perf(self) -> Perf:
        raise NotImplementedError("perf is unavailable for this transport")

    def pinned_writer(self) -> Writer:
        raise NotImplementedError("pinned_writer is unavailable for this transport")

    def prefix_count(self, __prefix: str) -> int:
        raise NotImplementedError("prefix_count is unavailable for this transport")

    def prefix_get(self, __prefix: str, __max_count: int) -> List[str]:
        raise NotImplementedError("prefix_get is unavailable for this transport")

    def properties(self) -> Properties:
        raise NotImplementedError("properties is unavailable for this transport")

    def purge_all(self, __timeout: datetime.timedelta) -> None:
        raise NotImplementedError("purge_all is unavailable for this transport")

    def purge_cache(self, __timeout: datetime.timedelta) -> None:
        raise NotImplementedError("purge_cache is unavailable for this transport")

    def query(self, query: str) -> List[Dict[str, Any]]:
        raise NotImplementedError("query is unavailable for this transport")

    def query_continuous_full(
        self, query: str, pace: datetime.timedelta
    ) -> QueryContinuous:
        raise NotImplementedError(
            "query_continuous_full is unavailable for this transport"
        )

    def query_continuous_new_values(
        self, query: str, pace: datetime.timedelta
    ) -> QueryContinuous:
        raise NotImplementedError(
            "query_continuous_new_values is unavailable for this transport"
        )

    def query_numpy(self, query: str) -> List[Tuple[str, MaskedArrayAny]]:
        raise NotImplementedError("query_numpy is unavailable for this transport")

    def reader(
        self,
        table_names: List[str],
        *,
        column_names: List[str] = [],
        batch_size: int = 0,
        ranges: RangeSet = [],
    ) -> Reader:
        raise NotImplementedError("reader is unavailable for this transport")

    def stream_query(
        self, query: str, *, batch_size: int = 0
    ) -> Union[QueryReader, RestQueryReader]:
        raise NotImplementedError("stream_query is unavailable for this transport")

    def string(self, __alias: str) -> String:
        raise NotImplementedError("string is unavailable for this transport")

    def suffix_count(self, __suffix: str) -> int:
        raise NotImplementedError("suffix_count is unavailable for this transport")

    def suffix_get(self, __suffix: str, __max_count: int) -> List[str]:
        raise NotImplementedError("suffix_get is unavailable for this transport")

    def table(self, __alias: str) -> Table:
        raise NotImplementedError("table is unavailable for this transport")

    def tag(self, __alias: str) -> Tag:
        raise NotImplementedError("tag is unavailable for this transport")

    def tidy_memory(self) -> None:
        raise NotImplementedError("tidy_memory is unavailable for this transport")

    def timestamp(self, __alias: str) -> Timestamp:
        raise NotImplementedError("timestamp is unavailable for this transport")

    def trim_all(
        self, __pause: datetime.timedelta, __timeout: datetime.timedelta
    ) -> None:
        raise NotImplementedError("trim_all is unavailable for this transport")

    def ts(self, __alias: str) -> Table:
        raise NotImplementedError("ts is unavailable for this transport")

    def uri(self) -> str:
        raise NotImplementedError("uri is unavailable for this transport")

    def wait_for_compaction(self) -> None:
        raise NotImplementedError(
            "wait_for_compaction is unavailable for this transport"
        )

    def validate_query(self, __query: str) -> List[Tuple[str, MaskedArrayAny]]:
        raise NotImplementedError("validate_query is unavailable for this transport")

    def split_query_range(
        self,
        __start: datetime.datetime,
        __end: datetime.datetime,
        __delta: datetime.timedelta,
    ) -> List[Tuple[datetime.datetime, datetime.datetime]]:
        raise NotImplementedError("split_query_range is unavailable for this transport")

    def writer(self) -> Writer:
        raise NotImplementedError("writer is unavailable for this transport")


# A metaclass __call__ is variadic; publish the constructor signature for inspection.
setattr(
    Cluster,
    "__signature__",
    signature(Cluster.__init__).replace(
        parameters=list(signature(Cluster.__init__).parameters.values())[1:]
    ),
)
