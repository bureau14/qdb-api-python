from __future__ import annotations

from .._cluster import Cluster
from ._query_reader import QueryReader

class NativeCluster(Cluster):
    def __enter__(self) -> NativeCluster: ...
    def stream_query(self, query: str, *, batch_size: int = 0) -> QueryReader: ...
