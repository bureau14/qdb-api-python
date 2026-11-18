from __future__ import annotations

from .._cluster import QueryStream

class QueryReader(QueryStream):
    def __enter__(self) -> QueryReader: ...
