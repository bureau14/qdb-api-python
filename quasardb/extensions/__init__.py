from typing import Any, Callable, List

import quasardb.table_cache as table_cache
from .writer import extend_writer

__all__: List[Any] = []


def _extend_cluster(x: Any) -> None:
    if getattr(x, "_qdb_cluster_extended", False):
        return

    original_table: Callable[..., Any] = x.table
    original_ts: Callable[..., Any] = x.ts

    def table_with_cluster(self: Any, *args: Any, **kwargs: Any) -> Any:
        table = original_table(self, *args, **kwargs)
        return table_cache.register_cluster(table, self)

    def ts_with_cluster(self: Any, *args: Any, **kwargs: Any) -> Any:
        table = original_ts(self, *args, **kwargs)
        return table_cache.register_cluster(table, self)

    x.table = table_with_cluster
    x.ts = ts_with_cluster
    x._qdb_cluster_extended = True


def extend_module(m: Any) -> None:
    extend_writer(m.Writer)
    _extend_cluster(m.Cluster)
