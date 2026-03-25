import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from quasardb.quasardb import Cluster, Table
else:
    Cluster = Any
    Table = Any

logger = logging.getLogger("quasardb.table_cache")

_cache: Dict[str, Table] = {}
_table_clusters: Dict[int, Cluster] = {}


def clear() -> None:
    global _cache
    global _table_clusters
    logger.info("Clearing table cache")
    _cache = {}
    _table_clusters = {}


def exists(table_name: str) -> bool:
    """
    Returns true if table already exists in table cache.
    """
    return table_name in _cache


def store(table: Table, table_name: Optional[str] = None) -> Table:
    """
    Stores a table into the cache. Ensures metadata is retrieved. This is useful if you want
    to retrieve all table metadata at the beginning of a process, to avoid doing expensive
    lookups in undesired code paths.

    Returns a reference to the table being stored.
    """
    if table_name is None:
        table_name = table.get_name()

    if exists(table_name):
        logger.warning("Table already in cache, overwriting: %s", table_name)

    logger.debug("Caching table %s", table_name)
    _cache[table_name] = table

    table.retrieve_metadata()

    return table


def register_cluster(table: Table, conn: Cluster) -> Table:
    _table_clusters[id(table)] = conn
    return table


def cluster_for(table: Table) -> Cluster:
    key = id(table)
    if key not in _table_clusters:
        raise KeyError(
            "Unable to resolve cluster for table '{}'. Please obtain the table from a Cluster instance.".format(
                table.get_name()
            )
        )

    return _table_clusters[key]


def lookup(table_name: str, conn: Cluster) -> Table:
    """
    Retrieves table from _cache if already exists. If it does not exist,
    looks up the table from `conn` and puts it in the cache.

    If `force_retrieve_metadata` equals True, we will ensure that the table's
    metadata is
    """
    if exists(table_name):
        return register_cluster(_cache[table_name], conn)

    logger.debug("table %s not yet found, looking up", table_name)
    table = conn.table(table_name)

    return register_cluster(store(table, table_name), conn)
