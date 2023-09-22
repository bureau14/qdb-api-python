import logging

logger = logging.getLogger('quasardb.table_cache')

_cache = {}

def clear():
    logger.info("Clearing table cache")
    _cache = {}

def exists(table_name: str) -> bool:
    """
    Returns true if table already exists in table cache.
    """
    return table_name in _cache

def store(table, table_name=None, force_retrieve_metadata=True):
    """
    Stores a table into the cache. Ensures metadata is retrieved. This is useful if you want
    to retrieve all table metadata at the beginning of a process, to avoid doing expensive
    lookups in undesired code paths.

    Returns a reference to the table being stored.
    """
    if table_name is None:
        table_name = table.get_name()

    if exists(table_name):
        logger.warn("Table already in cache, overwriting: %s", table_name)

    logger.debug("Caching table %s", table_name)
    _cache[table_name] = table

    table.retrieve_metadata()

    return table

def lookup(table_name: str, conn, force_retrieve_metadata=True):
    """
    Retrieves table from _cache if already exists. If it does not exist,
    looks up the table from `conn` and puts it in the cache.

    If `force_retrieve_metadata` equals True, we will ensure that the table's
    metadata is
    """
    if exists(table_name):
        return _cache[table_name]

    logger.debug("table %s not yet found, looking up", table_name)
    table = conn.table(table_name)

    return store(table, table_name, force_retrieve_metadata=force_retrieve_metadata)
