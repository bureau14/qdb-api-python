import re
import quasardb
import logging

logger = logging.getLogger('quasardb.stats')


stats_prefix = '$qdb.statistics.'
user_pattern = re.compile('\$qdb.statistics.(.*).uid_([0-9]+)$')
total_pattern = re.compile('\$qdb.statistics.(.*)$(?<!uid_[0-9])')

def by_node(conn):
    """
    Returns statistic grouped by node URI.

    Parameters:
    conn: quasardb.Cluster
      Active connection to the QuasarDB cluster
    """
    return {x: of_node(conn, x) for x in conn.endpoints()}

def of_node(conn, uri):
    """
    Returns statistic for a single node.

    Parameters:
    conn: quasardb.Cluster
      Active connection to the QuasarDB cluster

    uri: str
      URI of a node in the cluster, e.g. '127.0.0.1:2836'.
    """
    dconn = conn.node(uri)

    raw = {k: _get_stat(dconn, k) for k in dconn.prefix_get(stats_prefix, 1000)}
    return {'by_uid': _by_uid(raw),
            'cumulative': _cumulative(raw)}

def _get_stat(dconn, k):
    # Ugly, but works: try to retrieve as integer, if not an int, retrieve as blob
    try:
        return dconn.integer(k).get()
    except quasardb.quasardb.Error:
        blob = dconn.blob(k).get()
        return blob.decode('utf-8', 'replace')

def _by_uid(stats):
    xs = {}
    for k,v in stats.items():
        matches = user_pattern.match(k)
        if matches:
            (metric, uid_str) = matches.groups()
            uid = int(uid_str)
            if not uid in xs:
                xs[uid] = {}

            xs[uid][metric] = v

    return xs

def _cumulative(stats):
    xs = {}

    for k,v in stats.items():
        matches = total_pattern.match(k)
        if matches:
            metric = matches.groups()[0]
            xs[metric] = v

    return xs
