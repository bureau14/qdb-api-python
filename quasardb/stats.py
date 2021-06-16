import re
import quasardb
import logging
from datetime import datetime

logger = logging.getLogger('quasardb.stats')


stats_prefix  = '$qdb.statistics.'
user_pattern  = re.compile(r'\$qdb.statistics.(.*).uid_([0-9]+)$')
total_pattern = re.compile(r'\$qdb.statistics.(.*)$')


def is_user_stat(s):
    return user_pattern.match(s) is not None

def is_cumulative_stat(s):
    # NOTE(leon): It's quite difficult to express in Python that you don't want any
    # regex to _end_ with uid_[0-9]+, because Python's regex engine doesn't support
    # variable width look-behind.
    #
    # An alternative would be to use the PyPi regex library (for POSIX regexes), but
    # want to stay light on dependencies#
    #
    # As such, we define a 'cumulative' stat as anything that's not a user stat.
    # Simple but effective.
    return user_pattern.match(s) is None

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

    start = datetime.now()

    dconn = conn.node(uri)
    raw = {
        k: _get_stat(dconn,k) for k in dconn.prefix_get(stats_prefix, 1000)}

    ret = {'by_uid': _by_uid(raw),
           'cumulative': _cumulative(raw)}

    check_duration = datetime.now() - start

    ret['cumulative']['check.online'] = 1
    ret['cumulative']['check.duration_ms'] = int(check_duration.total_seconds() * 1000)

    return ret

def _get_stat(dconn, k):
    # Ugly, but works: try to retrieve as integer, if not an int, retrieve as
    # blob
    try:
        return dconn.integer(k).get()
    except quasardb.quasardb.Error:
        blob = dconn.blob(k).get()
        return blob.decode('utf-8', 'replace')

def _by_uid(stats):
    xs = {}
    for k, v in stats.items():
        matches = user_pattern.match(k)
        if is_user_stat(k) and matches:
            (metric, uid_str) = matches.groups()
            uid = int(uid_str)
            if uid not in xs:
                xs[uid] = {}

            xs[uid][metric] = v

    return xs


def _cumulative(stats):
    xs = {}

    for k, v in stats.items():
        matches = total_pattern.match(k)
        if is_cumulative_stat(k) and matches:
            metric = matches.groups()[0]
            xs[metric] = v

    return xs
