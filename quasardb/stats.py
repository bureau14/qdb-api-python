import re
import quasardb
import logging
from datetime import datetime

logger = logging.getLogger('quasardb.stats')


stats_prefix = '$qdb.statistics.'
user_pattern = re.compile(r'\$qdb.statistics.(.*).uid_([0-9]+)$')
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
    return {x: of_node(conn.node(x)) for x in conn.endpoints()}


def of_node(dconn):
    """
    Returns statistic for a single node.

    Parameters:
    dconn: quasardb.Node
      Direct node connection to the node we wish to connect to

    """

    start = datetime.now()
    raw = {
        k: _get_stat(dconn,k) for k in dconn.prefix_get(stats_prefix, 1000)}

    ret = {'by_uid': _by_uid(raw),
           'cumulative': _cumulative(raw)}

    check_duration = datetime.now() - start

    ret['cumulative']['check.online'] = 1
    ret['cumulative']['check.duration_ms'] = int(check_duration.total_seconds() * 1000)

    return ret

_stat_types = {'node_id': ('constant', None),
               'operating_system': ('constant', None),
               'partitions_count': ('constant', 'count'),

               'cpu.system': ('counter', 'ns'),
               'cpu.user': ('counter', 'ns'),
               'cpu.idle': ('counter', 'ns'),
               'startup': ('constant', None),
               'startup_time': ('constant', None),
               'shutdown_time': ('constant', None),

               'network.current_users_count': ('gauge', 'count'),
               'hardware_concurrency': ('gauge', 'count'),

               'check.online': ('gauge', 'count'),
               'check.duration_ms': ('constant', 'ms'),

               'requests.bytes_in': ('counter', 'bytes'),
               'requests.bytes_out': ('counter', 'bytes'),
               'requests.errors_count': ('counter', 'count'),
               'requests.successes_count': ('counter', 'count'),
               'requests.total_count': ('counter', 'count'),

               'async_pipelines.merge.bucket_count': ('counter', 'count'),
               'async_pipelines.merge.duration_us': ('counter', 'us'),
               'async_pipelines.write.successes_count': ('counter', 'count'),
               'async_pipelines.write.failures_count': ('counter', 'count'),
               'async_pipelines.write.time_us': ('counter', 'us'),

               'async_pipelines.merge.max_bucket_count': ('gauge', 'count'),
               'async_pipelines.merge.max_depth_count': ('gauge', 'count'),
               'async_pipelines.merge.requests_count': ('counter', 'count'),

               'evicted.count': ('counter', 'count'),
               'pageins.count': ('counter', 'count'),

               }

async_pipeline_bytes_pattern  = re.compile(r'async_pipelines.pipe_[0-9]+.merge_map.bytes')
async_pipeline_count_pattern  = re.compile(r'async_pipelines.pipe_[0-9]+.merge_map.count')

def _stat_type(stat_id):
    if stat_id in _stat_types:
        return _stat_types[stat_id]
    elif stat_id.endswith('total_ns'):
        return ('counter', 'ns')
    elif stat_id.endswith('total_bytes'):
        return ('counter', 'bytes')
    elif stat_id.endswith('read_bytes'):
        return ('counter', 'bytes')
    elif stat_id.endswith('written_bytes'):
        return ('counter', 'bytes')
    elif stat_id.endswith('total_count'):
        return ('counter', 'count')
    elif stat_id.startswith('network.sessions.'):
        return ('gauge', 'count')
    elif stat_id.startswith('memory.'):
        # memory statistics are all gauges i think, describes how much memory currently allocated where
        return ('gauge', 'bytes')
    elif stat_id.startswith('persistence.') or stat_id.startswith('disk'):
        # persistence are also all gauges, describes mostly how much is currently available/used on storage
        return ('gauge', 'bytes')
    elif stat_id.startswith('license.'):
        return ('gauge', None)
    elif stat_id.startswith('engine_'):
        return ('constant', None)
    elif async_pipeline_bytes_pattern.match(stat_id):
        return ('gauge', 'bytes')
    elif async_pipeline_count_pattern.match(stat_id):
        return ('gauge', 'count')
    else:
        return None

def stat_type(stat_id):
    """
    Returns the statistic type by a stat id. Returns one of:

     - 'gauge'
     - 'counter'
     - None in case of unrecognized statistics

    This is useful for determining which value should be reported in a dashboard.
    """
    return _stat_type(stat_id)


def _calculate_delta_stat(stat_id, prev, cur):
    logger.info("calculating delta for stat_id = {}, prev = {}. cur = {}".format(stat_id, prev, cur))

    stat_type = _stat_type(stat_id)
    if stat_type == 'counter':
        return cur - prev
    elif stat_type == 'gauge':
        return cur
    else:
        return None

def _calculate_delta_stats(prev_stats, cur_stats):
    ret = {}
    for stat_id in cur_stats.keys():
        try:
            prev_stat = cur_stats[stat_id]
            cur_stat  = cur_stats[stat_id]

            value = _calculate_delta_stat(stat_id, prev_stat, cur_stat)
            if value is not None:
                ret[stat_id] = value

        except KeyError:
            # Stat likely was not present yet in prev_stats
            pass

    return ret


def calculate_delta(prev, cur):
    """
    Calculates the 'delta' between two successive statistic measurements.
    """
    ret = {}
    for node_id in cur.keys():
        ret[node_id] = _calculate_delta_stats(prev[node_id]['cumulative'],
                                              cur[node_id]['cumulative'])

    return ret

def _clean_blob(x):
    x_ = x.decode('utf-8', 'replace')

    # remove trailing zero-terminator
    return ''.join(c for c in x_ if ord(c) != 0)


def _get_stat(dconn, k):
    # Ugly, but works: try to retrieve as integer, if not an int, retrieve as
    # blob
    try:
        return dconn.integer(k).get()
    except quasardb.quasardb.AliasNotFoundError:
        return _clean_blob(dconn.blob(k).get())

def _by_uid(stats):
    xs = {}
    for k, v in stats.items():
        matches = user_pattern.match(k)
        if is_user_stat(k) and matches:
            (metric, uid_str) = matches.groups()
            uid = int(uid_str)
            if uid not in xs:
                xs[uid] = {}

            if not metric.startswith('serialized'):
                xs[uid][metric] = v

    return xs


def _cumulative(stats):
    xs = {}

    for k, v in stats.items():
        matches = total_pattern.match(k)
        if is_cumulative_stat(k) and matches:
            metric = matches.groups()[0]
            if not metric.startswith('serialized'):
                xs[metric] = v

    return xs
