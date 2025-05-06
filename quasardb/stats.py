import re
from time import sleep

import quasardb
import logging
from collections import defaultdict
from datetime import datetime
from enum import Enum

logger = logging.getLogger("quasardb.stats")

MAX_KEYS = 4 * 1024 * 1024  # 4 million max keys
stats_prefix = "$qdb.statistics."
user_pattern = re.compile(r"\$qdb.statistics.(.*).uid_([0-9]+)$")
total_pattern = re.compile(r"\$qdb.statistics.(.*)$")


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

    ks = _get_all_keys(dconn)
    idx = _index_keys(dconn, ks)
    raw = {k: _get_stat(dconn, k) for k in ks}

    ret = {"by_uid": _by_uid(raw, idx), "cumulative": _cumulative(raw, idx)}

    check_duration = datetime.now() - start

    ret["cumulative"]["check.online"] = {
        "value": 1,
        "type": Type.ACCUMULATOR,
        "unit": Unit.NONE,
    }
    ret["cumulative"]["check.duration_ms"] = {
        "value": int(check_duration.total_seconds() * 1000),
        "type": Type.ACCUMULATOR,
        "unit": Unit.MILLISECONDS,
    }

    return ret


async_pipeline_bytes_pattern = re.compile(
    r"async_pipelines.pipe_[0-9]+.merge_map.bytes"
)
async_pipeline_count_pattern = re.compile(
    r"async_pipelines.pipe_[0-9]+.merge_map.count"
)


def stat_type(stat_id):
    """
    Returns the statistic type by a stat id. Returns one of:

     - 'gauge'
     - 'counter'
     - None in case of unrecognized statistics

    This is useful for determining which value should be reported in a dashboard.
    """
    raise Exception("Deprecated Method.")


def _get_all_keys(dconn, n=1024):
    """
    Returns all keys from a single node.

    Parameters:
    dconn: quasardb.Node
      Direct node connection to the node we wish to connect to.

    n: int
      Number of keys to retrieve.
    """
    xs = None
    increase_rate = 8
    # keep getting keys while number of results exceeds the given "n"
    while xs is None or len(xs) >= n:
        if xs is not None:
            n = n * increase_rate
        if n >= MAX_KEYS:
            raise Exception(f"ERROR: Cannot fetch more than {MAX_KEYS} keys.")
        xs = dconn.prefix_get(stats_prefix, n)

    return xs

class Type(Enum):
    ACCUMULATOR = 1
    GAUGE = 2
    LABEL = 3

class Unit(Enum):
    NONE = 0
    COUNT = 1

    # Size units
    BYTES = 32

    # Time/duration units
    EPOCH = 64,
    NANOSECONDS = 65
    MICROSECONDS = 66
    MILLISECONDS = 67
    SECONDS = 68



_type_string_to_enum = {'accumulator': Type.ACCUMULATOR,
                        'gauge': Type.GAUGE,
                        'label': Type.LABEL,
                        }

_unit_string_to_enum = {'none': Unit.NONE,
                        'count': Unit.COUNT,
                        'bytes': Unit.BYTES,
                        'epoch': Unit.EPOCH,
                        'nanoseconds': Unit.NANOSECONDS,
                        'microseconds': Unit.MICROSECONDS,
                        'milliseconds': Unit.MILLISECONDS,
                        'seconds': Unit.SECONDS,
                        }

def _lookup_enum(dconn, k, m):
    """
    Utility function to avoid code duplication: automatically looks up a key's value
    from QuasarDB and looks it up in provided dict.
    """

    x = dconn.blob(k).get()
    x = _clean_blob(x)

    if x not in m:
        print("x: ", x)
        print("type(x): ", type(x))
        print("k: ", k)
        raise Exception(f"Unrecognized unit/type {x} from key {k}")

    return m[x]

def _lookup_type(dconn, k):
    """
    Looks up and parses/validates the metric type.
    """
    assert k.endswith('.type')

    return _lookup_enum(dconn, k, _type_string_to_enum)


def _lookup_unit(dconn, k):
    """
    Looks up and parses/validates the metric type.
    """
    assert k.endswith('.unit')

    return _lookup_enum(dconn, k, _unit_string_to_enum)


def _index_keys(dconn, ks):
    """
    Takes all statistics keys that are retrieved, and "indexes" them in such a way
    that we end up with a dict of all statistic keys, their type and their unit.
    """

    ###
    # The keys generally look like this, for example:
    #
    # $qdb.statistics.requests.out_bytes
    # $qdb.statistics.requests.out_bytes.type
    # $qdb.statistics.requests.out_bytes.uid_1
    # $qdb.statistics.requests.out_bytes.uid_1.type
    # $qdb.statistics.requests.out_bytes.uid_1.unit
    # $qdb.statistics.requests.out_bytes.unit
    #
    # For this purpose, we simply get rid of the "uid" part, as the per-uid metrics are guaranteed
    # to be of the exact same type as all the others. So after trimming those, the keys will look
    # like this:
    #
    # $qdb.statistics.requests.out_bytes
    # $qdb.statistics.requests.out_bytes.type
    # $qdb.statistics.requests.out_bytes
    # $qdb.statistics.requests.out_bytes.type
    # $qdb.statistics.requests.out_bytes.unit
    # $qdb.statistics.requests.out_bytes.unit
    #
    # And after deduplication like this:
    #
    # $qdb.statistics.requests.out_bytes
    # $qdb.statistics.requests.out_bytes.type
    # $qdb.statistics.requests.out_bytes.unit
    #
    # In which case we'll store `requests.out_bytes` as the statistic type, and look up the type
    # and unit for those metrics and add a placeholder value.

    ret = defaultdict(lambda: {'value': None, 'type': None, 'unit': None})

    for k in ks:
        if "uid_" in k:
            # Per-uid key, skipping these as described above
            continue

        parts = k.rsplit(".", 1)

        if parts[1] == 'type':
            stat_id = parts[0]

            if ret[stat_id]['type'] == None:
                # We haven't seen this particular statistic yet
                ret[stat_id]['type'] = _lookup_type(dconn, k)
        elif parts[1] == 'unit':
            stat_id = parts[0]

            if ret[stat_id]['unit'] == None:
                # We haven't seen this particular statistic yet
                ret[stat_id]['unit'] = _lookup_unit(dconn, k)

    return ret


def _calculate_delta_stat(stat_id, prev_stat, curr_stat):
    logger.info(
        "calculating delta for stat_id = {}, prev = {}. cur = {}".format(
            stat_id, str(prev_stat["value"]), str(prev_stat["value"])
        )
    )
    if curr_stat["type"] in ["counter", "accumulator"]:
        return {**curr_stat, "value": curr_stat["value"] - prev_stat["value"]}
    elif curr_stat["type"] == "gauge":
        return {**curr_stat}
    else:
        return None


def _calculate_delta_stats(prev_stats, cur_stats):
    """
    Args:
        prev_stats: previous dictionary of cumulative stats
        cur_stats: current dictionary of cumulative stats

    Returns:
        a new dictionary with same structure and updated values
    """
    ret = {}
    for stat_id in cur_stats.keys():
        try:
            prev_stat = prev_stats[stat_id]
            cur_stat = cur_stats[stat_id]

            stat_content = _calculate_delta_stat(stat_id, prev_stat, cur_stat)
            if stat_content is not None:
                ret[stat_id] = stat_content

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
        ret[node_id] = {"by_uid": {}, "cumulative": {}}
        ret[node_id]["cumulative"] = _calculate_delta_stats(
            prev[node_id]["cumulative"], cur[node_id]["cumulative"]
        )

    return ret


def _clean_blob(x):
    """
    Utility function that decodes a blob as an UTF-8 string, as the direct node C API
    does not yet support 'string' types and as such all statistics are stored as blobs.
    """
    x_ = x.decode("utf-8", "replace")

    # remove trailing zero-terminator
    return "".join(c for c in x_ if ord(c) != 0)

def _get_stat(dconn, k):
    # Ugly, but works: try to retrieve as integer, if not an int, retrieve as
    # blob
    try:
        return dconn.integer(k).get()

    # Older versions of qdb api returned 'alias not found'
    except quasardb.quasardb.AliasNotFoundError:
        return _clean_blob(dconn.blob(k).get())

    # Since ~ 3.14.2, it returns 'Incompatible Type'
    except quasardb.quasardb.IncompatibleTypeError:
        return _clean_blob(dconn.blob(k).get())


def _by_uid(stats, idx):
    xs = {}
    for k, v in stats.items():
        matches = user_pattern.match(k)
        if is_user_stat(k) and matches:
            (metric, uid_str) = matches.groups()
            uid = int(uid_str)
            if uid not in xs:
                xs[uid] = {}

            if not metric.startswith("serialized"):
                xs[uid][metric] = v

    return xs


def _cumulative(stats, idx):
    xs = {}

    for k, v in stats.items():
        matches = total_pattern.match(k)
        if is_cumulative_stat(k) and matches:
            metric = matches.groups()[0]
            if metric.split(".")[-1] not in ["type", "unit"]:
                metric += ".value"
            metric_name, metric_att = metric.rsplit(".", 1)
            if not metric.startswith("serialized"):
                xs[metric_name] = {**xs.get(metric_name, {}), metric_att: v}

    return xs
