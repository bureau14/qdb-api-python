# pylint: disable=C0103,C0111,C0302,R0903

# Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of quasardb nor the names of its contributors may
#      be used to endorse or promote products derived from this software
#      without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import logging
import quasardb
import datetime
import re

logger = logging.getLogger("quasardb.dask")


class DaskRequired(ImportError):
    """
    Exception raised when trying to use QuasarDB dask integration, but
    required packages are not installed has not been installed.
    """

    pass


try:
    import dask.dataframe as dd
    from dask.delayed import delayed
    import quasardb.pandas as qdbpd
    import pandas as pd

except ImportError as err:
    logger.exception(err)
    raise DaskRequired(
        "The dask library is required to use QuasarDB dask integration."
    ) from err

class DateParserRequired(ImportError):
    pass

try:
    import dateparser

except ImportError as err:
    logger.exception(err)
    raise DaskRequired(
        "dateparser required!"
    ) from err

table_pattern = re.compile(r"(?i)\bFROM\s+([`\"\[]?\w+[`\"\]]?)")
range_pattern = re.compile(r"(?i)\bIN\s+RANGE\s*\(([^,]+),\s*([^,]+)\)")


def _read_dataframe(
    query: str, meta: pd.DataFrame, conn_kwargs: dict, query_kwargs: dict
) -> qdbpd.DataFrame:
    logger.debug('Querying QuasarDB with query: "%s"', query)
    with quasardb.Cluster(**conn_kwargs) as conn:
        df = qdbpd.query(conn, query, **query_kwargs)

    if len(df) == 0:
        return meta
    else:
        return df


def _extract_table_name_from_query(query: str) -> str:
    # XXX:igor for now this works for queries using only one table

    logger.debug('Extracting table name from query: "%s"', query)
    match = re.search(table_pattern, query)
    if match:
        table_name = match.group(1)
        logger.debug('Extracted table name: "%s"', table_name)
        return table_name
    else:
        raise ValueError("Could not extract table name from query. ")


def _extract_range_from_query(conn, query: str, table_name: str) -> tuple:
    """
    Extracts the range from the query, parses it to datetime and returns.
    If no range is found in the query, it queries the table for the first and last timestamp.
    """
    logger.debug('Extracting query range from: "%s"', query)
    match = re.search(range_pattern, query)
    # first we check try to extract "in range (start, end)" from query
    # if we can't do it we will query first() and last() from the table
    query_range = tuple()
    if match:
        if len(match.group()) == 2:
            start_str = match.group(1)
            end_str = match.group(2)
            logger.debug("Extracted strings: (%s, %s)", start_str, end_str)
            parser_settings = {
                "PREFER_DAY_OF_MONTH": "first",
                "PREFER_MONTH_OF_YEAR": "first",
            }
            start_date = dateparser.parse(start_str, settings=parser_settings)
            end_date = dateparser.parse(end_str, settings=parser_settings)
            query_range = (start_date, end_date)
            logger.debug("Parsed datetime: %s", query_range)
            return query_range

    logger.debug(
        "No range found in query, querying table for first and last timestamp"
    )
    range_query = f"SELECT first($timestamp), last($timestamp) FROM {table_name}"
    df = qdbpd.query(conn, range_query)
    if not df.empty:
        df.iloc[0]["last($timestamp)"] += datetime.timedelta(microseconds=1)
        query_range += tuple(df.iloc[0])
        logger.debug("Extracted range from table: %s", query_range)
    return query_range


def _create_subrange_query(
    query: str, query_range: tuple[datetime.datetime, datetime.datetime]
) -> str:
    """
    Adds range to base query.
    IF range is found in the query, it will be replaced with the new range.
    IF no range is found, it will be added after the "FROM {table}" clause.
    """
    new_query = query
    range_match = re.search(range_pattern, query)
    start_str = query_range[0].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    end_str = query_range[1].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if range_match:
        if len(range_match.groups()) == 2:
            new_query = re.sub(
                range_pattern,
                f"IN RANGE ({start_str}, {end_str})",
                query,
            )
        logger.debug("Created subquery: %s", new_query)
        return new_query

    table_match = re.search(table_pattern, query)
    new_query = re.sub(
        table_pattern,
        f"FROM {table_match.group(1)} IN RANGE ({start_str}, {end_str})",
        query,
    )
    logger.debug("Created subquery: %s", new_query)
    return new_query


def _split_by_timedelta(
    query_range: tuple[datetime.datetime, datetime.datetime], delta: datetime.timedelta
) -> tuple[datetime.datetime, datetime.datetime]:
    """
    Splits passed range into smaller ranges of size of delta.
    """

    ranges = []

    if len(query_range) != 2:
        return ranges

    start = query_range[0]
    end = query_range[1]
    current_start = start

    while current_start < end:
        current_end = min(current_start + delta, end)
        ranges.append((current_start, current_end))
        current_start = current_end

    return ranges


def _get_meta(conn, query: str) -> pd.DataFrame:
    """
    Meta is an empty dataframe with the expected schema of the query result.
    Extract df schema from the first row of the query result.
    """
    # XXX:igor we can use different approaches to get the meta
    # 1. get the meta from the first row
    #   this will require us to modify passed query, we have to add a limit 1 (check for existing limit, check for offset)
    #   does this put a lot of pressure on the db?
    #   this approach provides the most accurate meta
    # 2. get the meta from the table schema
    #   we will have to add $timestamp and $table for "select *" queries
    #   with more complicated queries we can run into different edge cases
    #       we will have to extract actual col names and aliases
    #       get types for col names and assign them to the aliases
    #   will be less accurate
    query += " LIMIT 1"
    return qdbpd.query(conn, query).iloc[:0]


def query(
    query: str,
    uri: str,
    *,
    user_name: str = "",
    user_private_key: str = "",
    cluster_public_key: str = "",
    user_security_file: str = "",
    cluster_public_key_file: str = "",
    timeout: datetime.timedelta = datetime.timedelta(seconds=60),
    do_version_check: bool = False,
    enable_encryption: bool = False,
    client_max_parallelism: int = 0,
    index=None,
    blobs: bool = False,
    numpy: bool = False,
):
    conn_kwargs = {
        "uri": uri,
        "user_name": user_name,
        "user_private_key": user_private_key,
        "cluster_public_key": cluster_public_key,
        "user_security_file": user_security_file,
        "cluster_public_key_file": cluster_public_key_file,
        "timeout": timeout,
        "do_version_check": do_version_check,
        "enable_encryption": enable_encryption,
        "client_max_parallelism": client_max_parallelism,
    }

    query_kwargs = {
        "index": index,
        "blobs": blobs,
        "numpy": numpy,
    }


    table_name = _extract_table_name_from_query(query)
    with quasardb.Cluster(**conn_kwargs) as conn:
        meta = _get_meta(conn, query)
        shard_size = conn.table(table_name.replace("\"", "")).get_shard_size()
        query_range = _extract_range_from_query(conn, query, table_name)
        # XXX:igor this will work good for tables with a lot of data in all buckets
        # for small tables we end up with a lot of small queries
        #
        # dd.read_sql_query function estimates data size from X first rows,
        # then splits the data so it fits into "bytes_per_chunk" parameter
        # (i think) it uses limit and offset to do this
        ranges_to_query = _split_by_timedelta(query_range, shard_size)

    if len(ranges_to_query) == 0:
        logging.warning("No ranges to query, returning empty dataframe")
        return meta

    logger.debug("Assembling subqueries for %d ranges", len(ranges_to_query))
    parts = []
    for rng in ranges_to_query:
        sub_query = _create_subrange_query(query, rng)
        parts.append(
            delayed(_read_dataframe)(sub_query, meta, conn_kwargs, query_kwargs)
        )
    logger.debug("Assembled %d subqueries", len(parts))

    return dd.from_delayed(parts, meta=meta)
