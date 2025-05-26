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
        "Dateparser library is required to use QuasarDB dask integration."
    ) from err

general_select_pattern = re.compile(r"(?i)^\s*SELECT\b")
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
    # XXX:igor for now this works for queries using one table
    # tags and multiple tables are not supported yet

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
    else:
        logger.debug(
            "No range found in query, querying table for first and last timestamp"
        )
        range_query = f"SELECT first($timestamp), last($timestamp) FROM {table_name}"
        df = qdbpd.query(conn, range_query)
        if not df.empty:
            df.loc[0, "last($timestamp)"] += datetime.timedelta(microseconds=1)
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


def _get_subqueries(conn, query: str, table_name: str) -> list[str]:
    # this will be moved to c++ functions in the future
    shard_size = conn.table(table_name.replace('"', "")).get_shard_size()
    start, end = _extract_range_from_query(conn, query, table_name)
    ranges_to_query = conn.split_query_range(start, end, shard_size)

    subqueries = []
    for rng in ranges_to_query:
        subqueries.append(_create_subrange_query(query, rng))
    return subqueries


def _get_meta(conn, query: str, query_kwargs: dict) -> pd.DataFrame:
    """
    Returns empty dataframe with the expected schema of the query result.
    """
    np_res = conn.validate_query(query)
    col_dtypes = {}
    for id, column in enumerate(np_res):
        col_dtypes[column[0]] = pd.Series(dtype=column[1].dtype)

    df = pd.DataFrame(col_dtypes)
    if query_kwargs["index"]:
        df.set_index(query_kwargs["index"], inplace=True)
    return df


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
    if not re.match(general_select_pattern, query):
        raise NotImplementedError(
            "Only SELECT queries are supported. Please refer to the documentation for more information."
        )

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
        meta = _get_meta(conn, query, query_kwargs)
        subqueries = _get_subqueries(conn, query, table_name)

    if len(subqueries) == 0:
        logging.warning("No subqueries, returning empty dataframe")
        return meta

    parts = []
    for subquery in subqueries:
        parts.append(
            delayed(_read_dataframe)(subquery, meta, conn_kwargs, query_kwargs)
        )
    logger.debug("Assembled %d subqueries", len(parts))

    return dd.from_delayed(parts, meta=meta)
