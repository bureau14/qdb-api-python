import time
import quasardb
import logging
import numpy as np

FIREHOSE_TABLE = "$qdb.firehose"
POLL_INTERVAL = 0.1

logger = logging.getLogger('quasardb.firehose')


def _init():
    """
    Initialize our internal state.
    """
    return {'last': None,
            'seen': set()}


def _get_transactions_since(conn, table_name, last):
    """
    Retrieve all transactions since a certain timestamp. `last` is expected to be a dict
    firehose row with at least a $timestamp attached.
    """
    if last is None:
        q = "SELECT $timestamp, transaction_id, begin, end FROM \"{}\" WHERE table = '{}' ORDER BY $timestamp".format(
            FIREHOSE_TABLE, table_name)
    else:
        q = "SELECT $timestamp, transaction_id, begin, end FROM \"{}\" IN RANGE ({}, +1y) WHERE table = '{}' ORDER BY $timestamp".format(
            FIREHOSE_TABLE, last['$timestamp'], table_name)

    return conn.query(q)


def _get_transaction_data(conn, table_name, begin, end):
    """
    Gets all data from a certain table.
    """
    q = "SELECT * FROM \"{}\" IN RANGE ({}, {}) ".format(
        table_name, begin, end)
    return conn.query(q)


def _get_next(conn, table_name, state):

    # Our flow to retrieve new data is as follows:
    # 1. Based on the state's last processed transaction, retrieve all transactions
    #    that are logged into the firehose since then.
    # 2. For each of the transactions, verify we haven't seen it before
    # 3. For each of the transactions, pull in all data
    # 4. Concatenate all this data (in order of quasardb transaction)

    txs = _get_transactions_since(conn, table_name, state['last'])

    xs = list()
    for tx in txs:
        txid = tx['transaction_id']

        if state['last'] is not None and tx['$timestamp'] > state['last']['$timestamp']:
            # At this point we are guaranteed that the transaction we encounter is
            # 'new', will not conflict with any other transaction ids. It is thus
            # safe to reset the txid set.
            state['seen'] = set()

        if txid not in state['seen']:
            xs = xs + _get_transaction_data(conn,
                                            table_name,
                                            tx['begin'],
                                            # The firehose logs transaction `end` span as
                                            # end inclusive, while our bulk reader and/or query
                                            # language are end exclusive.
                                            tx['end'] + np.timedelta64(1, 'ns'))

            # Because it is possible that multiple firehose changes are stored with the
            # exact same $timestamp, we also keep track of the actually seen
            # transaction ids.
            state['seen'].add(txid)

        state['last'] = tx

    return (state, xs)


def subscribe(conn, table_name):
    state = _init()

    while True:
        # Note how this is effectively a never-ending fold loop
        # that transforms state into a new state. This state effectively
        # functions as a checkpoint.
        #
        # At a later point, we could choose to provide the user
        # direct access to this 'state' object, so that they can
        # implement e.g. mechanisms to replay from a certain checkpoint.
        (state, xs) = _get_next(conn, table_name, state)

        for x in xs:
            yield x

        # Our poll interval
        time.sleep(POLL_INTERVAL)
