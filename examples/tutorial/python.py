# import-start
import json
import quasardb
import numpy as np
# import-end

def do_something_async_with(x):
    pass

def test_pool():
    # pool-connect-start
    import quasardb.pool as pool

    # Always initialize the connection pool global singleton.
    pool.initialize(uri="qdb://127.0.0.1:2836")

    # You can use the connection pool instance directly like this:
    with pool.instance().connect() as conn:
        # ... do something with conn
        pass

    # Alternatively, you can make use of the decorator function which handles
    # connection management for you
    @pool.with_conn()
    def my_handler(conn, additional_args):
        # By default, `conn` is always injected as the first argument
        result = conn.query()

    # pool-connect-end

# connect-start
with quasardb.Cluster("qdb://127.0.0.1:2836") as c:
# connect-end
    def secure_connect():
        user_key = {}
        cluster_key = ""

        with open('user_private.key', 'r') as user_key_file:
            user_key = json.load(user_key_file)
        with open('cluster_public.key', 'r') as cluster_key_file:
            cluster_key = cluster_key_file.read()

        # secure-connect-start
        with quasardb.Cluster(uri='qdb://127.0.0.1:2836',
                              user_name=user_key['username'],
                              user_private_key=user_key['secret_key'],
                              cluster_public_key=cluster_key) as scs:
        # secure-connect-end
            pass

    # create-table-start

    # First we acquire a reference to a table (which may or may not yet exist)
    t = c.table("stocks")

    # Initialize our column definitions
    cols = [quasardb.ColumnInfo(quasardb.ColumnType.Double, "open"),
            quasardb.ColumnInfo(quasardb.ColumnType.Double, "close"),
            quasardb.ColumnInfo(quasardb.ColumnType.Int64, "volume")]

    # Now create the table with the default shard size
    t.create(cols)

    # create-table-end

    # tags-start

    t.attach_tag("nasdaq")

    # tags-end

    # batch-insert-start

    # We need to tell the batch inserter which columns we plan to insert. Note
    # how we give it a hint that we expect to insert 2 rows for each of these columns.
    batch_columns = [quasardb.BatchColumnInfo("stocks", "open", 2),
                     quasardb.BatchColumnInfo("stocks", "close", 2),
                     quasardb.BatchColumnInfo("stocks", "volume", 2)]

    # Now that we know which columns we want to insert, we initialize our batch inserter.
    inserter = c.inserter(batch_columns)


    # Insert the first row: to start a new row, we must provide it with a mandatory
    # timestamp that all values for this row will share. QuasarDB will use this timestamp
    # as its primary index.
    #
    # QuasarDB only supports nanosecond timestamps, so we must specifically convert our
    # dates to nanosecond precision.
    inserter.start_row(np.datetime64('2019-02-01', 'ns'))

    # We now set the values for our columns by their relative offsets: column 0 below
    # refers to the first column we provide in the batch_columns variable above.
    inserter.set_double(0, 3.40)
    inserter.set_double(1, 3.50)
    inserter.set_int64(2, 10000)

    # We tell the batch inserter to start a new row before we can set the values for the
    # next row.
    inserter.start_row(np.datetime64('2019-02-02', 'ns'))

    inserter.set_double(0, 3.50)
    inserter.set_double(1, 3.55)
    inserter.set_int64(2, 7500)

    # Now that we're done, we push the buffer as one single operation.
    inserter.push()

    # batch-insert-end


    # bulk-read-start

    # We can initialize a bulk reader based directly from our table. By
    # providing a dict=True parameter, the QuasarDB API will automatically
    # expose our rows as dicts.
    reader = t.reader(dict=True, ranges=[(np.datetime64('2019-02-01', 'ns'), np.datetime64('2019-02-02', 'ns'))])

    # The bulk reader is exposed as a regular Python iterator
    for row in reader:

        # We can access the row locally within our loop:
        print(row)

        # But because the QuasarDB Python API is zero-copy, our row maintains a
        # reference to the underlying data. If we want to keep the row data alive
        # longer than the local scope, you can use row.copy() as follows:
        do_something_async_with(row.copy())

    # bulk-read-end

    # column-insert-start

    # Our API is built on top of numpy, and provides zero-copy integration with native
    # numpy arrays. As such, we first prepare three different arrays for each of our three
    # columns:
    opens = np.array([3.40, 3.50], dtype=np.float64)
    closes = np.array([3.50, 3.55], dtype=np.float64)
    volumes = np.array([10000, 7500], dtype=np.int64)

    # Seperately, we generate a numpy array of timestamps. Since our three columns share
    # the same timestamps, we can reuse this array for all of them, but this is not required.
    timestamps = np.array([np.datetime64('2019-02-01'), np.datetime64('2019-02-02')], dtype='datetime64[ns]')

    # When inserting, we provide the value arrays en timestamp arrays separately.
    t.double_insert("open", timestamps, opens)
    t.double_insert("close", timestamps, closes)
    t.int64_insert("volume", timestamps, volumes)

    # column-insert-end

    # column-get-start

    # We first prepare the intervals we want to select data from, that is, a list of
    # timeranges. An interval is defined as a tuple of start time (inclusive) and end
    # time (exclusive).
    #
    # In this example, we just use a single interval.
    intervals = np.array([(np.datetime64('2019-02-01', 'ns'), np.datetime64('2019-02-02', 'ns'))])

    # As with insertion, our API works with native numpy arrays and returns the results as such.
    (timestamps1, opens) = t.double_get_ranges("open", intervals)
    (timestamps2, closes) = t.double_get_ranges("close", intervals)
    (timestamps3, volumes) = t.int64_get_ranges("volume", intervals)

    # For this specific dataset, timestamps1 == timestamps2 == timestamps3, but
    # this does not necessarily have to be the case.
    np.testing.assert_array_equal(timestamps1, timestamps2)
    np.testing.assert_array_equal(timestamps1, timestamps3)

    # column-get-end

    # query-start

    result = c.query("SELECT SUM(volume) FROM stocks")

    # results is returned as a list of dicts
    for row in result:
        print("row: ", row)


    # Since we only expect one row, we also access it like this:
    aggregate_result = result[0]['sum(volume)']
    print("sum(volume): ", aggregate_result)

    # query-end

    # drop-table-start

    # Use the earlier reference of the table we acquired to remove it:
    t.remove()

    # drop-table-end
