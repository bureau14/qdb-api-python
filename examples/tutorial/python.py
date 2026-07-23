# import-start
import json
import quasardb
import quasardb.numpy as qdbnp
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

        with open("user_private.key", "r") as user_key_file:
            user_key = json.load(user_key_file)
        with open("cluster_public.key", "r") as cluster_key_file:
            cluster_key = cluster_key_file.read()

        # secure-connect-start
        with quasardb.Cluster(
            uri="qdb://127.0.0.1:2836",
            user_name=user_key["username"],
            user_private_key=user_key["secret_key"],
            cluster_public_key=cluster_key,
        ) as scs:
            # secure-connect-end
            pass

    # create-table-start

    # First we acquire a reference to a table (which may or may not yet exist)
    t = c.table("stocks")

    # Initialize our column definitions
    cols = [
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "open"),
        quasardb.ColumnInfo(quasardb.ColumnType.Double, "close"),
        quasardb.ColumnInfo(quasardb.ColumnType.Int64, "volume"),
    ]

    # Now create the table with the default shard size
    t.create(cols)

    # create-table-end

    # tags-start

    t.attach_tag("nasdaq")

    # tags-end

    # batch-insert-start

    # Prepare the data we want to write, keyed by column name.
    data = {
        "open": np.array([3.40, 3.50]),
        "close": np.array([3.50, 3.55]),
        "volume": np.array([10000, 7500]),
    }

    # QuasarDB only supports nanosecond timestamps, so we specifically convert our
    # dates to nanosecond precision.
    index = np.array(
        [np.datetime64("2019-02-01", "ns"), np.datetime64("2019-02-02", "ns")]
    )

    # Now that we're done, we push the arrays as one single operation.
    qdbnp.write_arrays(data, c, t, index=index, infer_types=False)

    # batch-insert-end

    # bulk-read-start

    # We can initialize a bulk reader based directly from our table. The reader needs to open
    # and close to acquire / release the appropriate resources.
    with t.reader(
        ranges=[(np.datetime64("2019-02-01", "ns"), np.datetime64("2019-02-02", "ns"))]
    ) as reader:

        # The bulk reader is exposed as a regular Python iterator, streaming batches of data.
        for batch in reader:

            # We can access a batch of data, which is a regular python dict with column names
            # as keys and numpy MaskedArrays as data.
            print(batch)

            do_something_async_with(batch)

    # bulk-read-end

    # query-start

    result = c.query("SELECT SUM(volume) FROM stocks")

    # results is returned as a list of dicts
    for row in result:
        print("row: ", row)

    # Since we only expect one row, we also access it like this:
    aggregate_result = result[0]["SUM(volume)"]
    print("sum(volume): ", aggregate_result)

    # query-end

    # drop-table-start

    # Use the earlier reference of the table we acquired to remove it:
    t.remove()

    # drop-table-end
