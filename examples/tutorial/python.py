# import-start
import quasardb
import numpy as np
# import-end

# connect-start
c = quasardb.Cluster("qdb://127.0.0.1:28360")
# connect-end

def secure_connect():
    # secure-connect-start
    sc = quasardb.Cluster(uri='qdb://127.0.0.1:2836',
                          user_name='user_name',
                          user_private_key='SL8sm9dM5xhPE6VNhfYY4ib4qk3vmAFDXCZ2FDi8AuJ4=',
                          cluster_public_key='PZMBhqk43w+HNr9lLGe+RYq+qWZPrksFWMF1k1UG/vwc=')
    # secure-connect-end

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
