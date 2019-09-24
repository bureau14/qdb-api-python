# import-start
import quasardb
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
