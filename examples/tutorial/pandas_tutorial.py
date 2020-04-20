# import-start
import quasardb
import quasardb.pandas as qdbpd

import pandas as pd
import numpy as np
# import-end

with quasardb.Cluster("qdb://127.0.0.1:2836") as c:

    # batch-insert-start

    # Prepare the entire DataFrame which we wish to store
    data = {'open': [3.40, 3.50],
            'close': [3.50, 3.55],
            'volume': [10000, 7500]}
    timestamps = np.array([np.datetime64('2019-02-01'),
                           np.datetime64('2019-02-02')],
                          dtype='datetime64[ns]')
    df = pd.DataFrame(data=data,
                      index=timestamps)

    # By providing the create=True parameter, we explicitly tell the Pandas connector to
    # create a new table based on the schema
    # of the DataFrame.
    qdbpd.write_dataframe(df, c, "stocks", create=True)

    # batch-insert-end


    # bulk-read-start

    ranges = [(np.datetime64('2019-02-01', 'ns'), np.datetime64('2019-02-02', 'ns'))]
    t = c.table("stocks")

    # By providing the row_index=True parameter, we explicitly tell the Pandas connector
    # to retrieve rows using the row-oriented bulk reader and use the default Pandas sequential
    # row index. This is useful if your dataset is sparse and may contain null values.
    df = qdbpd.read_dataframe(t, row_index=True, ranges=ranges)

    # bulk-read-end

    # column-insert-start

    # The Pandas connector maps QuasarDB's column-oriented API to Pandas Series. As such,
    # we initialize three Series for each of our three columns.
    #
    # Seperately, we generate a numpy array of timestamps. Since our three columns share
    # the same timestamps, we can reuse this as the index for all our Series.
    timestamps = np.array([np.datetime64('2019-02-01'), np.datetime64('2019-02-02')], dtype='datetime64[ns]')

    opens = pd.Series(data=[3.40, 3.50], index=timestamps, dtype=np.float64)
    closes = pd.Series(data=[3.50, 3.55], index=timestamps, dtype=np.float64)
    volumes = pd.Series(data=[10000, 7500], index=timestamps, dtype=np.int64)

    # We use the write_series function to insert column by column.
    qdbpd.write_series(opens, t, "open")
    qdbpd.write_series(closes, t, "close")
    qdbpd.write_series(volumes, t, "volume")

    # column-insert-end

    # column-get-start

    # We first prepare the intervals we want to select data from, that is, a list of
    # timeranges. An interval is defined as a tuple of start time (inclusive) and end
    # time (exclusive).
    #
    # In this example, we just use a single interval.
    intervals = np.array([(np.datetime64('2019-02-01', 'ns'), np.datetime64('2019-02-02', 'ns'))])

    # We can then use the read_series function to read column by column. The objects
    # returned are regular pd.Series objects.
    opens = qdbpd.read_series(t, "open", intervals)
    closes = qdbpd.read_series(t, "close", intervals)
    volumes = qdbpd.read_series(t, "volume", intervals)

    # column-get-end

    # query-start

    df = qdbpd.query(c, "SELECT SUM(volume) FROM stocks")

    # The API returns dataframe
    print("result: ", df)

    # query-end

    c.table("stocks").remove()
