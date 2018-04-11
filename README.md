# quasardb Python API

## Installation

You can download a precompiled egg directly from our site, or you can build from the sources.

### quasardb C API

To build the Python API, you will need the C API. It can either be installed on the machine (e.g. on unix in `/usr/lib` or `/usr/local/lib`) or you can unpack the C API archive in qdb.

### Building the extension

You will need [CMake](http://www.cmake.org/), [SWIG](http://www.swig.org/) and the Python dist tools installed. You can also download a pre-compiled package from our download site.

First, run cmake to create a project directory, for example:

```
mkdir build
cd build
cmake -G "your generator" ..
```

Depending on the generator you chose, you will then either have to run make or open the solution with your editor (e.g. Visual Studio).

For example on UNIX:

```
mkdir build
cd build
cmake -G "Unix Makefiles" ..
make
```

## Usage

Using *quasardb* starts with a Cluster:

```python
import quasardb

c = quasardb.Cluster('qdb://127.0.0.1:2836')
```

Now that we have a connection to the cluster, let's store some binary data:

```python
b = c.blob('bam')

b.put('boom')
v = b.get() # returns 'boom'
```

Want a queue? We have distributed queues.

```python
q = c.deque('q2')

q.push_back('boom')
v = q.pop_front() # returns 'boom'

q.push_front('bang')
```

quasardb comes out of the box with server-side atomic integers:

```python
i = c.integer('some_int')

i.put(3)  # i is equal to 3
i.add(7)  # i is equal to 10
i.add(-5) # is equal to 5
```

What about time series you say?

You can create a time series as such:

```python
ts = c.ts("dat_ts")

cols = ts.create([quasardb.TimeSeries.DoubleColumnInfo("col1"), quasardb.TimeSeries.BlobColumnInfo("col2")])
```

Then you can operate on columns:

```python
col1 = ts.column(quasardb.TimeSeries.DoubleColumnInfo("col1"))

# you can insert as many points as you want
col1.insert([(datetime.datetime.now(quasardb.tz), 1.0)])

# get the average for multiple intervals
# assuming start_time1, end_time1 are datetime.datetime objects
agg = quasardb.TimeSeries.Aggregations()
agg.append(quasardb.TimeSeries.Aggregation.arithmetic_mean, (start_time1, end_time1))
agg.append(quasardb.TimeSeries.Aggregation.arithmetic_mean, (start_time2, end_time2))

avg = col1.aggregate(agg)

# avg[0].value has the average for the first interval
# avg[1].value has the average for the second interval
```

It's also possible to get the raw values:

```python
# results contains the points, in a flattened list
results = col1.get_ranges([(start_time1, end_time1), (start_time2, end_time2)])
```

## Compilation Issues

`ImportError: No module named builtins`

Can be solved installing `future` library

```shell
pip install future
```
