# quasardb Python API

## Installation

You can download a precompiled egg directly from our site, or you can build from the sources.

The QuasarDB Python API requires [numpy](http://www.numpy.org/).

### quasardb C API

To build the Python API, you will need the C API. It can either be installed on the machine (e.g. on unix in `/usr/lib` or `/usr/local/lib`) or you can unpack the C API archive in qdb. You will also need the daemon to run the tests.

### Building the extension

The QuasarDB API module is written in C++ 17 using [pybind11](https://github.com/pybind/pybind11).

You will need [CMake](http://www.cmake.org/) and the Python dist tools installed.

First, run cmake in a project directory (here ```build```):

```
mkdir build
cd build
cmake -G "your generator" -DCMAKE_BUILD_TYPE=Release ..
```

Then compile via CMake:

```
cmake --build . --config Release
```

This will compile the modules and create in ```build/dist``` the different packages for your specific platform.

## Running the tests

To run the tests, you will need to have installed in ```qdb``` the daemon (download it from our web site). You will also need the ```xmlrunner``` extension.

Then you run the test from ```build``` with:

```
ctest -C Release . --verbose
```

## Usage

Using *quasardb* starts with a Cluster:

```python
import quasardb

c = quasardb.Cluster('qdb://127.0.0.1:2836')
```

### Blob API

Now that we have a connection to the cluster, let's store some binary data:

```python
b = c.blob('bam')

b.put('boom')
v = b.get() # returns 'boom'
```

### Timeseries API

What about time series you say?

You get an object in the same fashion than for a blob:

```python
ts = c.ts("dat_ts")

ts.create([quasardb.ColumnInfo(quasardb.ColumnType.Double, "doubles"), quasardb.ColumnInfo(quasardb.ColumnType.Blob, "blobs")])
```

Then you can directly insert numpy arrays:

```python
import numpy as np

dates = np.arange(np.datetime64('2015-07-01'), np.datetime64('2015-07-11')).astype('datetime64[ns]')
values = np.arange(0.0, 10.0, 1.0)

ts.double_insert("doubles", dates, values)
```

It's also possible to get the raw values:

```python
# results will contain the timestamps and the values in a couple of numpy arrays
results = ts.double_get_ranges("doubles", [(np.datetime64('2015-07-01', 'ns'), np.datetime64('2015-07-11', 'ns'))])
```

And last but not least, run queries:

```python
q = c.query("select blobs from dat_ts in range(2015-07-01, +10d)")
# results.tables will contain a dictionary mapped to every table
results = q.run()
```

## Compilation Issues

`ImportError: No module named builtins`

Can be solved installing `future` library

```shell
pip install future
```
