# QuasarDB Python API

[![PyPI version](https://badge.fury.io/py/quasardb.svg)](https://pypi.org/project/quasardb/)

The QuasarDB Python API is built and tested against the following versions:

- Python 3.7
- Python 3.8
- Python 3.9
- Python 3.10
- Python 3.11
- Python 3.12
- Python 3.13

In addition to this, we support the following environments:

- Linux (x86_64 + aarch64)
- macOS (x86_64 + aarch64)
- Windows (win32 + win64)
- FreeBSD (x86_64)

The QuasarDB Python API builds directly on top of the QuasarDB C API, and as such heavily leverages C++ integration. We integrate tightly with Numpy, which enables zero-copy transfer of data from the Python interpreter into the C++ backend.

Binary builds are pushed directly to Pypi. Custom compilation is possible, but intended for people who would like to customize this API itself, rather than use it.

## Installation via PyPi

```
python3 -m pip install quasardb
```

### Linux compatibility

For Linux builds, we use [manylinux2_28](https://github.com/pypa/manylinux) to ensure compatibility with Linux distributions that have glibc version 2.28 or later installed, built on RHEL8. This means that if you are using a Linux distribution that was released in 2019 or later, we support your environment.

This includes, but is not limited to:
* RHEL8+ and derivatives;
* Debian 10 (buster) or later;
* Ubuntu 19.04 or later.

You can check the glibc version installed on your machine as follows:

```
bash-4.2# ldd --version
ldd (GNU libc) 2.28
```

## Getting started

For instructions on how to use this Python module to interact with a QuasarDB cluster, please read our [tutorial](https://doc.quasardb.net/master/user-guide/api/python.html), you can also find the [python reference](https://doc.quasardb.net/master/pydoc/quasardb.quasardb.html).

# Developer information

The QuasarDB Python API builds on top of [pybind11](https://github.com/pybind/pybind11), which (along with several other libraries) is vendored into the git repository.

## Directory structure

| Directory     | Purpose                                                                                                                                                                    |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `doc/`        | Scripts to generate pydoc code                                                                                                                                             |
| `docker/`     | Dockerfile for `manylinux` builds                                                                                                                                          |
| `examples/`   | Example code. Code in the `tutorials/` subdirectory is tested as part of the test suite, and directly injected into our documentation available on https://doc.quasar.ai   |
| `quasardb/`   | C++ source code and python modules.                                                                                                                                        |
| `scripts/`    | Utility scripts for continuous integratioin and development.                                                                                                               |
| `tests/`      | Test suite based on pytest.                                                                                                                                                |
| `thirdparty/` | Vendored c++ libraries                                                                                                                                                     |

## Prepare for running tests

Prerequisite
* c++ compiler that supports C++20
* cmake

The instructions below have been verified to work on:
* Linux (RHEL, Fedora, Ubuntu, Debian and Arch Linux);
* macOS 15.4.1 (amd64 and aarch64);
* Windows WSL.

### QuasarDB tarball extraction

All QuasarDB APIs assume QuasarDB and associated utilities are extracted into the `qdb/` subdirectory.

Extract QuasarDB C API, utilities and server into qdb/
```
mkdir qdb
cd qdb
tar xf <archives>
cd ..
```

### Environment variables

The build/test process heavily relies on certain environment variables to be set. What is required at the minimum is:

* Set `QDB_TESTS_ENABLED` environment variable to ensure that C++ code found in `tests/` is also compiled, which is required to run the tests.
* Set `QDB_ENCRYPT_TRAFFIC` before launching the QuasarDB services, as the Python tests establish an encrypted connection with QuasarDB when tests are ran on the secure cluster.

All the environment variables are discovered and used appropriately by `setup.py`, which controls the C++ build process.


Export required environment variables:

```
export QDB_TESTS_ENABLED=ON
export QDB_ENCRYPT_TRAFFIC=1
```

Build type customization (recommended for development):

```
export CMAKE_BUILD_TYPE="Debug"
```

Using Ninja instead of Make enables parallel builds by default.

Build tool customization (optional):

```
export CMAKE_GENERATOR="Ninja"
```

Compiler customization (optional):

```
export CMAKE_C_COMPILER=/usr/local/clang18/bin/clang
export CMAKE_CXX_COMPILER=/usr/local/clang18/bin/clang++
```

### Launch services

Use the scripts from the qdb-test-setup submodule to start and stop background services. These scripts are used across all QuasarDB API and tools projects. For the Python API, as illustrated above, it is important to set the `QDB_ENCRYPT_TRAFFIC` environment variable:

```
$ scripts/tests/setup/start-services.sh

<.. snip ..>

qdbd secure and insecure were started properly.
```

## Run tests

Invoke the scripts that our continuous integration system uses directly:

```
$ bash scripts/teamcity/20.test.sh

<.. snip a lot ..>

========================================================================================= 9112 passed, 105 skipped, 8741 warnings in 87.43s (0:01:27) ==========================================================================================
$

```

This does the following out of the box:

* Create a virtualenv;
* Install dev requirements in virtualenv;
* Build the .whl file;
* Install the .whl file in virtualenv;
* Invoke pytest on the entire repository in the `tests/` subdirectory.

This process is not as optimal as before, as it doesn't use incremental development. CMake <> Python integration relies on setuptools facilities which are slowly being deprecated. Work to improve this process and bring back incremental builds is tracked in internal ticket QDB-16522.

All arguments passed to this `20.test.sh` script are passed directly to pytest. For example, to enable verbose output and test a single file, you can use this:

```
$ bash scripts/teamcity/20.test.sh -s test_stats.py

<.. snip a lot ..>

+ exec /Users/lmergen/git/qdb-api-python/scripts/teamcity/../../.env/bin/python -m pytest -s test_stats.py
============================================================================================================= test session starts ==============================================================================================================
platform darwin -- Python 3.12.10, pytest-8.3.5, pluggy-1.5.0
benchmark: 5.1.0 (defaults: timer=time.perf_counter disable_gc=False min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=100000)
rootdir: /Users/lmergen/git/qdb-api-python
configfile: pytest.ini
plugins: teamcity-messages-1.29, benchmark-5.1.0, hypothesis-6.131.8
collected 3 items

test_stats.py::test_stats_by_node PASSED
test_stats.py::test_stats_of_node PASSED
test_stats.py::test_stats_regex PASSED

============================================================================================================== 3 passed in 15.21s ==============================================================================================================
$
```
