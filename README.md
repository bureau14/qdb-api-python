# QuasarDB Python API

The QuasarDB Python API is built and tested against the following versions:

- Python 3.5
- Python 3.6
- Python 3.7

In addition to this, we support the following environments:

- MacOS 10.9+
- Microsoft Windows
- Linux
- FreeBSD

## Installation via PyPi

#### Windows and MacOS

The QuasarDB Python API is distributed using a PyPi package, and can be installed as follows:

```bash
$ pip install quasardb
```

This will download the API and install all its dependencies.

#### Linux

For Linux users, installation via pip through PyPi will trigger a compilation of this module. This will require additional packages to be installed:

- A modern C++ compiler (llvm, g++)
- CMake 3.5 or higher
- [QuasarDB C API](https://doc.quasardb.net/master/api/c.html)

##### Ubuntu / Debian

On Ubuntu or Debian, this can be achieved as follows:

```bash
$ apt install apt-transport-https ca-certificates -y
$ echo "deb [trusted=yes] https://repo.quasardb.net/apt/ /" > /etc/apt/sources.list.d/quasardb.list
$ apt update
$ apt install qdb-api cmake g++
$ pip install wheel
$ pip install quasardb
```

##### RHEL / CentOS

On RHEL or CentOS, the process is a bit more involved because we need a modern GCC compiler and cmake. It can be achieved as follows:

```bash
# Enable SCL for recent gcc
$ yum install centos-release-scl -y

# Enable EPEL for recent cmake
$ yum install epel-release -y

# Enable QuasarDB Repository
$ echo $'[quasardb]\nname=QuasarDB repo\nbaseurl=https://repo.quasardb.net/yum/\nenabled=1\ngpgcheck=0' > /etc/yum.repos.d/quasardb.repo

$ yum install devtoolset-7-gcc-c++ cmake3 make qdb-api

# Make cmake3 the default
$ alternatives --install /usr/bin/cmake cmake /usr/bin/cmake3 10

# Start using gcc 7
$ scl enable devtoolset-7 bash

# Install the Python module
$ pip install wheel
$ pip install quasardb
```



## Verifying installation

You can verify the QuasarDB Python module is installed correctly by trying to print the installed version:

```
$ python
Python 3.7.2 (default, Feb 13 2019, 15:08:44)
[GCC 7.3.1 20180303 (Red Hat 7.3.1-5)] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import quasardb
>>> print(quasardb.version())
3.1.0
```

This tells you the currently installed version of the Python module, and the QuasarDB C API it is linked against is 3.1.0. Ensure that this version also matched the version of the QuasarDB daemon you're connecting to.

## Getting started

For instructions on how to use this Python module to interact with a QuasarDB cluster, please refer to [the QuasarDB Python API documentation](https://doc.quasardb.net/master/api/python.html)
