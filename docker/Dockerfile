ARG PYTHON_VERSION

FROM centos:7

ARG PYTHON_VERSION

RUN yum install -y \

        which \

        # For recent GCC
        centos-release-scl \

        # For cmake3
        epel-release

RUN yum install -y devtoolset-7 cmake3 make python3-devel python3-wheel git
RUN alternatives --install /usr/bin/cmake cmake /usr/bin/cmake3 10

RUN scl enable devtoolset-7 bash

RUN ls -l /usr/lib/gcc* || true
RUN ls -l /usr/lib64/gcc* || true
RUN which gcc-7 || true
RUN which gcc || true
RUN which g++-7 || true
RUN which g++ || true

RUN ldconfig -v
RUN ln -fs /usr/local/bin/gcc /usr/local/bin/cc
RUN ln -fs /usr/local/bin/g++ /usr/local/bin/c++

# Install python $version
RUN mkdir -p /tmp/python-build
WORKDIR /tmp/python-build
RUN curl -sSL https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz | tar x --xz --strip=1 
RUN ./configure --enable-optimizations --prefix=/usr/local
RUN make altinstall

WORKDIR "~/"
RUN rm -rf /tmp/python-build
ENV PATH=/usr/local/bin:$PATH
ENV PYTHON=python$PYTHON_VERSION
RUN ln -s /usr/local/bin/python3.6 /usr/bin/python3

# CentOS 7 comes with very old Python modules
RUN curl -sSL https://bootstrap.pypa.io/get-pip.py | $PYTHON -
RUN $PYTHON -m pip install --upgrade setuptools wheel

