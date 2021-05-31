ARG PYTHON_VERSION

FROM centos:7

RUN yum install -y \

        # For recent GCC
        centos-release-scl \

        # For cmake3
        epel-release

RUN yum install -y devtoolset-7-gcc-c++ cmake3 make python3-devel python3-wheel git

# Install python $version
RUN mkdir -p /tmp/python-build
WORKDIR /tmp/python-build
RUN curl -sSL ${URLS["python${PYTHON_VERSION}.tar.xz"]} | tar x --xz --strip=1 https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz
RUN ./configure --enable-optimizations --prefix=/usr/local
RUN make -j72
RUN make install

WORKDIR "~/"
RUN rm -rf /tmp/python-build
ENV PATH=/usr/local/bin:$PATH
ENV PYTHON=python${PYTHON_VERSION}

# CentOS 7 comes with very old Python modules
RUN curl -sSL https://bootstrap.pypa.io/get-pip.py | ${PYTHON} -
RUN ${PYTHON} -m pip install --upgrade setuptools wheel


RUN alternatives --install /usr/bin/cmake cmake /usr/bin/cmake3 10
