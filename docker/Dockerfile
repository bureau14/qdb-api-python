ARG PYTHON_VERSION

FROM centos:7

ARG PYTHON_VERSION

RUN yum install -y \
        centos-release-scl \
        epel-release

RUN yum update -y openssl openssl-libs
RUN yum install -y openssl-devel
RUN yum install -y devtoolset-8 cmake3 make git zlib-devel bzip2-devel libffi-devel xz-devel
RUN alternatives --install /usr/bin/cmake cmake /usr/bin/cmake3 10

RUN scl enable devtoolset-8 bash

RUN ln -fs /opt/rh/devtoolset-8/root/usr/bin/gcc /usr/local/bin/cc
RUN ln -fs /opt/rh/devtoolset-8/root/usr/bin/g++ /usr/local/bin/c++
RUN ln -fs /opt/rh/devtoolset-8/root/usr/bin/gcc /usr/bin/cc
RUN ln -fs /opt/rh/devtoolset-8/root/usr/bin/g++ /usr/bin/c++

RUN ldconfig -v

# Install python $version
RUN mkdir -p /tmp/python-build
WORKDIR /tmp/python-build
RUN curl -sSL https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz | tar x --xz --strip=1 
RUN ./configure --enable-optimizations --prefix=/usr/local
RUN make altinstall

WORKDIR "~/"
RUN rm -rf /tmp/python-build
ENV PATH=/usr/local/bin:$PATH
RUN mv /usr/bin/python3 /usr/bin/python3.old
RUN ln -s /usr/local/bin/python`bash -c "echo ${PYTHON_VERSION%.*}"` /usr/bin/python3
RUN python3 --version
# run mv /usr/bin/python /usr/bin/python.old
# RUN ln -s /usr/local/bin/python3.6 /usr/bin/python

# CentOS 7 comes with very old Python modules
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --upgrade setuptools wheel

RUN echo "source /opt/rh/devtoolset-8/enable" >> ~/.bashrc
