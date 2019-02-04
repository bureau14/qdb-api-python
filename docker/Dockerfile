FROM ubuntu:18.04

ARG PYTHON_VERSION
ARG PYTHON_SHA

RUN apt-get update \
        && apt-get upgrade -y \
        && apt-get install -y wget gcc g++ make cmake libffi-dev libssl-dev zlib1g-dev less \
        && wget -q https://s3.eu-central-1.amazonaws.com/qdbbuilddeps/linux/python/Python-${PYTHON_VERSION}.tgz \
        && echo "${PYTHON_SHA} Python-${PYTHON_VERSION}.tgz" | sha256sum -c - \
        \
        && mkdir -p /opt/python/src/ \
        && tar -xzf Python-${PYTHON_VERSION}.tgz -C /opt/python/src/ --strip-components 1 \
        \
        && mkdir /opt/python/build \
        && cd /opt/python/build \
        && ../src/configure --srcdir=../src \
                            --prefix=/opt/python \
                            --enable-ipv6  \
                            --with-system-ffi \
                            --with-computed-gotos \
        \
        && cd /opt/python/build \
        && make -j32  \
        && make install \
        && /opt/python/bin/pip3 install --upgrade pip \
        && /opt/python/bin/pip3 install wheel xmlrunner numpy
