FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install wget gcc g++ make cmake libssl-dev zlib1g-dev less -y

ARG PYTHON_VERSION="3.5.6"
ARG PYTHON_SHA="30d2ff093988e74283e1abfee823292c6b59590796b9827e95ba4940b27d26f8"

RUN wget -q https://s3.eu-central-1.amazonaws.com/qdbbuilddeps/linux/python/Python-${PYTHON_VERSION}.tgz
RUN echo "${PYTHON_SHA} Python-${PYTHON_VERSION}.tgz" | sha256sum -c -

RUN mkdir -p /opt/python/src/ \
        && tar -xzf Python-${PYTHON_VERSION}.tgz -C /opt/python/src/ --strip-components 1


RUN mkdir /opt/python/build \
        && cd /opt/python/build \
        && ../src/configure --srcdir=../src \
                            --prefix=/opt/python \
                            --enable-ipv6  \
                            --with-system-ffi \
                            --with-computed-gotos
RUN cd /opt/python/build && make -j32
RUN cd /opt/python/build && make install
RUN /opt/python/bin/pip3 install --upgrade pip
RUN /opt/python/bin/pip3 install wheel
