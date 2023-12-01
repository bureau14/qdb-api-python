ARG POLICY="manylinux2014"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"

FROM quay.io/pypa/$PLATFORM

ARG DEVTOOLSET_VERSION="10"
ENV PATH="/opt/rh/devtoolset-$DEVTOOLSET_VERSION/root/usr/bin:$PATH"

ARG PYTHON_VERSION

ADD set-python-version.sh /set-python-version.sh
RUN bash /set-python-version.sh ${PYTHON_VERSION} \
    && rm /set-python-version.sh

RUN yum install -y devtoolset-$DEVTOOLSET_VERSION-toolchain \
    && g++ --version
