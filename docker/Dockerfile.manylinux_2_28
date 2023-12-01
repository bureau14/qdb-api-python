ARG POLICY="manylinux_2_28"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"

FROM quay.io/pypa/$PLATFORM

ARG GCC_TOOLSET_VERSION="12"
ENV PATH="/opt/rh/devtoolset-$DEVTOOLSET_VERSION/root/usr/bin:$PATH"

ARG PYTHON_VERSION

ADD set-python-version.sh /set-python-version.sh
RUN bash /set-python-version.sh ${PYTHON_VERSION} \
    && rm /set-python-version.sh

RUN yum install -y gcc-toolset-$GCC_TOOLSET_VERSION \
                   gcc-toolset-$GCC_TOOLSET_VERSION-runtime \
                   gcc-toolset-$GCC_TOOLSET_VERSION-binutils \
                   gcc-toolset-$GCC_TOOLSET_VERSION-gcc \
                   gcc-toolset-$GCC_TOOLSET_VERSION-gcc-c++ \
                   gcc-toolset-$GCC_TOOLSET_VERSION-libstdc++-devel \
    && g++ --version
