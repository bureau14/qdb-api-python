FROM centos:7

ARG PYTHON_VERSION
ARG PYTHON_SHA

RUN yum install -y centos-release-scl
RUN yum-config-manager -y --enable rhel-server-rhscl-7-rpms
RUN yum install -y devtoolset-8

RUN yum install -y \
        glibc-devel \
        libstdc++-devel \
        glib2-devel \
        libX11-devel \
        libXext-devel \
        libXrender-devel \
        mesa-libGL-devel \
        libICE-devel \
        libSM-devel \
        ncurses-devel \
        \
        zlib-devel \
        bzip2-devel \
        ncurses-devel \
        sqlite-devel \
        readline-devel \
        tk-devel \
        gdbm-devel \
        db4-devel \
        libpcap-devel \
        xz-devel \
        gpg \
        libffi-devel \
        \
        openssl \
        openssl-devel \
        libtool \
        \
        wget



RUN wget -q https://s3.eu-central-1.amazonaws.com/qdbbuilddeps/linux/python/Python-${PYTHON_VERSION}.tgz \
        && echo "${PYTHON_SHA} Python-${PYTHON_VERSION}.tgz" | sha256sum -c -


RUN mkdir -p /opt/python/src/ \
        && tar -xzf Python-${PYTHON_VERSION}.tgz -C /opt/python/src/ --strip-components 1

RUN mkdir /opt/python/build \
        && cd /opt/python/build \
        && ../src/configure --srcdir=../src \
                            --prefix=/opt/python \
                            --disable-shared \
        \
        && cd /opt/python/build \
        && make -j32  \
        && make install


RUN  /opt/python/bin/pip3 install --upgrade pip
RUN /opt/python/bin/pip3 install \
        certifi \
        auditwheel \
        pyelftools \
        typing \
        \
        xmlrunner \
        numpy \
        pytz
