FROM quay.io/pypa/manylinux1_x86_64:latest

ARG NUM_CPU=32

RUN echo 'exclude = *.i?86' >> /etc/yum.conf
RUN echo /usr/local/lib > /etc/ld.so.conf.d/local.conf && echo /usr/local/lib64 >> /etc/ld.so.conf.d/local.conf

ENV PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig


# build tools
RUN yum install -y \
        curl \
        bzip2 \
        make \
        m4 \
        file \
        pkgconfig \
        perl \
        expat-devel \
        zlib-devel \
        gettext \
        which \
        openssh-clients \
        rsync \
        bzip2-devel \
        readline-devel \
        mesa-libGLU-devel \
        man \
        xz \
        patch \
        libXt-devel \
        libXtst \
        libXrender \
        libXi \
        wget \
        && yum clean all \
        && rm -rf /usr/share/locale

# cmake
RUN mkdir -p /build

RUN cd /build \
        && curl https://s3.eu-central-1.amazonaws.com/qdbbuilddeps/generic/cmake/cmake-3.12.0.tar.gz | tar -xzv

RUN cd /build/cmake-3.12.0 \
        && ./configure \
        && make -j32 \
        && make install

# Binutils
RUN cd /build \
    && curl https://ftp.gnu.org/gnu/binutils/binutils-2.29.1.tar.bz2 | tar xj \
    && ./configure \
    && make -j $NUM_CPU && make install

# GMP
RUN curl https://gmplib.org/download/gmp/gmp-6.1.2.tar.bz2 | tar xj \
    && mkdir build && cd build && ../gmp-6.1.2/configure \
    && make -j $NUM_CPU install

# mpfr
RUN curl https://ftp.gnu.org/gnu/mpfr/mpfr-3.1.6.tar.bz2 | tar xj \
    && mkdir build && cd build && ../mpfr-3.1.6/configure \
    && make -j $NUM_CPU install

# mpc
RUN curl http://ftp.gnu.org/gnu/mpc/mpc-1.0.3.tar.gz | tar xz \
    && mkdir build && cd build && ../mpc-1.0.3/configure \
    && make -j $NUM_CPU install

# gcc
RUN mkdir -p /build/gcc/objdir \
        && mkdir -p /opt/gcc \
        && cd /build/gcc \
        && curl https://ftp.gnu.org/gnu/gcc/gcc-5.5.0/gcc-5.5.0.tar.gz | tar -xzv \
        && cd gcc-5.5.0 \
        && ./contrib/download_prerequisites \
        && cd ../objdir \
        && ../gcc-5.5.0/configure  --enable-languages=c,c++ --disable-multilib  \
        && cd /build/gcc/objdir \
        && make -j32 \
        && make install
