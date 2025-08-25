ARG POLICY="manylinux_2_28"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"
ARG TAG="latest"
FROM quay.io/pypa/$PLATFORM:${TAG}

ARG PYTHON_VERSION

ADD set-python-version.sh /set-python-version.sh
RUN bash /set-python-version.sh ${PYTHON_VERSION} \
    && rm /set-python-version.sh
