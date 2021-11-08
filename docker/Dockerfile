ARG POLICY="manylinux2014"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"

FROM quay.io/pypa/$PLATFORM

ARG PYTHON_VERSION

ADD set-python-version.sh /set-python-version.sh
RUN bash /set-python-version.sh ${PYTHON_VERSION} \
    && rm /set-python-version.sh

RUN python3 --version && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade setuptools wheel
