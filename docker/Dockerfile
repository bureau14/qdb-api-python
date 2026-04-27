ARG POLICY="manylinux_2_28"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"
ARG TAG="latest"
FROM quay.io/pypa/$PLATFORM:${TAG}

ARG PYTHON_VERSION

# We need to ensure that the builder user has the same UID and GID as the host buildkite agent to use the propagate-uid-gid feature of the docker plugin.
RUN groupadd -g 929 builder && useradd -m -s /bin/bash -u 929 -g 929 builder
USER builder

ADD set-python-version.sh /set-python-version.sh
RUN bash /set-python-version.sh ${PYTHON_VERSION} \
    && rm /set-python-version.sh

#Packages required for running start-services inside the container
RUN yum install lsof -y
