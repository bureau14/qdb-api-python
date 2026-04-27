ARG POLICY="manylinux_2_28"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"
ARG TAG="latest"
FROM quay.io/pypa/$PLATFORM:${TAG}

ARG PYTHON_VERSION

# Install packages required for running start-services inside the container
RUN yum install lsof -y

# We need to ensure that the builder user has the same UID and GID as the host buildkite agent to use the propagate-uid-gid feature of the docker plugin.
RUN groupadd -g 929 builder && useradd -m -s /bin/bash -u 929 -g 929 builder

USER builder

ADD set-python-version.sh /home/builder/set-python-version.sh
RUN bash /home/builder/set-python-version.sh 3.14 \
    && rm /home/builder/set-python-version.sh
