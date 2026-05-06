ARG POLICY="manylinux_2_28"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"
ARG TAG="latest"
FROM quay.io/pypa/$PLATFORM:${TAG}

ARG PYTHON_VERSION

# We need to ensure that the container user has the same UID and GID as the host buildkite agent to use the `propagate-uid-gid` feature of the docker plugin.
# The default UID, GID matches the default UID, GID of the buildkite agent on the host, if needed can be overridden
ARG USER_ID=929
ARG GROUP_ID=929
ARG USERNAME=builder
ARG HOME=/home/${USERNAME}
ARG COMMENT=builder

# Install packages required for running start-services inside the container
RUN yum install lsof procps -y

RUN groupadd --gid $GROUP_ID $USERNAME
RUN useradd --comment "$COMMENT" --home-dir $HOME --create-home --system --uid $USER_ID --gid $GROUP_ID $USERNAME

USER $USERNAME

ADD --chown=$USERNAME:$USERNAME set-python-version.sh ${HOME}/set-python-version.sh
RUN bash ${HOME}/set-python-version.sh ${PYTHON_VERSION} \
    && rm ${HOME}/set-python-version.sh
