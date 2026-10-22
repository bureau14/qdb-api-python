ARG POLICY="manylinux_2_28"
ARG ARCH="x86_64"
ARG PLATFORM="${POLICY}_${ARCH}"
ARG TAG="latest"
# The image quasardb-build compiles in; multi-arch, so --from resolves to $ARCH.
ARG BUILDER_IMAGE="bureau14/builder:rhel7"

FROM ${BUILDER_IMAGE} AS toolchain

FROM quay.io/pypa/$PLATFORM:${TAG}

ARG PYTHON_VERSION

# On Linux the extension links the static libqdb_api.a, which quasardb-build
# compiles with gcc16. Its objects reference libstdc++ symbols that only exist
# from GLIBCXX_3.4.35 on, so the extension must be linked with gcc16's own
# libstdc++ (-static-libstdc++) rather than the gcc-toolset this image ships.
# The manylinux policy only constrains what the wheel depends on at runtime,
# and a statically linked libstdc++ is not a dependency, so the wheel stays
# manylinux_2_28 compliant. The toolchain was built against glibc 2.26 and
# runs on this image's glibc 2.28 unchanged.
COPY --from=toolchain /usr/local/gcc16 /usr/local/gcc16

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
