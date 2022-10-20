#!/bin/bash

set -e -u -x

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source ${SCRIPT_DIR}/pyenv.sh

function relabel_wheel {
    wheel="$1"

    if ! auditwheel show "$wheel"
    then
        echo "Skipping non-platform wheel $wheel"
    else
        # ${AUDITWHEEL_PLAT} is defined in manylinux base docker image
        auditwheel repair "$wheel" --plat "$AUDITWHEEL_PLAT" -w dist/
    fi
}

DIST_DIR=dist


PLATFORM=''
if [[ "$OSTYPE" == "darwin"* ]] ; then
    PLATFORM='-p macosx-10.14-x86_64'
fi

export DISTUTILS_DEBUG=1

rm -r -f build/ ${DIST_DIR}/

${PYTHON} setup.py sdist -v -d ${DIST_DIR}/
${PYTHON} setup.py bdist_egg -v -d ${DIST_DIR}/ ${PLATFORM}
${PYTHON} setup.py bdist_wheel -v -d ${DIST_DIR}/ ${PLATFORM}

for whl in ${DIST_DIR}/*.whl; do
    relabel_wheel "$whl"
done
