#!/bin/bash

set -e -u -x

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

PYTHON="${PYTHON_CMD:-python3}"
DIST_DIR=dist
PLATFORM="-p macosx-11.0-x86_64"

rm -r -f build/ ${DIST_DIR}/

${PYTHON} -m pip install --user --upgrade setuptools wheel
${PYTHON} -m pip install --user -r dev-requirements.txt

${PYTHON} setup.py sdist -d ${DIST_DIR}/
${PYTHON} setup.py bdist_egg -d ${DIST_DIR}/ ${PLATFORM}
${PYTHON} setup.py bdist_wheel -d ${DIST_DIR}/ ${PLATFORM}

for whl in ${DIST_DIR}/*.whl; do
    relabel_wheel "$whl"
done
