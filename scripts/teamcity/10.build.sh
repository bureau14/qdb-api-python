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

PLATFORM=''
if [[ "$OSTYPE" == "darwin"* ]] ; then
    PLATFORM='-p macosx-10.14-x86_64'
fi

rm -r -f build/ ${DIST_DIR}/

if [[ "$OSTYPE" == "darwin"* && $PYTHON == "python3.9"* ]]; then
    ${PYTHON} -m pip install --user --upgrade setuptools==63.0.0b1 wheel
else
    ${PYTHON} -m pip install --user --upgrade setuptools wheel
fi
${PYTHON} -m pip install --user -r dev-requirements.txt

export DISTUTILS_DEBUG=1

${PYTHON} setup.py sdist -v -d ${DIST_DIR}/
${PYTHON} setup.py bdist_egg -v -d ${DIST_DIR}/ ${PLATFORM}
${PYTHON} setup.py bdist_wheel -v -d ${DIST_DIR}/ ${PLATFORM}

for whl in ${DIST_DIR}/*.whl; do
    relabel_wheel "$whl"
done
