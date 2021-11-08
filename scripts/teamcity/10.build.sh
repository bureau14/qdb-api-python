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

rm -r -f dist/

${PYTHON} setup.py sdist -d dist/
${PYTHON} setup.py bdist_egg -d dist/
${PYTHON} setup.py bdist_wheel -d dist/

for whl in dist/*.whl; do
    relabel_wheel "$whl"
done
