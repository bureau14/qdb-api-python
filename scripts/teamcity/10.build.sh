#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

source ${SCRIPT_DIR}/00.common.sh

git config --global --add safe.directory '*'

# No more errors should occur after here
set -e -u -x

PYTHON="${PYTHON_CMD:-python3}"

# Now use a virtualenv to run the tests. If the virtualenv already exists, we remove
# it to ensure a clean install.
${PYTHON} -m venv --clear ${SCRIPT_DIR}/../../.env/
if [[ "$(uname)" == MINGW* ]]
then
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/Scripts/python.exe"
else
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/bin/python"
fi

function relabel_wheel {
    wheel="$1"

    if ! ${VENV_PYTHON} -m auditwheel show "$wheel"
    then
        echo "Skipping non-platform specific wheel $wheel"
    else
        # ${AUDITWHEEL_PLAT} is defined in manylinux base docker image
        ${VENV_PYTHON} -m auditwheel repair "$wheel" --plat "$AUDITWHEEL_PLAT" -w dist/
    fi
}

DIST_DIR=dist

rm -r -f build/ ${DIST_DIR}/

if [[ "$OSTYPE" == "darwin"* && $PYTHON == "python3.9"* ]]; then
    ${VENV_PYTHON} -m pip install --upgrade setuptools==63.0.0b1 wheel
else
    ${VENV_PYTHON} -m pip install --upgrade setuptools wheel auditwheel
fi

${VENV_PYTHON} -m pip install -r dev-requirements.txt

export DISTUTILS_DEBUG=1
export QDB_TESTS_ENABLED=OFF

${VENV_PYTHON} -m build -w

for whl in ${DIST_DIR}/*.whl; do
    relabel_wheel "$whl"
done
