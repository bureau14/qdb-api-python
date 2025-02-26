#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

source ${SCRIPT_DIR}/00.common.sh

set -u -x

PYTHON="${PYTHON_CMD:-python3}"

${PYTHON} -m venv --clear ${SCRIPT_DIR}/../../.env/
if [[ "$(uname)" == MINGW* ]]
then
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/Scripts/python.exe"
else
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/bin/python"
fi

${VENV_PYTHON} -m pip install -r dev-requirements.txt
${VENV_PYTHON} -m pip install --no-deps --force-reinstall dist/quasardb-*manylinux*.whl
${VENV_PYTHON} -m pip install --upgrade pydoc3


# To avoid conflicts with `quasardb` directory and `import quasardb`
rm -rf doc/build || true

mkdir doc/build
pushd doc

${VENV_PYTHON} docgen.py

popd

tar -czvf dist/doc.tar.gz -C doc/build .
