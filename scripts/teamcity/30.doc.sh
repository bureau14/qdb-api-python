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
${VENV_PYTHON} -m pip install --no-deps --force-reinstall dist/quasardb-*.whl


# To avoid conflicts with `quasardb` directory and `import quasardb`
rm -rf tmp || true
rm -rf doc || true

mkdir tmp
pushd tmp

${VENV_PYTHON} ../docgen.py

popd

mv -v tmp/doc doc

tar -czvf dist/doc.tar.gz doc/*
