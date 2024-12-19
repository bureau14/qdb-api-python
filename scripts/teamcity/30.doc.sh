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


${VENV_PYTHON} -m pip install --no-deps --force-reinstall dist/quasardb-*.whl
${VENV_PYTHON} -m pip install -r dev-requirements.txt

rm -rf doc || true
mkdir doc || true
${VENV_PYTHON} docgen.py
tar -czvf dist/doc.tar.gz doc/*
