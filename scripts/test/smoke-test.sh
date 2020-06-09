#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FILE=${1}
PYTHON="${PYTHON_EXECUTABLE:-python}"

rm -rf .env/ || true

${PYTHON} -m venv .env/
source .env/bin/activate
${PYTHON} -m pip install ${FILE}
${PYTHON} ${DIR}/smoke-test.py

rm -rf .env/ || true
