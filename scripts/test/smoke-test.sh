#!/bin/bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FILE=${1}
PYTHON="${PYTHON_CMD:-python3}"


rm -rf .env/ || true

${PYTHON} -m venv .env/ && source .env/bin/activate

# Necessary for some bug in platform detection with certain python version + OSX
${PYTHON} -m pip install -U setuptools wheel pip
${PYTHON} -m pip install --force-reinstall ${FILE}

set -eu

echo ""
echo "Running smoketest.."
echo "-------------------"
echo ""
${PYTHON} ${DIR}/smoke-test.py
echo "-------------------"
echo "Smoketest finished!"
echo ""

set +eu

deactivate || true
rm -rf .env/ || true
