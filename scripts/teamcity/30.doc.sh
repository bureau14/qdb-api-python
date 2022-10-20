#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source ${SCRIPT_DIR}/pyenv.sh

${PYTHON} -m pip install dist/quasardb*.whl
mkdir doc || true
${PYTHON} docgen.py
tar -czvf dist/doc.tar.gz doc/*
