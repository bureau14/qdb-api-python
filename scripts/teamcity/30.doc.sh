#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

source ${SCRIPT_DIR}/00.common.sh

python3 -m pip install dist/quasardb*.whl
python3 -m pip install -r dev-requirements.txt
mkdir doc || true
python3 docgen.py
tar -czvf dist/doc.tar.gz doc/*
