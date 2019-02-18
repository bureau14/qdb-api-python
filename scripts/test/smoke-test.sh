#!/bin/bash

set -eux -o pipefail

PIPENV=$(which pipenv)
FILE=${1}


${PIPENV} install ${FILE}
${PIPENV} run python smoke-test.py
${PIPENV} uninstall quasardb

rm -f ./Pipfile ./Pipfile.lock
