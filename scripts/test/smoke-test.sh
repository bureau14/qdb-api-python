#!/bin/bash

set -eux -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FILE=${1}

rm -f ./Pipfile ./Pipfile.lock

${PYTHON_EXECUTABLE} -m pipenv clean
${PYTHON_EXECUTABLE} -m pipenv install ${FILE}
${PYTHON_EXECUTABLE} -m pipenv run ${PYTHON_EXECUTABLE} ${DIR}/smoke-test.py
${PYTHON_EXECUTABLE} -m pipenv uninstall quasardb

rm -f ./Pipfile ./Pipfile.lock
