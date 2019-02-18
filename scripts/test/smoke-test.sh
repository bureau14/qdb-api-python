#!/bin/bash

set -eux -o pipefail

function detect_pipenv() {
    if [[ ! "${PYTHON_EXECUTABLE-}" == "" ]]
    then
        echo "${PYTHON_EXECUTABLE} -m pipenv"
    else
        echo $(which pipenv)
    fi
}

PIPENV=$(detect_pipenv)

FILE=${1}

rm -f ./Pipfile ./Pipfile.lock

${PIPENV} clean
${PIPENV} install ${FILE}
${PIPENV} run python smoke-test.py
${PIPENV} uninstall quasardb

rm -f ./Pipfile ./Pipfile.lock
