#!/bin/bash

set -u -x

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source ${SCRIPT_DIR}/pyenv.sh

TEST_OPTS="-s"
if [[ ! -z ${JUNIT_XML_FILE-} ]]
then
    TEST_OPTS+=" --junitxml=${JUNIT_XML_FILE}"
fi

echo "Invoking pytest with --addopts '${TEST_OPTS}'"
${PYTHON} setup.py test  --addopts "${TEST_OPTS}"
