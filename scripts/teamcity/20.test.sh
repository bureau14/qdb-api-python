#!/bin/bash

set -u -x

source pyenv.sh

TEST_OPTS="-s"
if [[ ! -z ${JUNIT_XML_FILE-} ]]
then
    TEST_OPTS+=" --junitxml=${JUNIT_XML_FILE}"
fi

echo "Invoking pytest with --addopts '${TEST_OPTS}'"
${PYTHON} setup.py test  --addopts "${TEST_OPTS}"
