#!/bin/bash

set -u -x
PYTHON="${PYTHON_CMD:-python3}"

# remove previous environment
if [ -d .env ]; then
    case "$(uname)" in
        MINGW*)
            source .env/Scripts/activate
        ;;
        *)
            source .env/bin/activate
        ;;
    esac
    ${PYTHON} -m pip freeze > to_remove.txt

    if [ -s to_remove.txt ]; then
        ${PYTHON} -m pip uninstall -r to_remove.txt -y
    fi

    deactivate
    rm -Rf .env
fi

${PYTHON} -m venv .env/
case "$(uname)" in
    MINGW*)
        source .env/Scripts/activate
    ;;
    *)
        source .env/bin/activate
    ;;
esac

# first remove system then user
${PYTHON} -m pip uninstall -r dev-requirements.txt -y
${PYTHON} -m pip install --upgrade pip
${PYTHON} -m pip install --upgrade wheel
${PYTHON} -m pip install --upgrade "setuptools<=58.4"
${PYTHON} -m pip install -r dev-requirements.txt

TEST_OPTS="-s"
if [[ ! -z ${JUNIT_XML_FILE-} ]]
then
    TEST_OPTS+=" --junitxml=${JUNIT_XML_FILE}"
fi

echo "Invoking pytest with --addopts '${TEST_OPTS}'"
${PYTHON} setup.py test  --addopts "${TEST_OPTS}"
