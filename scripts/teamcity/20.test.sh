#!/bin/bash

PYTHON="${PYTHON_CMD:-python}"

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
    ${PYTHON} -m pip uninstall -r to_remove.txt -y
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
${PYTHON} -m pip uninstall -r dev-requirements.txt -y
${PYTHON} -m pip install --upgrade pip
${PYTHON} -m pip install -r dev-requirements.txt
${PYTHON} -m pip install --upgrade setuptools wheel
${PYTHON} setup.py test  --addopts "--junitxml=${JUNIT_XML_FILE}"
