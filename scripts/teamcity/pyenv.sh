#!/bin/bash

set -e -u -x

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

${PYTHON} -m pip install --upgrade pip

if [[ "$OSTYPE" == "darwin"* && $PYTHON == "python3.9"* ]]; then
    ${PYTHON} -m pip install --upgrade setuptools==63.0.0b1 wheel
else
    ${PYTHON} -m pip install --upgrade setuptools wheel
fi

${PYTHON} -m pip install -r dev-requirements.txt
${PYTHON} -m pip install -r dev-requirements.txt