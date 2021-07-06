#!/bin/bash

PYTHON="${PYTHON_CMD:-python}"

rm -r -f dist/

case "$(uname)" in
    Linux*)
        ${PYTHON} setup.py sdist -d dist/
    ;;
    *)
    ;;
esac

${PYTHON} setup.py bdist_egg -d dist/
${PYTHON} setup.py bdist_wheel -d dist/
