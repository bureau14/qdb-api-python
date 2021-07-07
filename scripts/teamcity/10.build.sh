#!/bin/bash

PYTHON="${PYTHON_CMD:-python}"

rm -r -f dist/

${PYTHON} setup.py sdist -d dist/
${PYTHON} setup.py bdist_egg -d dist/
${PYTHON} setup.py bdist_wheel -d dist/
