#!/bin/bash

source pyenv.sh

${PYTHON} -m pip install dist/quasardb*.whl
mkdir doc || true
${PYTHON} docgen.py
tar -czvf dist/doc.tar.gz doc/*
