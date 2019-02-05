#!/bin/bash

set -e -x

rm -rf /io/build

# Compile wheels
for PYBIN in /opt/python/cp3*/bin; do

    ${PYBIN}/pip install -r /io/requirements.txt
    mkdir /io/build
    cd /io/build
    PYTHON_EXECUTABLE=${PYBIN}/python cmake -DPYBIND11_PYTHON_VERSION=2.7 -DCMAKE_BUILD_TYPE=Release ..
    make

done
