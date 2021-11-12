#!/bin/bash
set -eux -o pipefail

PYTHON_VERSION=${1}

function parse_version {
    local version=${1}

    if [[ ${version}  == 3.10* ]]
    then
        echo "cp310-cp310"
    elif [[ ${version}  == 3.9* ]]
    then
        echo "cp39-cp39"
    elif [[ ${version}  == 3.8* ]]
    then
        echo "cp38-cp38"
    elif [[ ${version}  == 3.7* ]]
    then
        echo "cp37-cp37m"
    elif [[ ${version}  == 3.6* ]]
    then
        echo "cp36-cp36m"
    else
        exit 1
    fi
}

PARSED=$(parse_version ${PYTHON_VERSION})

PYTHON_BIN="/opt/python/${PARSED}/bin/python"
ln -s ${PYTHON_BIN} /usr/bin/python3
