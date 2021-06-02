#!/bin/bash

# Using cento7 docker we need to enable gcc-8 and g++-8
case "$(uname)" in
    Linux*)
        source /opt/rh/devtoolset-8/enable
    ;;
    *)
    ;;
esac

PYTHON="${PYTHON_CMD:-python}"

rm -r -fo dist/
${PYTHON} setup.py bdist_egg -d dist/
${PYTHON} setup.py bdist_wheel -d dist/
