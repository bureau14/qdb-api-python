#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

source ${SCRIPT_DIR}/00.common.sh

git config --global --add safe.directory '*'

# No more errors should occur after here
set -e -u -x

PYTHON="${PYTHON_CMD:-python3}"

# Now use a virtualenv to run the tests. If the virtualenv already exists, we remove
# it to ensure a clean install.
${PYTHON} -m venv --clear ${SCRIPT_DIR}/../../.env/
if [[ "$(uname)" == MINGW* ]]
then
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/Scripts/python.exe"
else
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/bin/python"
fi

${VENV_PYTHON} --version

function relabel_wheel {
    wheel="$1"

    if ! ${VENV_PYTHON} -m auditwheel show "$wheel"
    then
        echo "Skipping non-platform specific wheel $wheel"
    else
        # ${AUDITWHEEL_PLAT} is defined in manylinux base docker image
        ${VENV_PYTHON} -m auditwheel repair "$wheel" --plat "$AUDITWHEEL_PLAT" -w dist/
        rm "$wheel"
    fi
}

# TEMPORARY(QDB-20060): remove once quasardb-build runs on RHEL8 (glibc 2.28).
# A wheel linked against an archive compiled on glibc 2.28 cannot go below
# manylinux_2_28; raise the floor below to 28 at that point.
#
# auditwheel names the repaired wheel after the lowest manylinux policy its
# glibc symbol versions allow, next to the policy requested with --plat. Linux
# wheels must keep installing on glibc 2.26 (Amazon Linux 2); a higher floor
# means a symbol bound to a newer glibc version, see
# quasardb/detail/glibc_compat.cpp.
QDB_MANYLINUX_MAX_FLOOR="${QDB_MANYLINUX_MAX_FLOOR:-26}"

function check_manylinux_floor {
    wheel="$1"

    floor=$(basename "$wheel" | grep -o 'manylinux_2_[0-9]*' | sed 's/manylinux_2_//' | sort -n | head -1 || true)
    if [[ -n "$floor" ]] && (( floor > QDB_MANYLINUX_MAX_FLOOR ))
    then
        echo "ERROR: $wheel needs glibc 2.$floor, at most 2.$QDB_MANYLINUX_MAX_FLOOR is allowed" >&2
        exit 1
    fi
}

rm -r -f build/ dist/

if [[ "$OSTYPE" == "darwin"* && $PYTHON == "python3.9"* ]]; then
    ${VENV_PYTHON} -m pip install --upgrade setuptools==63.0.0b1 wheel
else
    ${VENV_PYTHON} -m pip install --upgrade setuptools wheel auditwheel
fi

${VENV_PYTHON} -m pip install -r dev-requirements.txt

export DISTUTILS_DEBUG=1
export QDB_TESTS_ENABLED=OFF

${VENV_PYTHON} -m build -w

for whl in dist/*.whl; do
    relabel_wheel "$whl"
done

for whl in dist/*.whl; do
    check_manylinux_floor "$whl"
done
