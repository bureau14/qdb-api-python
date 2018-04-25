#!/bin/sh
set -eu -o pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]] ; then
    >&2 echo "Usage: $0 <new_version>"
    exit 1
fi

INPUT_VERSION=$1; shift

MAJOR_VERSION=${INPUT_VERSION%%.*}
WITHOUT_MAJOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.}
MINOR_VERSION=${WITHOUT_MAJOR_VERSION%%.*}
WITHOUT_MINOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.${MINOR_VERSION}.}
PATCH_VERSION=${WITHOUT_MINOR_VERSION%%.*}

XYZ_VERSION="${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"

if [[ "${INPUT_VERSION}" == *-* ]] ; then
    TAGS_VERSION=${INPUT_VERSION#*-}
else
    TAGS_VERSION=
fi

if [[ -n "${TAGS_VERSION}" ]] ; then
    FULL_XYZ_VERSION="${XYZ_VERSION}.${TAGS_VERSION}"
else
    FULL_XYZ_VERSION="${XYZ_VERSION}"
fi

cd $(dirname -- $0)
cd ${PWD}/../..

# set(QDB_PY_VERSION "2.1.0b3")
sed -i -e 's/set(QDB_PY_VERSION *"[^"]*")/set(QDB_PY_VERSION "'"${FULL_XYZ_VERSION}"'")/' CMakeLists.txt
