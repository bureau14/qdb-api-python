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
CURRENT_YEAR=`date +%Y`

if [[ "${INPUT_VERSION}" == *-* ]] ; then
    TAGS_VERSION=${INPUT_VERSION#*-}
    TAGS_VERSION=${TAGS_VERSION/nightly/dev}
    TAGS_VERSION=${TAGS_VERSION/./}
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

# # Copyright (c) 2009-2019, quasardb SAS
sed -i -e 's/^\(# Copyright (c) [0-9]\+-\)[0-9]\+\(, quasardb SAS\)$/\1'"${CURRENT_YEAR}"'\2/' examples/*.py packaging/*.py

# Copyright (c) 2009-2019, quasardb SAS All rights reserved.
sed -i -e 's/^\(Copyright (c) [0-9]\+-\)[0-9]\+\(, quasardb SAS\)/\1'"${CURRENT_YEAR}"'\2/' LICENSE.md

#  * Copyright (c) 2009-2019, quasardb SAS
sed -i -e 's/^\( \* Copyright (c) [0-9]\+-\)[0-9]\+\(, quasardb SAS\)$/\1'"${CURRENT_YEAR}"'\2/' quasardb_module/*.{cpp,hpp}
