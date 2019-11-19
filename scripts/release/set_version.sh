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

SUB_RELEASE_VERSION=${INPUT_VERSION#*-}
SUB_RELEASE_TYPE=${SUB_RELEASE_VERSION%%.*}
SUB_RELEASE_MINOR_VERSION=${SUB_RELEASE_VERSION#*.}
if [[ "${SUB_RELEASE_TYPE}" == "rc" ]] ; then
    SUB_RELEASE_TYPE="rc"
    FULL_XYZ_VERSION="${XYZ_VERSION}.${SUB_RELEASE_TYPE}${SUB_RELEASE_MINOR_VERSION}"
elif [[ "${SUB_RELEASE_TYPE}" == "nightly" ]] ; then
    SUB_RELEASE_TYPE="dev"
    FULL_XYZ_VERSION="${XYZ_VERSION}.${SUB_RELEASE_TYPE}0"
else
    FULL_XYZ_VERSION="${XYZ_VERSION}"
fi

CURRENT_YEAR=`date +%Y`

cd $(dirname -- $0)
cd ${PWD}/../..

# qdb_version = "3.2.0.dev3".lower()
sed -i -e 's/qdb_version *= *"[^"]*"/qdb_version = "'${FULL_XYZ_VERSION}'"/' setup.py

# # Copyright (c) 2009-2019, quasardb SAS
sed -i -e 's/^\(# Copyright (c) [0-9]\+-\)[0-9]\+\(, quasardb SAS\)$/\1'${CURRENT_YEAR}'\2/' examples/*.py

# Copyright (c) 2009-2019, quasardb SAS All rights reserved.
sed -i -e 's/^\(Copyright (c) [0-9]\+-\)[0-9]\+\(, quasardb SAS\)/\1'${CURRENT_YEAR}'\2/' LICENSE.md

#  * Copyright (c) 2009-2019, quasardb SAS
sed -i -e 's/^\( \* Copyright (c) [0-9]\+-\)[0-9]\+\(, quasardb SAS\)$/\1'${CURRENT_YEAR}'\2/' quasardb/*.{cpp,hpp}
