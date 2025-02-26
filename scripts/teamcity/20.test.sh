#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

source ${SCRIPT_DIR}/00.common.sh

set -u -x

PYTHON="${PYTHON_CMD:-python3}"

###
# NOTE(leon):
###
#
# EVIL CODE SECTION BELOW.
#
# Looking at this will make you angry.
# Studying it will make you cry.
# Understanding it will bring enlightenment.
# Using it will enable massive parallelism for builds on Windows by using Ninja.
#
# ~~~
#
# The problem is solves: we want to use Ninja for Windows. For this, we need to set a whole
# bunch of environment variables. Microsoft helpfully provided a batch script called 'VsDevCmd.bat'
# which does exactly this.
#
# But herein lies the problem: we cannot just "source" a windows batch script from mingw/bash.
# We can use cmd to invoke it, but then it exits and the environment variables are immediately lost.
#
# The evil code below solves this by effectively invoking the VsDevCmd.bat, and then storing the
# entire environment in a file. Then we read this environment file, and export all the env keys.
#
# `evil_inner` is responsible for invoking the VsDevCmd and printing out all the environment
# variables to the console.
#
# `evil_outer` is responsible for capturing environment variables before, after, and setting all
# the changed variables.

function evil_inner {
    local arch=$1; shift;

    # Because VsDevCmd.bat prints some garbage data, we want to skip the first X rows. Basically
    # what I did was audit manually the type of regex that actually matches all environment data.
    # Then, because we know 100% sure after the first row is matched, the rest is also going to
    # be from printenv, we just tell grep "print the 5000 lines after your first match". This
    # avoids a scenario that if for some stupid reason a new env var is introduced which doesn't
    # match the regex, it still works.

    (cd /c/Program\ Files\ \(x86\)/Microsoft\ Visual\ Studio/2022/BuildTools/Common7/Tools/ \
         && cmd //C "VsDevCmd.bat -host_arch=amd64 -arch=$arch && bash -c printenv" \
             | grep -A5000 -m1 -E '^([a-zA-Z0-9_\(\):]+)=' )
}

function evil_outer {
    local arch=$1; shift;

    before=$(mktemp)
    after=$(mktemp)
    added=$(mktemp)

    printenv | sort > $before
    evil_inner $arch | sort > $after

    # Keep only those lines that are added/different in the $after file, and save them in a file.
    # Yes, I too did not know what `comm` was until I needed it, and it basically compares two sorted
    # files line-by-line, which is exactly what we need here.

    comm -13 $before $after > $added

    while read -r LINE; do

        local key=$(cut -d '=' -f 1 <<< "$LINE" )
        local value=$(cut -d '=' -f 2- <<< "$LINE" )

        ###
        # XXX(leon):
        ###
        #
        # It seems MSVC sets a few very weird environment variables with keys:
        # - !C:=C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\Tools
        # - !ExitCode=00000000
        #
        # I have no idea how to properly export these, but it doesn't appear to matter a lot. I *suspect*
        # that they're really some internal environment variables that represent the current working dir
        # and/or exit code of a program, and are not intended to be set.
        #
        # However, this will print out warnings. These can be safely ignored.

        export ${key}="${value}"
    done < $added
}

###
#
# EVIL CODE SECTION ABOVE. NO MORE EVIL CODE BELOW THIS LINE.
#
###

if [[ "$(uname)" == MINGW* ]]
then
    ARCH_BITS=$(${PYTHON} -c 'import struct;print( 8 * struct.calcsize("P"))')
    echo "Windows build detected, target arch with bits: ${ARCH_BITS}"

    if [[ "${ARCH_BITS}" == "32" ]]
    then
        echo "Targeting win32"
        evil_outer x86
    elif [[ "${ARCH_BITS}" == "64" ]]
    then
        echo "Targeting win64"
        evil_outer amd64
    else
        echo "Internal error: 'ARCH_BITS' is unrecognized: ${ARCH_BITS}"
        exit -1
    fi
fi

# No more errors should occur after here
set -e -o pipefail

if [[ -d "dist/" ]]
then
    echo "Removing dist/"
    rm -rf dist/
fi

if [[ -d "build/" ]]
then
    echo "Removing build/"
    rm -rf build/
fi

# Now use a virtualenv to run the tests

${PYTHON} -m venv --clear ${SCRIPT_DIR}/../../.env/
if [[ "$(uname)" == MINGW* ]]
then
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/Scripts/python.exe"
else
    VENV_PYTHON="${SCRIPT_DIR}/../../.env/bin/python"
fi


${VENV_PYTHON} -m pip install --upgrade -r dev-requirements.txt

export QDB_TESTS_ENABLED=ON
${VENV_PYTHON} -m build -w

${VENV_PYTHON} -m pip install --no-deps --force-reinstall dist/quasardb-*.whl

echo "Invoking pytest"

TEST_OPTS="--teamcity"
if [[ ! -z ${JUNIT_XML_FILE-} ]]
then
    TEST_OPTS+=" --junitxml=${JUNIT_XML_FILE}"
fi

pushd tests
exec ${VENV_PYTHON} -m pytest ${TEST_OPTS}
popd
