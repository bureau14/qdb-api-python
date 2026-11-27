#!/usr/bin/env bash

set +x

: "${REST_BINARY:=${QDB_DIR:-${REST_ROOT}/qdb/bin}/qdb_rest}"

case "$(uname)" in
    MINGW* | MSYS*) REST_BINARY="${REST_BINARY%.exe}.exe" ;;
esac

REST_FILENAME=${REST_BINARY##*/}

function check_binaries {
    if [[ ! -x "$REST_BINARY" ]]
    then
        echo "Binary ${REST_BINARY} not found." >&2
        return 1
    fi
    # Startup needs curl and jq to authenticate and validate REST query responses.
    command -v curl >/dev/null
    command -v jq >/dev/null
}
