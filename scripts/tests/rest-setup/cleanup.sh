#!/usr/bin/env bash

set +x

function archive_log_dir {
    local archive_name="$1"; shift
    local log_dir="$1"; shift
    if [ -d "$log_dir" ]
    then
        local tar=$(command -v tar)
        local epoch=$(date +%s)
        local archive_file="${REST_LOG_ARCHIVE_PATH}/${archive_name}-logs-${epoch}.tar.gz"

        echo "Archiving log dir: $log_dir"
        umask 077
        "$tar" -czvf "$archive_file" -C "$log_dir" .
    fi
}

function archive {
    archive_log_dir rest "$LOG_DIR_REST"
}

function cleanup {
    archive
}

function full_cleanup {
    cleanup
    rm -rf "$LOG_DIR_REST"
}
