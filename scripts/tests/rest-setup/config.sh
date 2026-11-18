#!/usr/bin/env bash

# The cluster setup enables tracing, so none of its files are sourced here.
set +x
SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
REST_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
: "${LOG_DIR_REST:=${REST_ROOT}/.rest/log}"
: "${REST_LOG_ARCHIVE_PATH:=${SCRIPT_DIR}/logs}"
: "${CONFIG_REST:=${SCRIPT_DIR}/default.rest.cfg}"
: "${CONSOLE_LOG_REST_INSECURE:=${LOG_DIR_REST}/rest_log_insecure.out.txt}"
: "${CONSOLE_ERR_LOG_REST_INSECURE:=${LOG_DIR_REST}/rest_log_insecure.err.txt}"
: "${CONSOLE_LOG_REST_SECURE:=${LOG_DIR_REST}/rest_log_secure.out.txt}"
: "${CONSOLE_ERR_LOG_REST_SECURE:=${LOG_DIR_REST}/rest_log_secure.err.txt}"
: "${URI_INSECURE:=127.0.0.1:2836}"
: "${URI_SECURE:=127.0.0.1:2838}"
: "${USER_PRIVATE_KEY:=${REST_ROOT}/user_private.key}"
: "${CLUSTER_PUBLIC_KEY:=${REST_ROOT}/cluster_public.key}"
: "${REST_PORT_INSECURE:=40080}"
: "${REST_PORT_SECURE:=40081}"
: "${REST_HTTPS_PORT_INSECURE:=40443}"
: "${REST_HTTPS_PORT_SECURE:=40444}"
: "${REST_FLIGHT_PORT_INSECURE:=40493}"
: "${REST_FLIGHT_PORT_SECURE:=40494}"
: "${REST_START_TIMEOUT:=60}"
: "${REST_REQUEST_TIMEOUT:=3}"

if [ ! -d "$REST_LOG_ARCHIVE_PATH" ]
then
    echo "Log archive path ${REST_LOG_ARCHIVE_PATH} does not yet exist, creating..."
    mkdir -v -p "$REST_LOG_ARCHIVE_PATH"
fi

