#!/usr/bin/env bash

set +x
source "$SCRIPT_DIR/binaries.sh"

function rest_native_path {
    case "$(uname)" in
        MINGW* | MSYS*) cygpath -am "$1" ;;
        *) printf '%s\n' "$1" ;;
    esac
}

function rest_cluster_uri {
    case "$1" in
        qdb://*) printf '%s\n' "$1" ;;
        *) printf 'qdb://%s\n' "$1" ;;
    esac
}

function qdb_rest_start_instance {
    local name=$1 uri http_port https_port flight_port public_key='' user_key=''
    local output err_output
    if [[ "$name" == insecure ]]
    then
        uri=$(rest_cluster_uri "$URI_INSECURE")
        http_port=$REST_PORT_INSECURE
        https_port=$REST_HTTPS_PORT_INSECURE
        flight_port=$REST_FLIGHT_PORT_INSECURE
        output=$CONSOLE_LOG_REST_INSECURE
        err_output=$CONSOLE_ERR_LOG_REST_INSECURE
    else
        uri=$(rest_cluster_uri "$URI_SECURE")
        http_port=$REST_PORT_SECURE
        https_port=$REST_HTTPS_PORT_SECURE
        flight_port=$REST_FLIGHT_PORT_SECURE
        public_key=$(rest_native_path "$CLUSTER_PUBLIC_KEY")
        user_key=$(rest_native_path "$USER_PRIVATE_KEY")
        output=$CONSOLE_LOG_REST_SECURE
        err_output=$CONSOLE_ERR_LOG_REST_SECURE
    fi
    # QDB_ENCRYPT_TRAFFIC is a qdbd setting. REST's aes selects a deprecated client cipher.
    "$REST_BINARY" --config "$(rest_native_path "$CONFIG_REST")" \
        --listen-http "127.0.0.1:$http_port" \
        --listen-https "127.0.0.1:$https_port" \
        --listen-flight "127.0.0.1:$flight_port" \
        --cluster-uri "$uri" \
        --cluster-public-key-file "$public_key" \
        --cluster-user-security-file "$user_key" \
        > "$output" 2> "$err_output" &
}

function count_instances {
    case "$(uname)" in
        MINGW* | MSYS*)
            Tasklist //FI "IMAGENAME eq $REST_FILENAME" //NH | grep -i -F "$REST_FILENAME" | wc -l || true
            ;;
        *) pgrep -x "$REST_FILENAME" | wc -l || true ;;
    esac
}

function kill_instances {
    echo "Killing ${REST_FILENAME} instances..."
    case "$(uname)" in
        MINGW* | MSYS*) Taskkill //IM "$REST_FILENAME" //F || true ;;
        *) pkill -SIGKILL -x "$REST_FILENAME" || true ;;
    esac
    sleep .1
    if (( $(count_instances) != 0 ))
    then
        echo "Could not kill all ${REST_FILENAME} instances, aborting..." >&2
        return 1
    fi
}

function rest_query {
    # This probe proves a database round trip by:
    # 1. Authenticating as the cluster's caller.
    # 2. Sending a native query with the returned access token.
    # 3. Validating the complete result, including its value.
    local name=$1 port=$2 credentials=$3
    local endpoint="http://127.0.0.1:${port}/api/v2"
    local status
    # Credentials and tokens travel through private files, never command arguments.
    if [[ "$credentials" == anonymous ]]
    then
        printf '{}\n' > "$SMOKE_DIR/login.json"
    else
        jq -e '{username, secret_key} | select((.username | type == "string" and length > 0) and (.secret_key | type == "string" and length > 0))' \
            "$credentials" > "$SMOKE_DIR/login.json" || return 1
    fi
    status=$(curl --noproxy '*' --silent --show-error --connect-timeout 1 \
        --max-time "$REST_REQUEST_TIMEOUT" --output "$SMOKE_DIR/login-response.json" \
        --write-out '%{http_code}' -H 'Content-Type: application/json' \
        --data-binary "@$(rest_native_path "$SMOKE_DIR/login.json")" "$endpoint/auth/login") || return 1
    if [[ "$status" != 200 ]]
    then
        echo "REST $name login failed: HTTP $status" >&2
        jq -r '.detail // empty' "$SMOKE_DIR/login-response.json" >&2
        return 1
    fi
    jq -er '.access_token | select(type == "string" and length > 0 and (test("[\\r\\n]") | not)) | "Authorization: Bearer " + .' \
        "$SMOKE_DIR/login-response.json" > "$SMOKE_DIR/headers" || return 1
    # SELECT 1 uses the native /query route and creates no persistent database data.
    status=$(curl --noproxy '*' --silent --show-error --connect-timeout 1 \
        --max-time "$REST_REQUEST_TIMEOUT" --output "$SMOKE_DIR/query.json" \
        --write-out '%{http_code}' -H "@$(rest_native_path "$SMOKE_DIR/headers")" \
        -H 'Content-Type: text/plain' -H 'Accept: application/json' \
        --data-binary 'SELECT 1' "$endpoint/query") || return 1
    if [[ "$status" != 200 ]] || ! jq -e \
        'type == "array" and length == 1 and (.[0].columns | length == 1) and .[0].columns[0].data == [1]' \
        "$SMOKE_DIR/query.json" >/dev/null
    then
        echo "REST $name query failed: HTTP $status or unexpected SELECT 1 result" >&2
        return 1
    fi
    echo "REST $name query OK: $endpoint/query"
}

function check_address {
    (
        umask 077
        local SMOKE_DIR
        SMOKE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/qdb-rest-smoke.XXXXXX") || exit 1
        trap 'rm -rf "$SMOKE_DIR"' EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM

        case "$1" in
            insecure) rest_query insecure "$REST_PORT_INSECURE" anonymous ;;
            secure) rest_query secure "$REST_PORT_SECURE" "$USER_PRIVATE_KEY" ;;
        esac
    )
}
