#!/usr/bin/env bash

set +x
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/config.sh"
source "$SCRIPT_DIR/utils.sh"
source "$SCRIPT_DIR/cleanup.sh"

kill_instances
full_cleanup
check_binaries
if [[ ! -r "$USER_PRIVATE_KEY" || ! -r "$CLUSTER_PUBLIC_KEY" ]]
then
    echo 'Secure cluster user security file and cluster public key are required.' >&2
    exit 1
fi

umask 077
mkdir -p "$LOG_DIR_REST"
PIDS=()

# The base file and explicit instance flags determine this fixture's configuration.
for variable in ${!QDB_REST_@}; do unset "$variable"; done
for name in insecure secure; do
    qdb_rest_start_instance "$name"
    PIDS+=("$!")
done

# Both backends share one startup budget to bound the total readiness wait.
# 10# keeps leading zeros in the configured timeout from selecting octal.
deadline=$((SECONDS + 10#$REST_START_TIMEOUT))
for name in insecure secure; do
    while true; do
        for pid in "${PIDS[@]}"; do
            if ! kill -0 "$pid" 2>/dev/null
            then
                status=0
                wait "$pid" || status=$?
                echo "REST child $pid exited during startup (status $status); logs: $LOG_DIR_REST" >&2
                exit 1
            fi
        done
        if check_address "$name" > "$LOG_DIR_REST/rest_probe_${name}.out.txt" 2>&1
        then
            break
        fi
        if (( SECONDS >= deadline ))
        then
            echo "REST $name did not execute SELECT 1 within ${REST_START_TIMEOUT}s; logs: $LOG_DIR_REST" >&2
            exit 1
        fi
        sleep 0.2
    done
done

function print_rest_endpoints {
    local backend=$1 http_port=$2 https_port=$3
    local https_endpoint="https://127.0.0.1:$https_port"
    # With port 0, qdb_rest logs the requested HTTPS port rather than the assigned port.
    if [[ "$https_port" == 0 ]]; then
        https_endpoint="HTTPS (server-assigned port)"
    fi
    echo "REST ready: http://127.0.0.1:$http_port -> $backend"
    echo "REST ready: $https_endpoint -> $backend"
}

print_rest_endpoints "$(rest_cluster_uri "$URI_INSECURE")" "$REST_PORT_INSECURE" "$REST_HTTPS_PORT_INSECURE"
print_rest_endpoints "$(rest_cluster_uri "$URI_SECURE") (user keys)" "$REST_PORT_SECURE" "$REST_HTTPS_PORT_SECURE"
echo "REST logs: $LOG_DIR_REST"
