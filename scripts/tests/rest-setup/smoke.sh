#!/usr/bin/env bash

set +x
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/config.sh"
source "$SCRIPT_DIR/utils.sh"

command -v curl >/dev/null
command -v jq >/dev/null

case "${1:-both}" in
    insecure) check_address insecure ;;
    secure) check_address secure ;;
    both)
        check_address insecure
        check_address secure
        ;;
    *) echo 'Usage: smoke.sh [both|insecure|secure]' >&2; exit 2 ;;
esac
