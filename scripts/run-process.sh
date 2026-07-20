#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

if [[ $# -lt 2 ]]; then
    echo "usage: $0 <process-name> <command> [args ...]" >&2
    exit 2
fi

process_name=$1
shift
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$repo_root/logs"

"$@" 2>&1 | tee "$repo_root/logs/${process_name}.log"
