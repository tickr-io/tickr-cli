#!/usr/bin/env bash
# Run the Rust suite against a disposable PostgreSQL instance. This must never
# reconcile the developer's tickr-cli-dev formation: verification runs from a
# worktree while the developer may have that formation running.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
# shellcheck disable=SC1090
source "${repo_root}/.envrc"

# An explicit URL is an operator/CI override. The default path below is always
# isolated and discarded after this invocation.
if [[ -n "${TICKR_TEST_PG_URL:-}" ]]; then
    exec cargo test --locked --workspace "$@" -- --test-threads=1
fi

workspace_hash="$(printf '%s' "${repo_root}" | shasum -a 256 | cut -c1-12)"
project="${TICKR_TEST_INFRA_PROJECT:-tickr-cli-test-${workspace_hash}}"
compose=(docker compose --project-name "${project}" --file "${repo_root}/docker-compose-test.yml")

cleanup() {
    local result=$?
    trap - EXIT
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
    exit "${result}"
}
trap cleanup EXIT

"${compose[@]}" up --detach --wait
endpoint="$("${compose[@]}" port db 5432)"
case "${endpoint}" in
    127.0.0.1:[0-9]*) ;;
    *)
        echo "test: isolated PostgreSQL exposed an unusable endpoint: ${endpoint}" >&2
        exit 1
        ;;
esac

export TICKR_TEST_PG_URL="postgres://${TICKR_DEV_POSTGRES_USER}:${TICKR_DEV_POSTGRES_PASSWORD}@${endpoint}"
cargo test --locked --workspace "$@" -- --test-threads=1
