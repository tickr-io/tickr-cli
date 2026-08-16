default:
    @just --list

compose := "docker compose --project-name tickr-cli-dev --file docker-compose-infra.yml"

install-hooks:
    git config core.hooksPath .githooks

build:
    cargo build --workspace

check:
    cargo fmt --check
    cargo check --locked --workspace --all-targets
    python3 scripts/dependency_policy_gate.py
    python3 scripts/public_contract_gate.py
    python3 scripts/repository_hygiene_gate.py
    python3 scripts/control_plane_terminology_gate.py
    python3 scripts/security_source_gate.py
    python3 scripts/license_gate.py --check

# Non-destructive repository security and licensing policy checks.
security-static:
    python3 scripts/dependency_policy_gate.py --self-test
    python3 scripts/dependency_policy_gate.py
    python3 scripts/public_contract_gate.py --self-test
    python3 scripts/public_contract_gate.py
    python3 scripts/repository_hygiene_gate.py --self-test
    python3 scripts/repository_hygiene_gate.py
    python3 scripts/control_plane_terminology_gate.py --self-test
    python3 scripts/control_plane_terminology_gate.py
    python3 scripts/security_source_gate.py --self-test
    python3 scripts/security_source_gate.py
    python3 scripts/license_gate.py --self-test
    python3 scripts/npm_audit_gate.py --self-test
    python3 scripts/npm_audit_gate.py
    python3 scripts/license_gate.py --check
    bash -c 'source .envrc; exec docker compose --file docker-compose-infra.yml config -q'

# Full worktree/dependency audit. Requires cargo-audit, cargo-deny, and gitleaks.
# Each RustSec exception is narrow and documented in deny.toml and the
# production-hardening guide; no advisory class is skipped wholesale.
security:
    just security-static
    cargo audit --file Cargo.lock --ignore RUSTSEC-2026-0194 --ignore RUSTSEC-2026-0195 --ignore RUSTSEC-2025-0111 --ignore RUSTSEC-2023-0071
    cargo deny check advisories licenses bans sources
    gitleaks dir . --no-banner --redact --exit-code 1

licenses:
    python3 scripts/license_gate.py

# Networked maintenance command: refresh locked npm license/notice evidence.
refresh-npm-attribution:
    python3 scripts/refresh_npm_attribution.py
    python3 scripts/license_gate.py

check-licenses:
    python3 scripts/license_gate.py --check

migrate:
    bash -c 'source .envrc; exec cargo run --bin tickr-cli -- migrate'

# Start PostgreSQL, NATS/JetStream, MinIO, and the idempotent bucket provisioner.
infra-up:
    mkdir -p infra logs
    bash -c 'source .envrc; exec docker compose --project-name tickr-cli-dev --file docker-compose-infra.yml up -d'

infra-down:
    bash -c 'source .envrc; exec docker compose --project-name tickr-cli-dev --file docker-compose-infra.yml down'

# Start the complete local runtime formation in the foreground.
up:
    just infra-up
    just _wait-migrate
    bash -c 'source .envrc; exec overmind start'

# Start the complete data-plane formation detached.
up-bg:
    just infra-up
    just _wait-migrate
    bash -c 'source .envrc; exec overmind start -D'

# Stop processes and containers without deleting local state.
down:
    -overmind quit
    bash -c 'source .envrc; exec docker compose --project-name tickr-cli-dev --file docker-compose-infra.yml down'

# Destructive greenfield reset of this repository's development state.
fresh:
    -overmind quit
    bash -c 'source .envrc; exec docker compose --project-name tickr-cli-dev --file docker-compose-infra.yml down'
    rm -rf infra logs
    mkdir -p infra logs
    just infra-up
    just _wait-migrate
    bash -c 'source .envrc; exec overmind start -D'
    @echo "Formation is compiling/starting; run 'just verify' to wait for readiness."

# Restart one application process, or bounce the complete data-plane formation
# (applications plus PostgreSQL, NATS/JetStream, and MinIO) without deleting state.
restart service:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "{{service}}" == "all" ]]; then
        just down
        just up-bg
    else
        overmind restart "{{service}}"
    fi

logs service:
    overmind connect {{service}}

ps:
    overmind ps

# Retry the real idempotent migration so first-time image startup is handled.
_wait-migrate:
    #!/usr/bin/env bash
    set -uo pipefail
    for attempt in $(seq 1 30); do
        if just migrate; then
            exit 0
        fi
        if [[ "$attempt" -eq 30 ]]; then
            echo "Postgres did not become migration-ready after 30 attempts" >&2
            exit 1
        fi
        echo "Postgres not migration-ready (attempt $attempt/30); retrying in 2s..."
        sleep 2
    done

# Wait until the shared development/test Postgres accepts connections.
_wait-db:
    #!/usr/bin/env bash
    set -euo pipefail
    source .envrc
    for attempt in $(seq 1 30); do
        if {{compose}} exec -T db pg_isready -U "$TICKR_DEV_POSTGRES_USER" -d postgres >/dev/null 2>&1; then
            exit 0
        fi
        sleep 2
    done
    echo "shared test Postgres did not become ready" >&2
    exit 1

# Verify only the four data-plane processes, their infrastructure, and ports.
verify:
    #!/usr/bin/env bash
    set -uo pipefail
    source .envrc
    fail=0

    retry_url() {
        local label=$1 url=$2
        for attempt in $(seq 1 90); do
            if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
                echo "  ok    $label"
                return 0
            fi
            sleep 2
        done
        echo "  FAIL  $label ($url)" >&2
        return 1
    }

    echo "== Overmind (exact data-plane formation) =="
    overmind_ps=$(overmind ps 2>/dev/null || true)
    normalized_ps=$(awk '
        toupper($1) == "PROCESS" { next }
        NF {
            if (index($0, "|") > 0) {
                split($0, fields, "|")
                name=fields[1]; state=tolower(fields[2])
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", state)
            } else {
                name=$1; state=tolower($NF)
            }
            sub(/:$/, "", name)
            print name, state
        }
    ' <<<"$overmind_ps")
    process_names=$(awk 'NF {print $1}' <<<"$normalized_ps" | sort | tr '\n' ' ' | sed 's/ $//')
    expected_names="api conductor console executor"
    if [[ "$process_names" != "$expected_names" ]]; then
        echo "  FAIL  expected exactly: $expected_names" >&2
        echo "        found: ${process_names:-none}" >&2
        fail=1
    fi
    for service in api conductor console executor; do
        if ! awk -v service="$service" '$1 == service && $2 == "running" { found=1 } END { exit !found }' <<<"$normalized_ps"; then
            echo "  FAIL  $service is not running" >&2
            fail=1
        else
            echo "  ok    $service"
        fi
    done

    echo "== PostgreSQL =="
    if {{compose}} exec -T db pg_isready -U "$TICKR_DEV_POSTGRES_USER" -d tickr >/dev/null 2>&1; then
        echo "  ok    tickr database"
        applied=$({{compose}} exec -T -e PGPASSWORD="$TICKR_DEV_POSTGRES_PASSWORD" db psql -U "$TICKR_DEV_POSTGRES_USER" -d tickr -Atqc \
            'SELECT count(*) FROM _sqlx_migrations WHERE success = TRUE' 2>/dev/null || true)
        expected=$(find src/conductor/migrations -maxdepth 1 -type f -name '*.sql' | wc -l | tr -d ' ')
        if [[ "$applied" == "$expected" ]]; then
            echo "  ok    conductor schema ($applied migrations)"
        else
            echo "  FAIL  conductor schema is not current: ${applied:-0}/$expected migrations applied" >&2
            fail=1
        fi
    else
        echo "  FAIL  tickr database is not ready" >&2
        fail=1
    fi

    echo "== NATS/JetStream =="
    if curl -fsS --max-time 2 http://127.0.0.1:8222/healthz >/dev/null 2>&1 && \
       curl -fsS --max-time 2 http://127.0.0.1:8222/jsz >/dev/null 2>&1; then
        echo "  ok    NATS with JetStream"
    else
        echo "  FAIL  NATS/JetStream monitoring endpoints" >&2
        fail=1
    fi

    echo "== MinIO =="
    if curl -fsS --max-time 2 http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
        echo "  ok    MinIO"
    else
        echo "  FAIL  MinIO health endpoint" >&2
        fail=1
    fi
    if {{compose}} run --rm --no-deps \
        -e MC_USER="$TICKR_DEV_MINIO_ROOT_USER" -e MC_PASSWORD="$TICKR_DEV_MINIO_ROOT_PASSWORD" \
        --entrypoint /bin/sh createbuckets -c \
        'mc alias set local http://minio:9000 "$MC_USER" "$MC_PASSWORD" >/dev/null && mc stat local/tickr-logs >/dev/null' \
        >/dev/null 2>&1; then
        echo "  ok    tickr-logs bucket"
    else
        echo "  FAIL  tickr-logs bucket" >&2
        fail=1
    fi

    echo "== HTTP =="
    retry_url "API :6000" http://127.0.0.1:6000/health || fail=1
    retry_url "Console :3000" http://127.0.0.1:3000/ || fail=1

    if [[ "$fail" -ne 0 ]]; then
        echo "verify: FAILED" >&2
        exit 1
    fi
    echo "verify: ok"

test *args:
    docker info >/dev/null
    scripts/test_with_isolated_postgres.sh {{args}}

# Run the complete supportability evidence against disposable external services.
all-redis-release-gate:
    scripts/all_redis_release_gate.sh

dsl-check file:
    nickel export {{file}} --format json -I dsl

console-install:
    cd console && npm ci

console-test:
    cd console && npm test

console-build:
    cd console && npm run build

docs-install:
    cd docs-site && npm ci

docs-start:
    cd docs-site && npm run start

docs-typecheck:
    cd docs-site && npm run typecheck

docs-build:
    cd docs-site && npm run build

docs-check: docs-install
    cd docs-site && npm run typecheck
    cd docs-site && npm run build

# Regenerate the code-first OpenAPI contract and Console TypeScript bindings.
generate-openapi:
    cargo run -p tickr_api --bin generate_openapi
    cd console && npm run generate-api

# Fail when either committed generated artifact differs from its source.
check-openapi:
    cargo run -p tickr_api --bin generate_openapi -- --check
    cd console && npm run check-api
