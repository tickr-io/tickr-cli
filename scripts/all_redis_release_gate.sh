#!/usr/bin/env bash
set -uo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

log_dir=$(mktemp -d "${TMPDIR:-/tmp}/tickr-all-redis-release-gate.XXXXXX")
trap 'rm -rf "$log_dir"' EXIT

covered_capabilities=()

mark_coverage() {
    local capability
    for capability in "$@"; do
        covered_capabilities+=("$capability")
    done
}

run_command() {
    local suite=$1
    local capability=$2
    shift 2

    local log_file="$log_dir/${suite//\//-}.log"
    printf 'RUN  suite=%s capability=%s\n' "$suite" "$capability"
    if "$@" >"$log_file" 2>&1; then
        printf 'PASS suite=%s capability=%s\n' "$suite" "$capability"
        return 0
    fi

    printf 'all-redis-release-gate: FAIL suite=%s capability=%s\n' "$suite" "$capability" >&2
    exit 1
}

run_target() {
    local suite=$1
    local capability=$2
    local package=$3
    local target=$4
    run_command "$suite" "$capability" \
        cargo test --locked --package "$package" --test "$target" -- \
        --test-threads=1
}

run_ignored_test() {
    local suite=$1
    local capability=$2
    local package=$3
    local target=$4
    local test_name=$5
    run_command "$suite" "$capability" \
        cargo test --locked --package "$package" --test "$target" "$test_name" -- \
        --ignored --exact --test-threads=1
}

require_complete_coverage() {
    local required found covered
    local required_capabilities=(
        command-bus
        task-dispatch
        task-events
        task-cancellation
        compaction-staging
        lifecycle-work
        log-staging
        scope-store
        ingress-idempotency-store
        liveness-watchdog
        signal-applied-notifier
        executor-fleet-status
        event-ingress
        safe-pickup-handoff
        safe-attempt-outcome-handoff
        safe-cancellation-fence
    )

    for required in "${required_capabilities[@]}"; do
        found=0
        for covered in "${covered_capabilities[@]}"; do
            if [[ "$covered" == "$required" ]]; then
                found=1
                break
            fi
        done
        if [[ "$found" -ne 1 ]]; then
            printf 'all-redis-release-gate: FAIL suite=coverage capability=%s\n' "$required" >&2
            exit 1
        fi
    done
}

export CARGO_TERM_COLOR=never
export RUST_BACKTRACE=0

run_command preflight/container-runtime external-service-formation docker info
run_command preflight/openssl tls openssl version
run_command preflight/nickel workflow-registration nickel --version

run_command formation/descriptor complete-role-set-and-choreography \
    cargo test --locked --package tickr --lib formation -- --test-threads=1

mark_coverage command-bus
run_ignored_test role-law/command-bus command-bus tickr redis_command_bus_law_test \
    real_backends_obey_the_same_command_bus_laws

mark_coverage task-dispatch liveness-watchdog safe-pickup-handoff safe-attempt-outcome-handoff
run_ignored_test role-law/task-dispatch task-dispatch tickr redis_task_pickup_law_test \
    real_redis_pickup_laws_cover_capacity_handoff_fences_and_pressure
run_ignored_test role-law/attempt-outcome safe-attempt-outcome-handoff tickr redis_task_pickup_law_test \
    real_redis_outcome_laws_cover_deadlines_races_restart_and_capability_restoration
run_ignored_test role-law/liveness-watchdog liveness-watchdog tickr redis_task_pickup_law_test \
    real_redis_liveness_role_isolated_election_laws
run_ignored_test recovery/task-pickup task-dispatch tickr redis_task_pickup_law_test \
    redis_task_pickup_real_process_crash_matrix

mark_coverage task-events
run_ignored_test role-law/task-events task-events tickr redis_task_events_law_test \
    real_redis_task_event_laws_preserve_bytes_redelivery_and_pressure
run_ignored_test role-law/task-events-production task-events tickr redis_task_events_law_test \
    production_interfaces_preserve_encoded_nonterminal_and_terminal_envelopes
run_ignored_test recovery/task-events task-events tickr redis_task_events_law_test \
    redis_task_event_real_process_crash_boundaries

mark_coverage task-cancellation safe-cancellation-fence
run_ignored_test role-law/task-cancellation task-cancellation tickr redis_task_cancellation_law_test \
    real_redis_cancellation_laws_cover_fences_kill_restart_and_terminal_races
run_ignored_test recovery/task-cancellation safe-cancellation-fence tickr redis_task_cancellation_law_test \
    redis_task_cancellation_real_process_crash_matrix

mark_coverage compaction-staging
run_ignored_test role-law/compaction-staging compaction-staging tickr redis_compaction_staging_law_test \
    real_redis_compaction_laws_cover_staging_redelivery_archive_pressure_and_acl
run_ignored_test recovery/compaction-staging compaction-staging tickr redis_compaction_staging_law_test \
    real_process_crashes_at_every_redis_compaction_boundary_converge

mark_coverage lifecycle-work
run_ignored_test role-law/lifecycle-work lifecycle-work tickr redis_lifecycle_work_law_test \
    real_redis_lifecycle_laws_bound_hints_and_recover_all_sql_pipelines

mark_coverage log-staging
run_ignored_test role-law/log-staging log-staging tickr redis_log_stream_law_test \
    real_redis_log_stream_laws_cover_crash_pressure_seal_archive_and_purge

mark_coverage scope-store
run_target role-law/scope-store scope-store tickr redis_scope_store_law_test

mark_coverage ingress-idempotency-store event-ingress
run_ignored_test role-law/event-ingress event-ingress tickr redis_event_ingress_law_test \
    real_redis_delivery_crosses_the_production_consumer_once
run_ignored_test role-law/event-ingress-recovery event-ingress tickr redis_event_ingress_law_test \
    real_redis_event_ingress_laws_cover_replay_reclaim_pressure_rejection_and_ack
run_ignored_test role-law/ingress-idempotency-pressure ingress-idempotency-store tickr redis_event_ingress_law_test \
    real_redis_ingress_idempotency_hard_limit_preserves_accepted_reservation
run_ignored_test recovery/event-ingress event-ingress tickr redis_event_ingress_law_test \
    real_process_redis_ingress_crash_boundaries_converge

mark_coverage signal-applied-notifier
run_ignored_test role-law/signal-applied-notifier signal-applied-notifier tickr redis_signal_applied_notifier_law_test \
    real_redis_signal_notifier_laws_cover_acl_pressure_restart_and_reconciliation

mark_coverage executor-fleet-status
run_target role-law/executor-fleet-status-local executor-fleet-status tickr \
    redis_executor_fleet_status_law_test
run_ignored_test role-law/executor-fleet-status-redis executor-fleet-status tickr \
    redis_executor_fleet_status_law_test \
    real_redis_fleet_laws_cover_duplicates_pressure_isolation_restart_and_dispatch_independence

run_target admission/local-laws admission-state-machine tickr redis_admission_test
run_ignored_test admission/tls-topology-version tls-topology-version tickr redis_admission_test \
    redis_oss_74_tls_admission_matrix
run_ignored_test admission/formation-identity formation-identity tickr redis_admission_test \
    redis_identity_inspection_and_canary_are_namespace_scoped
run_ignored_test pressure/calibrated-role-matrix quota-reserve-cleanup-boundaries tickr \
    redis_admission_test redis_role_quota_calibration_covers_all_roles_and_real_pressure
run_ignored_test admission/capacity-reserve quota-reserve tickr redis_admission_test \
    redis_admission_rejects_capacity_without_real_reserve
run_ignored_test recovery/primary-local-durability aof-fsync-boundaries tickr redis_admission_test \
    redis_primary_local_durability_crash_boundaries
run_ignored_test recovery/runtime-capability capability-loss-reconstruction tickr redis_admission_test \
    redis_capability_monitor_real_process_loss_and_recovery_matrix

run_ignored_test admission/acl-matrix acl-identity-isolation tickr redis_acl_admission_test \
    real_tls_acl_matrix_proves_complete_probe_commit_and_recovery
run_ignored_test smoke/diagnostics-startup secret-free-diagnostics-and-startup tickr \
    redis_acl_admission_test all_redis_diagnostics_startup_smoke_has_no_nats_dependency
run_ignored_test smoke/all-redis-workflow workflow-ingress-cancellation-recovery tickr \
    redis_acl_admission_test all_redis_workflow_recovery_release_smoke

run_target parity/all-nats-formation fresh-all-nats-common-role-evidence tickr_conductor \
    all_nats_formation_test
run_target parity/all-nats-choreography fresh-all-nats-handoff-and-cancellation tickr_executor \
    task_dispatch_test
run_target parity/tickr-lite tickr-lite-common-role-and-crash-evidence tickr \
    lite_formation_parity_test

require_complete_coverage
printf 'all-redis-release-gate: PASS\n'
