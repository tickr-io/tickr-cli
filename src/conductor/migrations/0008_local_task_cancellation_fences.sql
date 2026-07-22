ALTER TABLE local_task_dispatches ADD COLUMN task_instance_id text;
ALTER TABLE local_task_dispatches ADD COLUMN workflow_instance_id text;

CREATE UNIQUE INDEX local_task_dispatches_task_identity
    ON local_task_dispatches (task_instance_id, workflow_instance_id)
    WHERE task_instance_id IS NOT NULL AND workflow_instance_id IS NOT NULL;

ALTER TABLE local_task_terminal_outcomes
    DROP CONSTRAINT local_task_terminal_outcomes_outcome_check;

ALTER TABLE local_task_terminal_outcomes
    ADD CONSTRAINT local_task_terminal_outcomes_outcome_check
    CHECK (outcome IN (
        'process-exited-success',
        'process-exited-failure',
        'process-setup-failed',
        'liveness-expired',
        'cancellation-killed',
        'cancellation-already-exited',
        'cancellation-no-process'
    ));

CREATE TABLE local_task_cancellation_fences (
    acknowledgement_identity text PRIMARY KEY,
    task_instance_id text NOT NULL,
    workflow_instance_id text NOT NULL,
    dispatch_key text,
    pickup_generation bigint CHECK (pickup_generation > 0),
    owner text,
    committed_at timestamp with time zone NOT NULL,
    owner_notified_at timestamp with time zone,
    reconciliation text CHECK (reconciliation IN ('killed', 'already-exited', 'no-process')),
    settled_at timestamp with time zone,
    UNIQUE (task_instance_id, workflow_instance_id),
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE,
    CHECK ((dispatch_key IS NULL AND pickup_generation IS NULL AND owner IS NULL)
        OR (dispatch_key IS NOT NULL AND pickup_generation IS NOT NULL)),
    CHECK ((reconciliation IS NULL AND settled_at IS NULL)
        OR (reconciliation IS NOT NULL AND settled_at IS NOT NULL))
);

CREATE TABLE local_task_cancellation_ack_outbox (
    acknowledgement_identity text PRIMARY KEY,
    acknowledgement jsonb NOT NULL,
    staged_at timestamp with time zone NOT NULL,
    forwarded_at timestamp with time zone,
    FOREIGN KEY (acknowledgement_identity)
        REFERENCES local_task_cancellation_fences(acknowledgement_identity) ON DELETE CASCADE
);
