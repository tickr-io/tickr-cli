ALTER TABLE local_task_dispatches ADD COLUMN task_instance_id TEXT;
ALTER TABLE local_task_dispatches ADD COLUMN workflow_instance_id TEXT;

CREATE UNIQUE INDEX local_task_dispatches_task_identity
    ON local_task_dispatches (task_instance_id, workflow_instance_id)
    WHERE task_instance_id IS NOT NULL AND workflow_instance_id IS NOT NULL;

ALTER TABLE local_task_terminal_outcomes RENAME TO local_task_terminal_outcomes_before_cancellation;

CREATE TABLE local_task_terminal_outcomes (
    dispatch_key TEXT NOT NULL,
    pickup_generation INTEGER NOT NULL CHECK (pickup_generation > 0),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'process-exited-success',
        'process-exited-failure',
        'process-setup-failed',
        'liveness-expired',
        'cancellation-killed',
        'cancellation-already-exited',
        'cancellation-no-process'
    )),
    settled_at TIMESTAMP_MICROS NOT NULL,
    PRIMARY KEY (dispatch_key, pickup_generation),
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE
);

INSERT INTO local_task_terminal_outcomes
    (dispatch_key, pickup_generation, outcome, settled_at)
SELECT dispatch_key, pickup_generation, outcome, settled_at
FROM local_task_terminal_outcomes_before_cancellation;

DROP TABLE local_task_terminal_outcomes_before_cancellation;

CREATE TABLE local_task_cancellation_fences (
    acknowledgement_identity TEXT PRIMARY KEY,
    task_instance_id TEXT NOT NULL,
    workflow_instance_id TEXT NOT NULL,
    dispatch_key TEXT,
    pickup_generation INTEGER CHECK (pickup_generation > 0),
    owner TEXT,
    committed_at TIMESTAMP_MICROS NOT NULL,
    owner_notified_at TIMESTAMP_MICROS,
    reconciliation TEXT CHECK (reconciliation IN ('killed', 'already-exited', 'no-process')),
    settled_at TIMESTAMP_MICROS,
    UNIQUE (task_instance_id, workflow_instance_id),
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE,
    CHECK ((dispatch_key IS NULL AND pickup_generation IS NULL AND owner IS NULL)
        OR (dispatch_key IS NOT NULL AND pickup_generation IS NOT NULL)),
    CHECK ((reconciliation IS NULL AND settled_at IS NULL)
        OR (reconciliation IS NOT NULL AND settled_at IS NOT NULL))
);

CREATE TABLE local_task_cancellation_ack_outbox (
    acknowledgement_identity TEXT PRIMARY KEY,
    acknowledgement JSON NOT NULL CHECK (json_valid(acknowledgement)),
    staged_at TIMESTAMP_MICROS NOT NULL,
    forwarded_at TIMESTAMP_MICROS,
    FOREIGN KEY (acknowledgement_identity)
        REFERENCES local_task_cancellation_fences(acknowledgement_identity) ON DELETE CASCADE
);
