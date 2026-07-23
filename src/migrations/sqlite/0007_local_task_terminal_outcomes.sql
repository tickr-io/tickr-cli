ALTER TABLE local_task_event_outbox RENAME TO local_task_event_outbox_before_terminal_outcomes;

CREATE TABLE local_task_event_outbox (
    dispatch_key TEXT NOT NULL,
    pickup_generation INTEGER NOT NULL CHECK (pickup_generation > 0),
    kind TEXT NOT NULL CHECK (kind IN ('Assigned', 'Started', 'Completed', 'Failed', 'Unhealthy')),
    event JSON NOT NULL CHECK (json_valid(event)),
    staged_at TIMESTAMP_MICROS NOT NULL,
    forwarded_at TIMESTAMP_MICROS,
    PRIMARY KEY (dispatch_key, pickup_generation, kind),
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE
);

INSERT INTO local_task_event_outbox
    (dispatch_key, pickup_generation, kind, event, staged_at, forwarded_at)
SELECT dispatch_key, pickup_generation, kind, event, staged_at, forwarded_at
FROM local_task_event_outbox_before_terminal_outcomes;

DROP TABLE local_task_event_outbox_before_terminal_outcomes;

CREATE UNIQUE INDEX local_task_event_outbox_one_terminal
    ON local_task_event_outbox (dispatch_key, pickup_generation)
    WHERE kind IN ('Completed', 'Failed', 'Unhealthy');

CREATE TABLE local_task_terminal_outcomes (
    dispatch_key TEXT NOT NULL,
    pickup_generation INTEGER NOT NULL CHECK (pickup_generation > 0),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'process-exited-success',
        'process-exited-failure',
        'process-setup-failed',
        'liveness-expired'
    )),
    settled_at TIMESTAMP_MICROS NOT NULL,
    PRIMARY KEY (dispatch_key, pickup_generation),
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE
);
