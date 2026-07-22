ALTER TABLE local_task_event_outbox
    DROP CONSTRAINT local_task_event_outbox_kind_check;

ALTER TABLE local_task_event_outbox
    ADD CONSTRAINT local_task_event_outbox_kind_check
    CHECK (kind IN ('Assigned', 'Started', 'Completed', 'Failed', 'Unhealthy'));

CREATE UNIQUE INDEX local_task_event_outbox_one_terminal
    ON local_task_event_outbox (dispatch_key, pickup_generation)
    WHERE kind IN ('Completed', 'Failed', 'Unhealthy');

CREATE TABLE local_task_terminal_outcomes (
    dispatch_key text NOT NULL,
    pickup_generation bigint NOT NULL CHECK (pickup_generation > 0),
    outcome text NOT NULL CHECK (outcome IN (
        'process-exited-success',
        'process-exited-failure',
        'process-setup-failed',
        'liveness-expired'
    )),
    settled_at timestamp with time zone NOT NULL,
    PRIMARY KEY (dispatch_key, pickup_generation),
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE
);
