CREATE TABLE local_task_dispatches (
    dispatch_key TEXT PRIMARY KEY NOT NULL,
    payload JSON NOT NULL CHECK (json_valid(payload)),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'claimed', 'rejected')),
    pickup_generation INTEGER NOT NULL DEFAULT 0 CHECK (pickup_generation >= 0),
    owner TEXT,
    liveness_deadline TIMESTAMP_MICROS,
    liveness_armed_at TIMESTAMP_MICROS,
    rejection_reason TEXT,
    created_at TIMESTAMP_MICROS NOT NULL,
    updated_at TIMESTAMP_MICROS NOT NULL,
    CHECK (
        (state = 'pending' AND owner IS NULL AND liveness_deadline IS NULL AND liveness_armed_at IS NULL AND rejection_reason IS NULL)
        OR
        (state = 'claimed' AND owner IS NOT NULL AND liveness_deadline IS NOT NULL AND rejection_reason IS NULL)
        OR
        (state = 'rejected' AND owner IS NULL AND liveness_deadline IS NULL AND liveness_armed_at IS NULL AND rejection_reason IS NOT NULL)
    )
);

CREATE TABLE local_task_event_outbox (
    dispatch_key TEXT NOT NULL,
    pickup_generation INTEGER NOT NULL CHECK (pickup_generation > 0),
    kind TEXT NOT NULL CHECK (kind IN ('Assigned', 'Started')),
    event JSON NOT NULL CHECK (json_valid(event)),
    staged_at TIMESTAMP_MICROS NOT NULL,
    forwarded_at TIMESTAMP_MICROS,
    PRIMARY KEY (dispatch_key, pickup_generation, kind),
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE
);

CREATE TABLE local_task_dispatch_quarantine (
    dispatch_key TEXT PRIMARY KEY NOT NULL,
    payload JSON NOT NULL CHECK (json_valid(payload)),
    reason TEXT NOT NULL,
    quarantined_at TIMESTAMP_MICROS NOT NULL,
    FOREIGN KEY (dispatch_key) REFERENCES local_task_dispatches(dispatch_key) ON DELETE CASCADE
);
