CREATE TABLE public.local_task_dispatches (
    dispatch_key text PRIMARY KEY,
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'claimed', 'rejected')),
    pickup_generation bigint NOT NULL DEFAULT 0 CHECK (pickup_generation >= 0),
    owner text,
    liveness_deadline timestamp with time zone,
    liveness_armed_at timestamp with time zone,
    rejection_reason text,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CHECK (
        (state = 'pending' AND owner IS NULL AND liveness_deadline IS NULL AND liveness_armed_at IS NULL AND rejection_reason IS NULL)
        OR
        (state = 'claimed' AND owner IS NOT NULL AND liveness_deadline IS NOT NULL AND rejection_reason IS NULL)
        OR
        (state = 'rejected' AND owner IS NULL AND liveness_deadline IS NULL AND liveness_armed_at IS NULL AND rejection_reason IS NOT NULL)
    )
);

CREATE TABLE public.local_task_event_outbox (
    dispatch_key text NOT NULL,
    pickup_generation bigint NOT NULL CHECK (pickup_generation > 0),
    kind text NOT NULL CHECK (kind IN ('Assigned', 'Started')),
    event jsonb NOT NULL,
    staged_at timestamp with time zone NOT NULL,
    forwarded_at timestamp with time zone,
    PRIMARY KEY (dispatch_key, pickup_generation, kind),
    FOREIGN KEY (dispatch_key) REFERENCES public.local_task_dispatches(dispatch_key) ON DELETE CASCADE
);

CREATE TABLE public.local_task_dispatch_quarantine (
    dispatch_key text PRIMARY KEY,
    payload jsonb NOT NULL,
    reason text NOT NULL,
    quarantined_at timestamp with time zone NOT NULL,
    FOREIGN KEY (dispatch_key) REFERENCES public.local_task_dispatches(dispatch_key) ON DELETE CASCADE
);
