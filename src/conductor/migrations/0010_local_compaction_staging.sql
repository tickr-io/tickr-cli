CREATE TABLE public.local_compaction_staging (
    workflow_instance_id uuid PRIMARY KEY,
    protocol_version bigint NOT NULL,
    payload_digest text NOT NULL,
    payload bytea,
    state text NOT NULL CHECK (state IN ('staged', 'complete', 'purged')),
    scope_id uuid,
    scope_digest text,
    final_log_references jsonb,
    staged_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    purged_at timestamp with time zone,
    CHECK (
        (state = 'staged' AND payload IS NOT NULL AND completed_at IS NULL AND purged_at IS NULL)
        OR (state = 'complete' AND payload IS NOT NULL AND completed_at IS NOT NULL AND purged_at IS NULL)
        OR (state = 'purged' AND payload IS NULL AND completed_at IS NOT NULL AND purged_at IS NOT NULL)
    )
);

CREATE INDEX local_compaction_staging_pending_idx
    ON public.local_compaction_staging (state, staged_at, workflow_instance_id);
