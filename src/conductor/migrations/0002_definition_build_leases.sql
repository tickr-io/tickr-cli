ALTER TABLE public.workflow_task_builds
    ADD COLUMN lease_owner text,
    ADD COLUMN lease_token uuid,
    ADD COLUMN lease_expires_at timestamp with time zone,
    ADD CONSTRAINT workflow_task_builds_lease_complete_check CHECK (
        (lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
        OR
        (lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    );

CREATE INDEX workflow_task_builds_eligible_idx
    ON public.workflow_task_builds (pending_since, workflow_id, workflow_version, task_id)
    WHERE status = 'pending';
