ALTER TABLE public.workflows
    ADD COLUMN submission_lease_owner text,
    ADD COLUMN submission_lease_token uuid,
    ADD COLUMN submission_lease_expires_at timestamp with time zone,
    ADD CONSTRAINT workflows_submission_lease_complete_check CHECK (
        (submission_lease_owner IS NULL AND submission_lease_token IS NULL AND submission_lease_expires_at IS NULL)
        OR
        (submission_lease_owner IS NOT NULL AND submission_lease_token IS NOT NULL AND submission_lease_expires_at IS NOT NULL)
    );

CREATE INDEX workflows_submission_eligible_idx
    ON public.workflows (id, version)
    WHERE status = 'Ready';
