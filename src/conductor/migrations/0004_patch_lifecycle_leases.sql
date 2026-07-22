ALTER TABLE public.workflow_patch_task_builds
    ADD COLUMN lease_owner text,
    ADD COLUMN lease_token uuid,
    ADD COLUMN lease_expires_at timestamp with time zone,
    ADD CONSTRAINT workflow_patch_task_builds_lease_complete_check CHECK (
        (lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
        OR
        (lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    );

CREATE INDEX workflow_patch_task_builds_eligible_idx
    ON public.workflow_patch_task_builds (pending_since, patch_key, task_id)
    WHERE status = 'pending';

ALTER TABLE public.workflow_patches
    ADD COLUMN lifecycle_lease_owner text,
    ADD COLUMN lifecycle_lease_token uuid,
    ADD COLUMN lifecycle_lease_expires_at timestamp with time zone,
    ADD CONSTRAINT workflow_patches_lifecycle_lease_complete_check CHECK (
        (lifecycle_lease_owner IS NULL AND lifecycle_lease_token IS NULL AND lifecycle_lease_expires_at IS NULL)
        OR
        (lifecycle_lease_owner IS NOT NULL AND lifecycle_lease_token IS NOT NULL AND lifecycle_lease_expires_at IS NOT NULL)
    );

CREATE INDEX workflow_patches_lifecycle_eligible_idx
    ON public.workflow_patches (updated_at, patch_key)
    WHERE status IN ('Validating', 'Submitted');
