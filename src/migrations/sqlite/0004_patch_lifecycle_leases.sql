ALTER TABLE workflow_patch_task_builds ADD COLUMN lease_owner TEXT;
ALTER TABLE workflow_patch_task_builds ADD COLUMN lease_token UUID;
ALTER TABLE workflow_patch_task_builds ADD COLUMN lease_expires_at TIMESTAMP_MICROS;

CREATE INDEX workflow_patch_task_builds_eligible_idx
    ON workflow_patch_task_builds (pending_since, patch_key, task_id)
    WHERE status = 'pending';

ALTER TABLE workflow_patches ADD COLUMN lifecycle_lease_owner TEXT;
ALTER TABLE workflow_patches ADD COLUMN lifecycle_lease_token UUID;
ALTER TABLE workflow_patches ADD COLUMN lifecycle_lease_expires_at TIMESTAMP_MICROS;

CREATE INDEX workflow_patches_lifecycle_eligible_idx
    ON workflow_patches (updated_at, patch_key)
    WHERE status IN ('Validating', 'Submitted');
