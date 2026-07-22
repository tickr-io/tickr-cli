ALTER TABLE workflow_task_builds ADD COLUMN lease_owner TEXT;
ALTER TABLE workflow_task_builds ADD COLUMN lease_token UUID;
ALTER TABLE workflow_task_builds ADD COLUMN lease_expires_at TIMESTAMP_MICROS;

CREATE INDEX workflow_task_builds_eligible_idx
    ON workflow_task_builds (pending_since, workflow_id, workflow_version, task_id)
    WHERE status = 'pending';
