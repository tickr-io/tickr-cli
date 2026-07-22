ALTER TABLE workflows ADD COLUMN submission_lease_owner TEXT;
ALTER TABLE workflows ADD COLUMN submission_lease_token UUID;
ALTER TABLE workflows ADD COLUMN submission_lease_expires_at TIMESTAMP_MICROS;

CREATE INDEX workflows_submission_eligible_idx
    ON workflows (id, version)
    WHERE status = 'Ready';
