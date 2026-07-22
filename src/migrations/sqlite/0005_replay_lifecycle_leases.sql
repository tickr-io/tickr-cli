ALTER TABLE workflow_replays ADD COLUMN lease_owner TEXT;
ALTER TABLE workflow_replays ADD COLUMN lease_token UUID;
ALTER TABLE workflow_replays ADD COLUMN lease_expires_at TIMESTAMP_MICROS;
