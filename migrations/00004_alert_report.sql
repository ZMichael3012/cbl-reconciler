-- +goose Up
-- The listing index is created concurrently in 00005 (the table is written by the pass).
-- Alert metadata for the discrepancy report and dashboard (CBL-9.4). The
-- metadata is written on the first dedup insert; repeated detections only move
-- last_seen_at and occurrences. Legacy rows keep the empty defaults.
ALTER TABLE recon_alert_keys
    ADD COLUMN alert_type  text NOT NULL DEFAULT '',
    ADD COLUMN ref_id      text NOT NULL DEFAULT '',
    ADD COLUMN merchant_id text NOT NULL DEFAULT '',
    ADD COLUMN severity    text NOT NULL DEFAULT '',
    ADD COLUMN expected    text NOT NULL DEFAULT '',
    ADD COLUMN actual      text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE recon_alert_keys
    DROP COLUMN actual,
    DROP COLUMN expected,
    DROP COLUMN severity,
    DROP COLUMN merchant_id,
    DROP COLUMN ref_id,
    DROP COLUMN alert_type;
