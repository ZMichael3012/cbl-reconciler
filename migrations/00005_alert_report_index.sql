-- +goose NO TRANSACTION
-- +goose Up
-- The report/dashboard listing index (CBL-9.4). CONCURRENTLY keeps the alert
-- inserts of the recon pass unblocked while the index builds.
CREATE INDEX CONCURRENTLY IF NOT EXISTS recon_alert_keys_last_seen_idx
    ON recon_alert_keys (last_seen_at DESC);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS recon_alert_keys_last_seen_idx;
