-- +goose Up
-- Reconciliation state (17.13): per-provider export windows and cursors, the
-- canonical observations, the replay barrier, the payout verdict store and the
-- alert dedup keys.
CREATE TABLE recon_runs (
    provider text PRIMARY KEY,
    cursor text NOT NULL DEFAULT '',
    window_from timestamptz NOT NULL,
    window_until timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE recon_canonical_seen (
    payment_id uuid NOT NULL,
    state text NOT NULL,
    provider text NOT NULL,
    gateway_id text NOT NULL,
    received_at timestamptz NOT NULL,
    PRIMARY KEY (payment_id, state)
);

CREATE TABLE recon_replays (
    payment_id uuid NOT NULL,
    state text NOT NULL,
    replayed_at timestamptz NOT NULL,
    PRIMARY KEY (payment_id, state)
);

CREATE TABLE recon_payout_observations (
    payout_id uuid PRIMARY KEY,
    provider text NOT NULL,
    gateway_payout_id text NOT NULL,
    type text NOT NULL,
    received_at timestamptz NOT NULL
);

CREATE TABLE recon_alert_keys (
    dedup_key text PRIMARY KEY,
    alert_id uuid NOT NULL,
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    occurrences integer NOT NULL DEFAULT 1
);

-- +goose Down
DROP TABLE IF EXISTS recon_alert_keys;
DROP TABLE IF EXISTS recon_payout_observations;
DROP TABLE IF EXISTS recon_replays;
DROP TABLE IF EXISTS recon_canonical_seen;
DROP TABLE IF EXISTS recon_runs;
