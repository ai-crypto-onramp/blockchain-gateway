-- broadcasts: one row per broadcast attempt. Idempotent on (chain_id, tx_hash).
CREATE TABLE IF NOT EXISTS broadcasts (
    chain_id      TEXT        NOT NULL,
    tx_hash       TEXT        NOT NULL,
    signed_tx     BYTEA       NOT NULL,
    from_addr     TEXT        NOT NULL DEFAULT '',
    to_addr       TEXT        NOT NULL DEFAULT '',
    value         NUMERIC     NOT NULL DEFAULT 0,
    nonce         BIGINT      NOT NULL DEFAULT 0,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_by  TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (chain_id, tx_hash)
);

-- tx_confirmations: confirmation tracker state.
CREATE TABLE IF NOT EXISTS tx_confirmations (
    chain_id       TEXT        NOT NULL,
    tx_hash        TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    block_height   BIGINT      NOT NULL DEFAULT 0,
    block_hash     TEXT        NOT NULL DEFAULT '',
    confirmations  BIGINT      NOT NULL DEFAULT 0,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at   TIMESTAMPTZ,
    finalized_at   TIMESTAMPTZ,
    PRIMARY KEY (chain_id, tx_hash)
);

-- chain_tips: hot row per chain.
CREATE TABLE IF NOT EXISTS chain_tips (
    chain_id         TEXT        NOT NULL PRIMARY KEY,
    tip_height       BIGINT      NOT NULL DEFAULT 0,
    tip_hash         TEXT        NOT NULL DEFAULT '',
    finalized_height BIGINT      NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- fee_estimates: time-series.
CREATE TABLE IF NOT EXISTS fee_estimates (
    chain_id               TEXT        NOT NULL,
    priority               TEXT        NOT NULL,
    gas_limit              BIGINT      NOT NULL DEFAULT 0,
    max_fee_per_gas        NUMERIC     NOT NULL DEFAULT 0,
    max_priority_fee_per_gas NUMERIC   NOT NULL DEFAULT 0,
    gas_price              NUMERIC     NOT NULL DEFAULT 0,
    total_fee              NUMERIC     NOT NULL DEFAULT 0,
    sample_count           INT         NOT NULL DEFAULT 0,
    strategy               TEXT        NOT NULL DEFAULT '',
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, priority, computed_at)
);

-- reorg_events: append-only audit.
CREATE TABLE IF NOT EXISTS reorg_events (
    id                      BIGSERIAL   PRIMARY KEY,
    chain_id                TEXT        NOT NULL,
    detected_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    old_tip_hash            TEXT        NOT NULL DEFAULT '',
    new_tip_hash            TEXT        NOT NULL DEFAULT '',
    common_ancestor_height  BIGINT      NOT NULL DEFAULT 0,
    affected_tx_hashes      TEXT[]      NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS reorg_events_chain_id_idx ON reorg_events(chain_id);

-- outbox: deduped event emission.
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL   PRIMARY KEY,
    chain_id      TEXT        NOT NULL,
    tx_hash       TEXT        NOT NULL,
    status        TEXT        NOT NULL,
    block_height  BIGINT      NOT NULL DEFAULT 0,
    event_type    TEXT        NOT NULL,
    payload       BYTEA       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    emitted_at    TIMESTAMPTZ,
    UNIQUE (chain_id, tx_hash, status, block_height)
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(emitted_at) WHERE emitted_at IS NULL;