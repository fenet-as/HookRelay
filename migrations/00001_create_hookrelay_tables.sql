-- +goose Up
-- +goose StatementBegin
CREATE TABLE endpoints (
    id          BIGSERIAL PRIMARY KEY,
    url         TEXT        NOT NULL,
    secret      TEXT        NOT NULL,
    event_types TEXT[]      NOT NULL DEFAULT '{}',
    active      BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
    id              TEXT PRIMARY KEY,
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    idempotency_key TEXT UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE delivery_status AS ENUM ('pending', 'succeeded', 'dead');

CREATE TABLE deliveries (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT            NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id BIGINT          NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    status      delivery_status NOT NULL DEFAULT 'pending',
    attempts    INT             NOT NULL DEFAULT 0,
    next_run_at TIMESTAMPTZ     NOT NULL DEFAULT now(),
    locked_at   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);

-- Partial index: only index pending deliveries ordered by next_run_at.
-- This keeps the index tiny and fast even with millions of succeeded/dead deliveries.
CREATE INDEX deliveries_due_idx ON deliveries (next_run_at) WHERE status = 'pending';

CREATE TABLE delivery_attempts (
    id            BIGSERIAL PRIMARY KEY,
    delivery_id   BIGINT      NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt       INT         NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    duration_ms   INT,
    status_code   INT,
    error         TEXT,
    response_body TEXT
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS deliveries;
DROP TYPE IF EXISTS delivery_status;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS endpoints;
-- +goose StatementEnd

