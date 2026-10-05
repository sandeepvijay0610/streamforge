-- ============================================================================
-- StreamForge V1 — PostgreSQL 16 Initialization
-- ============================================================================
-- Loaded via /docker-entrypoint-initdb.d/01-init.sql
--
-- Tables:
--   transactions       — canonical cleaned transaction events
--   processed_events   — deduplication / processing state machine
--   anomaly_alerts     — suspicious-pattern detections (not confirmed fraud)
--   processing_errors  — queryable operational error records
--
-- No foreign keys, triggers, stored procedures, materialized views,
-- or extensions. Kafka topics remain the durable failed-event stream.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- transactions
-- ----------------------------------------------------------------------------
-- Canonical cleaned transaction events.
--
-- amount_minor stores a NON-NEGATIVE event magnitude, not a signed ledger
-- movement. V1 supports only 2-decimal currencies; currency decimal rules
-- are enforced in application code, not in SQL.
--
-- currency and country_code must be uppercase (enforced here as a safety net).
-- status is intentionally constrained to a strict V1 enum.
-- payment_method is intentionally NOT constrained in the DB so that future
-- payment methods do not require a migration; canonical values are enforced
-- by application / Pydantic validation.
-- ----------------------------------------------------------------------------
CREATE TABLE transactions (
    id              BIGSERIAL     PRIMARY KEY,

    schema_version  VARCHAR(20)   NOT NULL,

    event_id        VARCHAR(128)  NOT NULL UNIQUE,
    transaction_id  VARCHAR(128)  NOT NULL,

    source          VARCHAR(100)  NOT NULL,
    event_type      VARCHAR(100)  NOT NULL,

    customer_id     VARCHAR(128)  NULL,

    amount_minor    BIGINT        NOT NULL,
    currency        CHAR(3)       NOT NULL,

    merchant        VARCHAR(200)  NULL,
    payment_method  VARCHAR(30)   NULL,

    country_code    CHAR(2)       NOT NULL,
    status          VARCHAR(30)   NOT NULL,

    occurred_at     TIMESTAMPTZ   NOT NULL,
    received_at     TIMESTAMPTZ   NOT NULL,
    processed_at    TIMESTAMPTZ   NOT NULL,

    metadata        JSONB         NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT transactions_amount_minor_nonneg
        CHECK (amount_minor >= 0),

    CONSTRAINT transactions_currency_upper
        CHECK (currency = UPPER(currency)),

    CONSTRAINT transactions_country_upper
        CHECK (country_code = UPPER(country_code)),

    CONSTRAINT transactions_status_enum
        CHECK (status IN ('PENDING','SUCCESS','FAILED','REFUNDED'))
);

-- Indexes justified by V1 query patterns only.
CREATE INDEX idx_transactions_transaction_id
    ON transactions (transaction_id);

CREATE INDEX idx_transactions_customer_time
    ON transactions (customer_id, occurred_at);

CREATE INDEX idx_transactions_occurred_at
    ON transactions (occurred_at);


-- ----------------------------------------------------------------------------
-- processed_events
-- ----------------------------------------------------------------------------
-- Persistent deduplication / processing state machine.
--
-- The application performs the atomic claim via:
--
--   INSERT INTO processed_events
--       (event_id, payload_hash, source, status, last_attempt_at)
--   VALUES
--       ($1, $2, $3, 'PROCESSING', NOW())
--   ON CONFLICT (event_id) DO NOTHING
--   RETURNING event_id;
--
-- This table only needs to support that operation correctly.
--
-- processed_at must be non-null only when status = 'SUCCEEDED'
-- and must be null for all non-SUCCEEDED states.
-- ----------------------------------------------------------------------------
CREATE TABLE processed_events (
    event_id         VARCHAR(128)  PRIMARY KEY,
    payload_hash     CHAR(64)      NOT NULL,
    source           VARCHAR(100)  NOT NULL,

    status           VARCHAR(30)   NOT NULL,

    attempt_count    INTEGER       NOT NULL DEFAULT 1,

    last_attempt_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ   NULL,

    CONSTRAINT processed_events_status_enum
        CHECK (status IN
               ('PROCESSING','SUCCEEDED','FAILED_RETRYABLE','FAILED_PERMANENT')),

    CONSTRAINT processed_events_attempt_count_positive
        CHECK (attempt_count > 0),

    CONSTRAINT processed_events_processed_at_consistency
        CHECK (
            (status =  'SUCCEEDED' AND processed_at IS NOT NULL) OR
            (status <> 'SUCCEEDED' AND processed_at IS NULL)
        )
);

CREATE INDEX idx_processed_events_status
    ON processed_events (status);

CREATE INDEX idx_processed_events_last_attempt
    ON processed_events (last_attempt_at);


-- ----------------------------------------------------------------------------
-- anomaly_alerts
-- ----------------------------------------------------------------------------
-- Detections of suspicious / anomalous patterns. These are NOT confirmed
-- fraud verdicts; they are signals produced by V1 rules.
--
-- Deliberately decoupled from transactions: an alert may exist even when a
-- corresponding transaction row does not (no foreign keys in V1).
-- ----------------------------------------------------------------------------
CREATE TABLE anomaly_alerts (
    id              BIGSERIAL     PRIMARY KEY,

    transaction_id  VARCHAR(128)  NOT NULL,
    event_id        VARCHAR(128)  NULL,
    customer_id     VARCHAR(128)  NULL,

    rule_code       VARCHAR(100)  NOT NULL,
    severity        VARCHAR(20)   NOT NULL,

    description     TEXT          NULL,

    detected_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    details         JSONB         NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT anomaly_alerts_severity_enum
        CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL'))
);

CREATE INDEX idx_anomaly_alerts_customer
    ON anomaly_alerts (customer_id);

CREATE INDEX idx_anomaly_alerts_transaction_id
    ON anomaly_alerts (transaction_id);

CREATE INDEX idx_anomaly_alerts_rule_code
    ON anomaly_alerts (rule_code);

CREATE INDEX idx_anomaly_alerts_detected_at
    ON anomaly_alerts (detected_at DESC);


-- ----------------------------------------------------------------------------
-- processing_errors
-- ----------------------------------------------------------------------------
-- Queryable operational visibility for processing failures.
--
-- This is NOT the durable failed-event store. Kafka topic
-- transactions.failed.v1 remains the source of truth for failed events and
-- replay. This table exists so that operational questions such as
-- "how many events failed by error_type in the last hour?" are easy to answer.
--
-- No foreign keys in V1: errors may reference events that never produced a
-- transaction row, and must remain decoupled from retry/recovery workflows.
-- ----------------------------------------------------------------------------
CREATE TABLE processing_errors (
    id              BIGSERIAL     PRIMARY KEY,

    event_id        VARCHAR(128)  NULL,
    source          VARCHAR(100)  NULL,

    error_type      VARCHAR(100)  NOT NULL,
    error_message   TEXT          NULL,
    failure_stage   VARCHAR(50)   NULL,

    retry_count     INTEGER       NULL,

    occurred_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    metadata        JSONB         NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_processing_errors_error_type
    ON processing_errors (error_type);

CREATE INDEX idx_processing_errors_occurred_at
    ON processing_errors (occurred_at DESC);

CREATE INDEX idx_processing_errors_event_id
    ON processing_errors (event_id);

CREATE INDEX idx_processing_errors_source
    ON processing_errors (source);