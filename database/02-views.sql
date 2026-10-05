-- ============================================================================
-- StreamForge V1 — Analytical Views
-- ============================================================================
-- Loaded via /docker-entrypoint-initdb.d/02-views.sql (fresh DB init only).
-- For an already-initialized database, apply manually:
--   docker compose exec -T postgres psql -U streamforge -d streamforge \
--     < database/02-views.sql
-- ============================================================================


-- ----------------------------------------------------------------------------
-- v_transaction_volume_per_minute
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_transaction_volume_per_minute AS
SELECT
    DATE_TRUNC('minute', occurred_at)                            AS minute,
    COUNT(*)                                                     AS transaction_count,
    COUNT(*) FILTER (WHERE status = 'SUCCESS')                   AS success_count,
    COUNT(*) FILTER (WHERE status = 'FAILED')                    AS failed_count,
    COALESCE(SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'), 0)
                                                                 AS successful_amount_minor
FROM transactions
GROUP BY DATE_TRUNC('minute', occurred_at);


-- ----------------------------------------------------------------------------
-- v_success_rate_by_payment_method
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_success_rate_by_payment_method AS
SELECT
    payment_method,
    COUNT(*)                                                     AS total_transactions,
    COUNT(*) FILTER (WHERE status = 'SUCCESS')                   AS successful_transactions,
    ROUND(
        (COUNT(*) FILTER (WHERE status = 'SUCCESS')::numeric
         / NULLIF(COUNT(*), 0)) * 100,
        2
    )                                                            AS success_rate_pct,
    COALESCE(SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'), 0)
                                                                 AS successful_amount_minor
FROM transactions
WHERE payment_method IS NOT NULL
GROUP BY payment_method;


-- ----------------------------------------------------------------------------
-- v_merchant_ranking
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_merchant_ranking AS
SELECT
    merchant,
    COALESCE(SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'), 0)
                                                                 AS total_volume_minor,
    COUNT(*)                                                     AS total_transactions,
    COUNT(*) FILTER (WHERE status = 'SUCCESS')                   AS successful_transactions,
    RANK() OVER (
        ORDER BY COALESCE(SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'), 0) DESC
    )                                                            AS volume_rank
FROM transactions
WHERE merchant IS NOT NULL
GROUP BY merchant;


-- ----------------------------------------------------------------------------
-- v_rolling_transaction_volume
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_rolling_transaction_volume AS
WITH per_minute AS (
    SELECT
        DATE_TRUNC('minute', occurred_at)                        AS minute,
        COUNT(*)                                                 AS tx_count,
        COALESCE(SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'), 0)
                                                                 AS amount_minor
    FROM transactions
    GROUP BY DATE_TRUNC('minute', occurred_at)
)
SELECT
    minute,
    tx_count,
    amount_minor,
    SUM(tx_count) OVER (
        ORDER BY minute
        RANGE BETWEEN INTERVAL '14 minutes' PRECEDING AND CURRENT ROW
    )                                                            AS rolling_15m_tx_count,
    SUM(amount_minor) OVER (
        ORDER BY minute
        RANGE BETWEEN INTERVAL '14 minutes' PRECEDING AND CURRENT ROW
    )                                                            AS rolling_15m_amount_minor
FROM per_minute;


-- ----------------------------------------------------------------------------
-- v_customer_activity_profile
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_customer_activity_profile AS
SELECT
    customer_id,
    COUNT(*)                                                     AS transaction_count,
    COUNT(*) FILTER (WHERE status = 'SUCCESS')                   AS successful_count,
    COUNT(*) FILTER (WHERE status = 'FAILED')                    AS failed_count,
    ROUND(AVG(amount_minor) FILTER (WHERE status = 'SUCCESS'))   AS avg_successful_amount_minor,
    MAX(amount_minor) FILTER (WHERE status = 'SUCCESS')          AS max_successful_amount_minor,
    MAX(occurred_at)                                             AS latest_transaction_at,
    COUNT(DISTINCT country_code)                                 AS countries_used
FROM transactions
WHERE customer_id IS NOT NULL
GROUP BY customer_id;