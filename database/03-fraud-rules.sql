-- ============================================================================
-- StreamForge V1 — Fraud / Anomaly Rule Support
-- ============================================================================
-- Loaded via /docker-entrypoint-initdb.d/03-fraud-rules.sql (fresh DB init only).
-- For an already-initialized database, apply manually:
--   docker compose exec -T postgres psql -U streamforge -d streamforge \
--     < database/03-fraud-rules.sql
-- ============================================================================
-- Postgres treats NULL as distinct in unique indexes, so rows with NULL
-- event_id are unaffected by this constraint. This keeps the index
-- compatible with `ON CONFLICT (rule_code, event_id) DO NOTHING`.
-- ============================================================================

DROP INDEX IF EXISTS anomaly_alerts_rule_event_unique;

CREATE UNIQUE INDEX anomaly_alerts_rule_event_unique
    ON anomaly_alerts (rule_code, event_id);