-- ============================================================================
-- StreamForge V1 — Synthetic seed data for analytics verification
-- ============================================================================
-- Dev-only. Apply manually:
--   docker compose exec -T postgres psql -U streamforge -d streamforge \
--     < database/seed.sql
-- ============================================================================
-- ~5% of rows get an inflated amount so that SPENDING_SPIKE can trigger.
-- Without this, uniform amounts never exceed 5x the prior average.
-- ============================================================================

INSERT INTO transactions (
    schema_version, event_id, transaction_id, source, event_type,
    customer_id, amount_minor, currency, merchant, payment_method,
    country_code, status, occurred_at, received_at, processed_at
)
SELECT
    '1.0',
    'evt_seed_' || g,
    'txn_seed_' || g,
    (ARRAY['provider-a','provider-b'])[1 + floor(random() * 2)::int],
    'payment.succeeded',
    'usr_' || floor(random() * 25)::int,
    CASE
        WHEN random() < 0.05
            THEN (3000000 + floor(random() * 7000000))::bigint   -- 30k-100k INR spike
        ELSE (10000 + floor(random() * 490000))::bigint
    END,
    'INR',
    (ARRAY['AMAZON','FLIPKART','SWIGGY','ZOMATO','MYNTRA'])[1 + floor(random() * 5)::int],
    (ARRAY['UPI','CARD','WALLET','BANK_TRANSFER'])[1 + floor(random() * 4)::int],
    (ARRAY['IN','IN','IN','SG','AE'])[1 + floor(random() * 5)::int],
    CASE floor(random() * 10)
        WHEN 0 THEN 'FAILED'
        WHEN 1 THEN 'FAILED'
        WHEN 9 THEN 'REFUNDED'
        ELSE 'SUCCESS'
    END,
    NOW() - (g || ' minutes')::interval,
    NOW() - (g || ' minutes')::interval,
    NOW() - (g || ' minutes')::interval
FROM generate_series(1, 500) AS g
ON CONFLICT (event_id) DO NOTHING;