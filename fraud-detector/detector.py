import logging
import os
import signal
import sys
import time

import psycopg2

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("fraud-detector")

PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=postgres port=5432 dbname=streamforge user=streamforge password=streamforge",
)
SCAN_INTERVAL_SECONDS = int(os.getenv("FRAUD_SCAN_INTERVAL_SECONDS", "30"))

_shutdown = {"flag": False}


def _handle_signal(signum, _frame):
    logger.info("Received signal %s, shutting down", signum)
    _shutdown["flag"] = True


# ---------------------------------------------------------------------------
# Rule 1: RAPID_COUNTRY_CHANGE
# ---------------------------------------------------------------------------
# For the same customer, if the current successful transaction's country
# differs from the previous successful transaction's country and the two
# transactions are within 5 minutes, raise a HIGH severity alert.
# ---------------------------------------------------------------------------
RAPID_COUNTRY_CHANGE_SQL = """
    INSERT INTO anomaly_alerts (
        transaction_id, event_id, customer_id,
        rule_code, severity, description, details
    )
    SELECT
        o.transaction_id,
        o.event_id,
        o.customer_id,
        'RAPID_COUNTRY_CHANGE',
        'HIGH',
        'Customer changed country from ' || o.prev_country
            || ' to ' || o.country_code
            || ' within ' || EXTRACT(EPOCH FROM (o.occurred_at - o.prev_occurred_at))::int
            || ' seconds',
        jsonb_build_object(
            'previous_country', o.prev_country,
            'current_country', o.country_code,
            'previous_transaction_id', o.prev_transaction_id,
            'previous_occurred_at', o.prev_occurred_at,
            'seconds_between', EXTRACT(EPOCH FROM (o.occurred_at - o.prev_occurred_at))::int
        )
    FROM (
        SELECT
            event_id,
            transaction_id,
            customer_id,
            country_code,
            occurred_at,
            LAG(country_code) OVER w     AS prev_country,
            LAG(occurred_at) OVER w      AS prev_occurred_at,
            LAG(transaction_id) OVER w   AS prev_transaction_id
        FROM transactions
        WHERE customer_id IS NOT NULL
          AND status = 'SUCCESS'
        WINDOW w AS (PARTITION BY customer_id ORDER BY occurred_at, event_id)
    ) AS o
    WHERE o.prev_country IS NOT NULL
      AND o.prev_country <> o.country_code
      AND o.occurred_at - o.prev_occurred_at <= INTERVAL '5 minutes'
    ON CONFLICT (rule_code, event_id) DO NOTHING;
"""


# ---------------------------------------------------------------------------
# Rule 2: SPENDING_SPIKE
# ---------------------------------------------------------------------------
# Flag a successful transaction when its amount is at least 5x the customer's
# prior average. Only applies when the customer has at least 5 prior
# successful transactions. Prior average is computed over the previous 30
# successful transactions (or all available if fewer).
# ---------------------------------------------------------------------------
SPENDING_SPIKE_SQL = """
    INSERT INTO anomaly_alerts (
        transaction_id, event_id, customer_id,
        rule_code, severity, description, details
    )
    SELECT
        s.transaction_id,
        s.event_id,
        s.customer_id,
        'SPENDING_SPIKE',
        'MEDIUM',
        'Amount ' || s.amount_minor || ' exceeds 5x prior average '
            || ROUND(s.prior_avg)::bigint,
        jsonb_build_object(
            'current_amount_minor', s.amount_minor,
            'prior_average_minor', ROUND(s.prior_avg)::bigint,
            'prior_count', s.prior_count,
            'multiplier', ROUND(s.amount_minor / NULLIF(s.prior_avg, 0), 2)
        )
    FROM (
        SELECT
            event_id,
            transaction_id,
            customer_id,
            amount_minor,
            AVG(amount_minor) OVER w AS prior_avg,
            COUNT(*) OVER w          AS prior_count
        FROM transactions
        WHERE customer_id IS NOT NULL
          AND status = 'SUCCESS'
        WINDOW w AS (
            PARTITION BY customer_id
            ORDER BY occurred_at, event_id
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        )
    ) AS s
    WHERE s.prior_count >= 5
      AND s.prior_avg IS NOT NULL
      AND s.prior_avg > 0
      AND s.amount_minor >= 5 * s.prior_avg
    ON CONFLICT (rule_code, event_id) DO NOTHING;
"""


def run_scan(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(RAPID_COUNTRY_CHANGE_SQL)
        country_count = cur.rowcount

        cur.execute(SPENDING_SPIKE_SQL)
        spike_count = cur.rowcount

    conn.commit()
    return country_count, spike_count


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    conn = None
    logger.info("Fraud detector started, scan interval=%ss", SCAN_INTERVAL_SECONDS)

    while not _shutdown["flag"]:
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(PG_DSN)
                conn.autocommit = False
                logger.info("connected to postgres")

            country_count, spike_count = run_scan(conn)

            if country_count or spike_count:
                logger.info(
                    "scan: %d RAPID_COUNTRY_CHANGE, %d SPENDING_SPIKE",
                    country_count,
                    spike_count,
                )

        except Exception:
            logger.exception("scan failed")
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None

        # Sleep responsively so SIGTERM is handled quickly.
        for _ in range(SCAN_INTERVAL_SECONDS):
            if _shutdown["flag"]:
                break
            time.sleep(1)

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    logger.info("fraud detector shut down")
    return 0


if __name__ == "__main__":
    sys.exit(main())