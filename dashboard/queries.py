import os
from contextlib import contextmanager

import pandas as pd
import psycopg2

PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=postgres port=5432 dbname=streamforge user=streamforge password=streamforge",
)


@contextmanager
def _cursor():
    conn = psycopg2.connect(PG_DSN)
    try:
        yield conn.cursor()
    finally:
        conn.close()


def _df(sql: str, params=None) -> pd.DataFrame:
    with _cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# --- KPIs ----------------------------------------------------------------

def kpi_totals() -> dict:
    df = _df("""
        SELECT
            COUNT(*)                                                AS total_transactions,
            COUNT(*) FILTER (WHERE status = 'SUCCESS')              AS successful_transactions,
            COUNT(*) FILTER (WHERE status = 'FAILED')               AS failed_transactions,
            COALESCE(SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'), 0)
                                                                    AS successful_amount_minor,
            COUNT(DISTINCT customer_id)                             AS active_customers
        FROM transactions
    """)
    row = df.iloc[0].to_dict()
    total = int(row["total_transactions"] or 0)
    ok = int(row["successful_transactions"] or 0)
    row["success_rate_pct"] = round(ok * 100.0 / total, 2) if total else 0.0
    row["total_transactions"] = total
    row["successful_transactions"] = ok
    row["failed_transactions"] = int(row["failed_transactions"] or 0)
    row["active_customers"] = int(row["active_customers"] or 0)
    row["successful_amount_minor"] = int(row["successful_amount_minor"] or 0)
    return row


def kpi_alert_counts() -> dict:
    df = _df("""
        SELECT
            COUNT(*)                                                AS total_alerts,
            COUNT(*) FILTER (WHERE severity = 'HIGH')               AS high_alerts
        FROM anomaly_alerts
    """)
    row = df.iloc[0].to_dict()
    row["total_alerts"] = int(row["total_alerts"] or 0)
    row["high_alerts"] = int(row["high_alerts"] or 0)
    return row


# --- Charts --------------------------------------------------------------

def recent_volume_per_minute(limit: int = 60) -> pd.DataFrame:
    return _df("""
        SELECT minute, transaction_count, success_count, failed_count,
               successful_amount_minor
        FROM v_transaction_volume_per_minute
        ORDER BY minute DESC
        LIMIT %s
    """, (limit,)).iloc[::-1].reset_index(drop=True)


def merchant_ranking() -> pd.DataFrame:
    return _df("""
        SELECT merchant, total_volume_minor, total_transactions,
               successful_transactions, volume_rank
        FROM v_merchant_ranking
        ORDER BY volume_rank
    """)


def payment_method_breakdown() -> pd.DataFrame:
    return _df("""
        SELECT payment_method, total_transactions, success_rate_pct
        FROM v_success_rate_by_payment_method
        ORDER BY total_transactions DESC
    """)


def status_breakdown() -> pd.DataFrame:
    return _df("""
        SELECT status, COUNT(*) AS cnt
        FROM transactions
        GROUP BY status
    """)


# --- Feeds ---------------------------------------------------------------

def live_transaction_feed(limit: int = 25) -> pd.DataFrame:
    return _df("""
        SELECT
            occurred_at, customer_id, merchant, amount_minor, currency, status
        FROM transactions
        ORDER BY occurred_at DESC
        LIMIT %s
    """, (limit,))


def recent_alerts(limit: int = 50) -> pd.DataFrame:
    return _df("""
        SELECT
            detected_at, rule_code, severity, customer_id,
            transaction_id, description
        FROM anomaly_alerts
        ORDER BY detected_at DESC
        LIMIT %s
    """, (limit,))