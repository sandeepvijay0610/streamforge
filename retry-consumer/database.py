import logging
import time
from typing import Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger("db-consumer.database")

# Error types the driver raises for transient failures (safe to retry).
TRANSIENT_PG_SQLSTATES = {
    "08000",  # connection_exception
    "08003",  # connection_does_not_exist
    "08006",  # connection_failure
    "08001",  # sqlclient_unable_to_establish_sqlconnection
    "08004",  # sqlserver_rejected_establishment_of_sqlconnection
    "57P01",  # admin_shutdown
    "57P02",  # crash_shutdown
    "57P03",  # cannot_connect_now
    "40001",  # serialization_failure
    "40P01",  # deadlock_detected
}


class TransientDatabaseError(Exception):
    pass


class PermanentDatabaseError(Exception):
    pass


class Database:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn = None

    def _connect(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = False
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                logger.exception("error closing connection")

    def insert_transaction(self, canonical: dict[str, Any]) -> None:
        """Insert one canonical transaction.

        Raises TransientDatabaseError for retryable failures,
        PermanentDatabaseError for anything else.
        """
        sql = """
            INSERT INTO transactions (
                schema_version,
                event_id, transaction_id,
                source, event_type,
                customer_id,
                amount_minor, currency,
                merchant, payment_method,
                country_code, status,
                occurred_at, received_at, processed_at,
                metadata
            ) VALUES (
                %(schema_version)s,
                %(event_id)s, %(transaction_id)s,
                %(source)s, %(event_type)s,
                %(customer_id)s,
                %(amount_minor)s, %(currency)s,
                %(merchant)s, %(payment_method)s,
                %(country_code)s, %(status)s,
                %(occurred_at)s, %(received_at)s, %(processed_at)s,
                %(metadata)s
            )
            ON CONFLICT (event_id) DO NOTHING
        """
        params = dict(canonical)
        params.setdefault("metadata", {})
        params["metadata"] = psycopg2.extras.Json(params["metadata"])

        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
        except psycopg2.Error as e:
            pgcode = e.pgcode or ""
            try:
                if self._conn is not None and not self._conn.closed:
                    self._conn.rollback()
            except Exception:
                logger.exception("rollback failed")

            if pgcode in TRANSIENT_PG_SQLSTATES:
                raise TransientDatabaseError(
                    f"transient pg error {pgcode}: {e}"
                ) from e

            # Connection object is likely dead; force reconnect next time.
            try:
                if self._conn is not None:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None

            if not pgcode:
                # No SQLSTATE means the driver couldn't even talk to the server.
                raise TransientDatabaseError(f"no sqlstate, treating as transient: {e}") from e

            raise PermanentDatabaseError(
                f"permanent pg error {pgcode}: {e}"
            ) from e