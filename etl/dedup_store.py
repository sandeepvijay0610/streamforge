import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger("etl.dedup")

CLAIM_LEASE_SECONDS = 60


class DedupStore(ABC):
    """Abstract dedup registry.

    V1 implementation uses PostgreSQL. V2 can swap for Redis without
    touching the consumer.
    """

    @abstractmethod
    def claim(self, event_id: str, payload_hash: str, source: str) -> bool:
        """Atomically claim an unseen event_id. Returns True if we own it."""

    @abstractmethod
    def get(self, event_id: str) -> Optional[dict[str, Any]]:
        """Return the current registry row, or None."""

    @abstractmethod
    def reclaim(self, event_id: str) -> bool:
        """Atomically re-claim an event whose lease has expired or that
        previously failed retryably. Returns True if we now own it."""

    @abstractmethod
    def mark_succeeded(self, event_id: str) -> None: ...

    @abstractmethod
    def mark_permanent_failure(self, event_id: str) -> None: ...

    @abstractmethod
    def mark_retryable_failure(self, event_id: str) -> None: ...


class PostgresDedupStore(DedupStore):
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn = None

    def _connect(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = True
        return self._conn

    def claim(self, event_id: str, payload_hash: str, source: str) -> bool:
        with self._connect().cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_events
                    (event_id, payload_hash, source, status, last_attempt_at)
                VALUES
                    (%s, %s, %s, 'PROCESSING', NOW())
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                (event_id, payload_hash, source),
            )
            return cur.fetchone() is not None

    def get(self, event_id: str) -> Optional[dict[str, Any]]:
        with self._connect().cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(
                """
                SELECT event_id, payload_hash, source, status,
                       attempt_count, last_attempt_at, processed_at
                FROM processed_events
                WHERE event_id = %s
                """,
                (event_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def reclaim(self, event_id: str) -> bool:
        """Reclaim if: FAILED_RETRYABLE, or PROCESSING with expired lease."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=CLAIM_LEASE_SECONDS)
        with self._connect().cursor() as cur:
            cur.execute(
                """
                UPDATE processed_events
                SET status = 'PROCESSING',
                    attempt_count = attempt_count + 1,
                    last_attempt_at = NOW(),
                    processed_at = NULL
                WHERE event_id = %s
                  AND (
                       status = 'FAILED_RETRYABLE'
                    OR (status = 'PROCESSING' AND last_attempt_at < %s)
                  )
                RETURNING event_id
                """,
                (event_id, cutoff),
            )
            return cur.fetchone() is not None

    def mark_succeeded(self, event_id: str) -> None:
        with self._connect().cursor() as cur:
            cur.execute(
                """
                UPDATE processed_events
                SET status = 'SUCCEEDED',
                    processed_at = NOW(),
                    last_attempt_at = NOW()
                WHERE event_id = %s
                """,
                (event_id,),
            )

    def mark_permanent_failure(self, event_id: str) -> None:
        with self._connect().cursor() as cur:
            cur.execute(
                """
                UPDATE processed_events
                SET status = 'FAILED_PERMANENT',
                    processed_at = NULL,
                    last_attempt_at = NOW()
                WHERE event_id = %s
                """,
                (event_id,),
            )

    def mark_retryable_failure(self, event_id: str) -> None:
        with self._connect().cursor() as cur:
            cur.execute(
                """
                UPDATE processed_events
                SET status = 'FAILED_RETRYABLE',
                    processed_at = NULL,
                    last_attempt_at = NOW()
                WHERE event_id = %s
                """,
                (event_id,),
            )