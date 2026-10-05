import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from database import (
    Database,
    PermanentDatabaseError,
    TransientDatabaseError,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("db-consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CLEANED_TOPIC = os.getenv("KAFKA_CLEANED_TOPIC", "transactions.cleaned.v1")
RETRY_TOPIC = os.getenv("KAFKA_RETRY_TOPIC", "transactions.retry.v1")
FAILED_TOPIC = os.getenv("KAFKA_FAILED_TOPIC", "transactions.failed.v1")
GROUP_ID = os.getenv("DB_CONSUMER_GROUP_ID", "streamforge-db-consumer-v1")
PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=postgres port=5432 dbname=streamforge user=streamforge password=streamforge",
)

# 3 retries after the initial attempt, exponential backoff: 1s, 2s, 4s.
RETRY_BACKOFF_SECONDS = [1, 2, 4]
# If the DB has been down for so long we've retried this long in-process,
# give up and route to the retry topic (a dedicated retry worker will pick it up).
MAX_INLINE_RETRIES = 3

_shutdown = {"flag": False}


def _handle_signal(signum, _frame):
    logger.info("Received signal %s, shutting down", signum)
    _shutdown["flag"] = True


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _parse_dt(value: Any) -> Any:
    """JSON dates come back as strings; psycopg2 will coerce them for
    TIMESTAMPTZ columns, but we normalize to datetime for safety."""
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def normalize_canonical_for_db(canonical: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": canonical["schema_version"],
        "event_id": canonical["event_id"],
        "transaction_id": canonical["transaction_id"],
        "source": canonical["source"],
        "event_type": canonical["event_type"],
        "customer_id": canonical.get("customer_id"),
        "amount_minor": canonical["amount_minor"],
        "currency": canonical["currency"],
        "merchant": canonical.get("merchant"),
        "payment_method": canonical.get("payment_method"),
        "country_code": canonical["country_code"],
        "status": canonical["status"],
        "occurred_at": _parse_dt(canonical["occurred_at"]),
        "received_at": _parse_dt(canonical["received_at"]),
        "processed_at": _parse_dt(canonical["processed_at"]),
        "metadata": canonical.get("metadata") or {},
    }


def build_retry_envelope(
    canonical: dict[str, Any],
    error_type: str,
    error_message: str,
    retry_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": canonical.get("schema_version", "1.0"),
        "event_id": canonical.get("event_id"),
        "source": canonical.get("source"),
        "failed_at": _utc_now_iso(),
        "error_type": error_type,
        "error_message": error_message,
        "failure_stage": "PERSISTENCE",
        "retry_count": retry_count,
        "original_event": canonical,
    }


def build_failed_envelope(
    canonical: dict[str, Any],
    error_type: str,
    error_message: str,
) -> dict[str, Any]:
    return {
        "schema_version": canonical.get("schema_version", "1.0"),
        "event_id": canonical.get("event_id"),
        "source": canonical.get("source"),
        "failed_at": _utc_now_iso(),
        "error_type": error_type,
        "error_message": error_message,
        "failure_stage": "PERSISTENCE",
        "original_event": canonical,
    }


def publish(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"),
    )
    producer.poll(0)


def persist_with_retries(
    db: Database,
    canonical_row: dict[str, Any],
) -> tuple[bool, str, str]:
    """Attempt to persist. Returns (success, error_type, error_message).

    Blocks the consumer thread during backoff. Only runs for transient errors
    — permanent errors short-circuit immediately.
    """
    last_err_type = "DATABASE_UNAVAILABLE"
    last_err_msg = "unknown"

    for attempt in range(MAX_INLINE_RETRIES + 1):
        try:
            db.insert_transaction(canonical_row)
            return True, "", ""
        except TransientDatabaseError as e:
            last_err_type = "DATABASE_UNAVAILABLE"
            last_err_msg = str(e)
            logger.warning(
                "transient db error attempt=%s/%s event_id=%s: %s",
                attempt + 1,
                MAX_INLINE_RETRIES + 1,
                canonical_row.get("event_id"),
                e,
            )
            if attempt < MAX_INLINE_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS[attempt])
                continue
            return False, last_err_type, last_err_msg
        except PermanentDatabaseError as e:
            logger.error(
                "permanent db error event_id=%s: %s",
                canonical_row.get("event_id"),
                e,
            )
            return False, "DATABASE_ERROR", str(e)

    return False, last_err_type, last_err_msg


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    db = Database(PG_DSN)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "db-consumer-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        }
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "client.id": "db-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([CLEANED_TOPIC])

    logger.info(
        "DB consumer started group=%s topic=%s -> retry=%s failed=%s",
        GROUP_ID,
        CLEANED_TOPIC,
        RETRY_TOPIC,
        FAILED_TOPIC,
    )

    try:
        while not _shutdown["flag"]:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("consumer error: %s", msg.error())
                continue

            try:
                canonical = json.loads(msg.value().decode("utf-8"))
            except Exception:
                logger.exception("invalid JSON on cleaned topic; skipping")
                consumer.commit(message=msg, asynchronous=False)
                continue

            event_id = str(canonical.get("event_id", "unknown"))

            try:
                row = normalize_canonical_for_db(canonical)
            except (KeyError, ValueError) as e:
                logger.error("malformed canonical event_id=%s: %s", event_id, e)
                publish(
                    producer,
                    FAILED_TOPIC,
                    key=event_id,
                    value=build_failed_envelope(
                        canonical, "INVALID_SCHEMA", f"malformed canonical: {e}"
                    ),
                )
                producer.flush(5.0)
                consumer.commit(message=msg, asynchronous=False)
                continue

            success, err_type, err_msg = persist_with_retries(db, row)

            if success:
                logger.info(
                    "persisted event_id=%s customer=%s amount_minor=%s",
                    row["event_id"],
                    row.get("customer_id"),
                    row["amount_minor"],
                )
            else:
                if err_type == "DATABASE_ERROR":
                    publish(
                        producer,
                        FAILED_TOPIC,
                        key=event_id,
                        value=build_failed_envelope(canonical, err_type, err_msg),
                    )
                else:
                    publish(
                        producer,
                        RETRY_TOPIC,
                        key=event_id,
                        value=build_retry_envelope(
                            canonical, err_type, err_msg, MAX_INLINE_RETRIES
                        ),
                    )

            producer.flush(5.0)
            consumer.commit(message=msg, asynchronous=False)

    finally:
        logger.info("shutting down db consumer")
        consumer.close()
        db.close()
        try:
            producer.flush(5.0)
        except Exception:
            logger.exception("producer flush on shutdown")

    return 0


if __name__ == "__main__":
    sys.exit(main())