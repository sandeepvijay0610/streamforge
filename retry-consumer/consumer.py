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
logger = logging.getLogger("retry-consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
RETRY_TOPIC = os.getenv("KAFKA_RETRY_TOPIC", "transactions.retry.v1")
FAILED_TOPIC = os.getenv("KAFKA_FAILED_TOPIC", "transactions.failed.v1")
GROUP_ID = os.getenv("RETRY_CONSUMER_GROUP_ID", "streamforge-retry-consumer-v1")
PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=postgres port=5432 dbname=streamforge user=streamforge password=streamforge",
)

RETRY_BACKOFF_SECONDS = [1, 2, 4]

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


def build_failed_envelope(
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
        "failure_stage": "PERSISTENCE_RETRY_EXHAUSTED",
        "retry_count": retry_count,
        "original_event": canonical,
    }


def publish(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"),
    )
    producer.poll(0)


def try_persist(
    db: Database, canonical_row: dict[str, Any]
) -> tuple[bool, str, str]:
    """One pass with inline backoff. Returns (success, error_type, error_message)."""
    for attempt in range(len(RETRY_BACKOFF_SECONDS) + 1):
        try:
            db.insert_transaction(canonical_row)
            return True, "", ""
        except TransientDatabaseError as e:
            logger.warning(
                "retry attempt=%s/%s event_id=%s: %s",
                attempt + 1,
                len(RETRY_BACKOFF_SECONDS) + 1,
                canonical_row.get("event_id"),
                e,
            )
            if attempt < len(RETRY_BACKOFF_SECONDS):
                time.sleep(RETRY_BACKOFF_SECONDS[attempt])
                continue
            return False, "DATABASE_UNAVAILABLE", str(e)
        except PermanentDatabaseError as e:
            logger.error(
                "permanent db error event_id=%s: %s",
                canonical_row.get("event_id"),
                e,
            )
            return False, "DATABASE_ERROR", str(e)

    return False, "DATABASE_UNAVAILABLE", "retries exhausted"


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    db = Database(PG_DSN)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "retry-consumer-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        }
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "client.id": "retry-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([RETRY_TOPIC])

    logger.info(
        "Retry consumer started group=%s topic=%s -> failed=%s",
        GROUP_ID,
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
                envelope = json.loads(msg.value().decode("utf-8"))
            except Exception:
                logger.exception("invalid JSON on retry topic; skipping")
                consumer.commit(message=msg, asynchronous=False)
                continue

            event_id = str(envelope.get("event_id", "unknown"))
            original = envelope.get("original_event")
            if not isinstance(original, dict):
                logger.error("retry envelope missing original_event event_id=%s", event_id)
                publish(
                    producer,
                    FAILED_TOPIC,
                    key=event_id,
                    value=build_failed_envelope(
                        envelope,
                        "INVALID_SCHEMA",
                        "retry envelope missing original_event",
                        0,
                    ),
                )
                producer.flush(5.0)
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                row = normalize_canonical_for_db(original)
            except (KeyError, ValueError) as e:
                logger.error("malformed canonical in retry event_id=%s: %s", event_id, e)
                publish(
                    producer,
                    FAILED_TOPIC,
                    key=event_id,
                    value=build_failed_envelope(
                        original, "INVALID_SCHEMA", f"malformed canonical: {e}", 0
                    ),
                )
                producer.flush(5.0)
                consumer.commit(message=msg, asynchronous=False)
                continue

            prior_retry_count = int(envelope.get("retry_count") or 0)
            success, err_type, err_msg = try_persist(db, row)

            if success:
                logger.info(
                    "retry succeeded event_id=%s customer=%s prior_retries=%s",
                    event_id,
                    row.get("customer_id"),
                    prior_retry_count,
                )
            else:
                logger.error(
                    "retry exhausted event_id=%s type=%s prior_retries=%s",
                    event_id,
                    err_type,
                    prior_retry_count,
                )
                publish(
                    producer,
                    FAILED_TOPIC,
                    key=event_id,
                    value=build_failed_envelope(
                        original,
                        err_type,
                        err_msg,
                        prior_retry_count + len(RETRY_BACKOFF_SECONDS),
                    ),
                )

            producer.flush(5.0)
            consumer.commit(message=msg, asynchronous=False)

    finally:
        logger.info("shutting down retry consumer")
        consumer.close()
        db.close()
        try:
            producer.flush(5.0)
        except Exception:
            logger.exception("producer flush on shutdown")

    return 0


if __name__ == "__main__":
    sys.exit(main())