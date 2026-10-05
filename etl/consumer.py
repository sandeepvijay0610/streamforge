import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from pydantic import ValidationError

from adapters.router import get_adapter
from canonical import CanonicalTransaction
from dedup_store import PostgresDedupStore
from errors import PermanentEventError, TransientInfrastructureError
from normalizers import (
    normalize_amount_minor,
    normalize_country,
    normalize_currency,
    normalize_merchant,
    normalize_payment_method,
    normalize_timestamp,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("etl")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "transactions.raw.v1")
CLEANED_TOPIC = os.getenv("KAFKA_CLEANED_TOPIC", "transactions.cleaned.v1")
FAILED_TOPIC = os.getenv("KAFKA_FAILED_TOPIC", "transactions.failed.v1")
RETRY_TOPIC = os.getenv("KAFKA_RETRY_TOPIC", "transactions.retry.v1")
GROUP_ID = os.getenv("ETL_GROUP_ID", "streamforge-etl-v1")
PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=postgres port=5432 dbname=streamforge user=streamforge password=streamforge",
)

_shutdown = {"flag": False}


def _handle_signal(signum, _frame):
    logger.info("Received signal %s, shutting down", signum)
    _shutdown["flag"] = True


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def compute_payload_hash(envelope: dict[str, Any]) -> str:
    """Hash source + event_type + payload. NOT received_at."""
    parts = json.dumps(
        {
            "source": envelope["source"],
            "event_type": envelope["event_type"],
            "payload": envelope["payload"],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def build_canonical(envelope: dict[str, Any]) -> CanonicalTransaction:
    adapter = get_adapter(envelope["source"])
    mapped = adapter.transform(envelope)

    # Cross-check: adapter's interpretation vs raw event_type.
    event_type = mapped.get("event_type_override") or envelope["event_type"]

    status_by_event = {
        "payment.created": "PENDING",
        "payment.succeeded": "SUCCESS",
        "payment.failed": "FAILED",
        "payment.refunded": "REFUNDED",
    }
    canonical_status = status_by_event.get(event_type)
    if canonical_status is None:
        raise PermanentEventError(
            "TRANSFORMATION_ERROR",
            f"Unknown canonical event_type: {event_type!r}",
        )

    try:
        canonical = CanonicalTransaction(
            schema_version=envelope["schema_version"],
            event_id=envelope["event_id"],
            transaction_id=str(mapped["transaction_id"]),
            source=envelope["source"],
            event_type=event_type,
            customer_id=str(mapped["customer_id"]),
            amount_minor=normalize_amount_minor(mapped["amount_raw"]),
            currency=normalize_currency(mapped["currency_raw"]),
            merchant=normalize_merchant(mapped.get("merchant_raw")),
            payment_method=normalize_payment_method(mapped.get("payment_method_raw")),
            country_code=normalize_country(mapped["country_raw"]),
            status=canonical_status,
            occurred_at=normalize_timestamp(
                mapped["occurred_at_raw"], default_tz=adapter.default_tz
            ),
            received_at=normalize_timestamp(envelope["received_at"]),
            processed_at=_utc_now(),
            metadata={},
        )
    except ValidationError as e:
        raise PermanentEventError(
            "INVALID_SCHEMA", f"Canonical validation failed: {e}", details=str(e)
        )

    # Guard against provider-declared status contradicting the canonical event_type.
    declared = mapped.get("declared_status")
    if declared and declared != canonical_status:
        raise PermanentEventError(
            "EVENT_STATUS_CONFLICT",
            f"event_type={event_type} implies {canonical_status}, "
            f"provider declared {declared}",
        )

    return canonical


def publish(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"),
    )
    producer.poll(0)


def failed_envelope(
    envelope: dict[str, Any], err: PermanentEventError
) -> dict[str, Any]:
    return {
        "schema_version": envelope.get("schema_version", "1.0"),
        "event_id": envelope.get("event_id"),
        "source": envelope.get("source"),
        "failed_at": _utc_now_iso(),
        "error_type": err.error_type,
        "error_message": err.message,
        "original_event": envelope,
    }


def retry_envelope(
    envelope: dict[str, Any], err: TransientInfrastructureError
) -> dict[str, Any]:
    return {
        "schema_version": envelope.get("schema_version", "1.0"),
        "event_id": envelope.get("event_id"),
        "source": envelope.get("source"),
        "failed_at": _utc_now_iso(),
        "error_type": err.error_type,
        "error_message": err.message,
        "original_event": envelope,
    }


def handle_message(
    envelope: dict[str, Any],
    dedup: PostgresDedupStore,
    producer: Producer,
) -> None:
    event_id = envelope.get("event_id")
    if not event_id:
        # No dedup key possible. Send straight to failed.
        err = PermanentEventError("MISSING_REQUIRED_FIELD", "Missing event_id")
        publish(producer, FAILED_TOPIC, key="unknown", value=failed_envelope(envelope, err))
        return

    source = envelope.get("source", "unknown")
    payload_hash = compute_payload_hash(envelope)

    # --- Dedup state machine ---------------------------------------------
    if not dedup.claim(event_id, payload_hash, source):
        existing = dedup.get(event_id)
        if existing is None:
            # Extremely unlikely; race with a delete. Treat as retry.
            raise TransientInfrastructureError(
                "DATABASE_TIMEOUT", "Registry row vanished after claim failure"
            )

        if existing["payload_hash"] != payload_hash:
            err = PermanentEventError(
                "EVENT_ID_CONFLICT",
                f"event_id {event_id} already exists with a different payload",
            )
            publish(producer, FAILED_TOPIC, key=event_id, value=failed_envelope(envelope, err))
            return

        status = existing["status"]
        if status == "SUCCEEDED":
            logger.info("exact duplicate ignored event_id=%s", event_id)
            return
        if status == "FAILED_PERMANENT":
            logger.warning("dropping previously-permanent-failed event_id=%s", event_id)
            return
        if status == "FAILED_RETRYABLE":
            if not dedup.reclaim(event_id):
                logger.info("retryable lost reclaim race event_id=%s", event_id)
                return
        elif status == "PROCESSING":
            if not dedup.reclaim(event_id):
                logger.info("still leased by another worker event_id=%s", event_id)
                return
        else:
            logger.error("unknown registry status %r event_id=%s", status, event_id)
            return

    # --- We own the event_id. Process. -----------------------------------
    try:
        canonical = build_canonical(envelope)
    except PermanentEventError as e:
        logger.warning("permanent failure event_id=%s type=%s", event_id, e.error_type)
        publish(producer, FAILED_TOPIC, key=event_id, value=failed_envelope(envelope, e))
        dedup.mark_permanent_failure(event_id)
        return
    except Exception as e:
        logger.exception("unexpected transformation error event_id=%s", event_id)
        err = PermanentEventError("TRANSFORMATION_ERROR", str(e))
        publish(producer, FAILED_TOPIC, key=event_id, value=failed_envelope(envelope, err))
        dedup.mark_permanent_failure(event_id)
        return

    try:
        publish(
            producer,
            CLEANED_TOPIC,
            key=canonical.customer_id,
            value=canonical.model_dump(mode="json"),
        )
    except Exception as e:
        logger.exception("kafka publish to cleaned failed event_id=%s", event_id)
        err = TransientInfrastructureError("KAFKA_PUBLISH_ERROR", str(e))
        publish(producer, RETRY_TOPIC, key=event_id, value=retry_envelope(envelope, err))
        dedup.mark_retryable_failure(event_id)
        return

    dedup.mark_succeeded(event_id)
    logger.info(
        "cleaned event_id=%s customer=%s amount_minor=%s",
        canonical.event_id,
        canonical.customer_id,
        canonical.amount_minor,
    )


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    dedup = PostgresDedupStore(PG_DSN)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "etl-consumer-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        }
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "client.id": "etl-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([RAW_TOPIC])

    logger.info(
        "ETL consumer started group=%s topic=%s -> cleaned=%s failed=%s retry=%s",
        GROUP_ID,
        RAW_TOPIC,
        CLEANED_TOPIC,
        FAILED_TOPIC,
        RETRY_TOPIC,
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
                logger.exception("invalid JSON on topic")
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                handle_message(envelope, dedup, producer)
            except TransientInfrastructureError as e:
                # The dedup store itself is unreachable — cannot even mark state.
                # Route the original envelope to retry and commit so we don't spin.
                logger.warning("transient error event_id=%s type=%s", envelope.get("event_id"), e.error_type)
                publish(
                    producer,
                    RETRY_TOPIC,
                    key=str(envelope.get("event_id", "unknown")),
                    value=retry_envelope(envelope, e),
                )
            except Exception:
                # Bugs only. Crash the consumer so the container restarts
                # and the offset stays uncommitted.
                logger.exception("fatal error handling message")
                raise

            producer.flush(5.0)
            consumer.commit(message=msg, asynchronous=False)

    finally:
        logger.info("shutting down consumer")
        consumer.close()
        try:
            producer.flush(5.0)
        except Exception:
            logger.exception("producer flush on shutdown")

    return 0


if __name__ == "__main__":
    sys.exit(main())