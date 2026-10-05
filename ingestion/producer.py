import json
import logging
import os
from typing import Any

from confluent_kafka import Producer

logger = logging.getLogger("ingestion.producer")


class KafkaProducerWrapper:
    """Thin synchronous wrapper around confluent_kafka.Producer.

    Publishes one event per call and blocks until delivery is confirmed or
    the delivery timeout expires. Callers get an exception on failure so the
    HTTP layer can return 503 rather than silently drop the event.
    """

    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        )
        self.topic = os.getenv("KAFKA_RAW_TOPIC", "transactions.raw.v1")
        self.client_id = os.getenv("KAFKA_CLIENT_ID", "fastapi-ingestor")
        self.delivery_timeout = float(
            os.getenv("KAFKA_DELIVERY_TIMEOUT_SECONDS", "5")
        )

        self._producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": self.client_id,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
            }
        )

        logger.info(
            "Kafka producer configured: bootstrap=%s topic=%s client_id=%s",
            self.bootstrap_servers,
            self.topic,
            self.client_id,
        )

    def publish(self, key: str, value: dict[str, Any]) -> None:
        state: dict[str, Any] = {"error": None, "delivered": False}

        def _callback(err, _msg) -> None:
            if err is not None:
                state["error"] = err
            else:
                state["delivered"] = True

        self._producer.produce(
            topic=self.topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
            callback=_callback,
        )
        self._producer.flush(self.delivery_timeout)

        if state["error"] is not None:
            raise RuntimeError(f"Kafka delivery failed: {state['error']}")
        if not state["delivered"]:
            raise RuntimeError(
                f"Kafka delivery not confirmed within {self.delivery_timeout}s"
            )

    def close(self) -> None:
        try:
            self._producer.flush(self.delivery_timeout)
        except Exception:
            logger.exception("Error flushing producer during shutdown")
        finally:
            self._producer = None  # type: ignore[assignment]