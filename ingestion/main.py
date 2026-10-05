import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

from producer import KafkaProducerWrapper

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("ingestion")

SCHEMA_VERSION = "1.0"

ALLOWED_PROVIDERS = {"provider-a", "provider-b"}

ALLOWED_EVENT_TYPES = {
    "payment.created",
    "payment.succeeded",
    "payment.failed",
    "payment.refunded",
}


class WebhookBody(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=128)
    event_type: str = Field(..., min_length=1, max_length=100)
    payload: dict[str, Any]


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.producer = KafkaProducerWrapper()
    logger.info("Ingestion service ready")
    try:
        yield
    finally:
        app.state.producer.close()
        logger.info("Ingestion service shut down")


app = FastAPI(
    title="StreamForge Ingestion API",
    version="1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/v1/webhooks/{provider}",
    status_code=status.HTTP_202_ACCEPTED,
)
def receive_webhook(
    provider: str,
    body: WebhookBody,
    request: Request,
) -> dict[str, str]:
    if provider not in ALLOWED_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown provider: {provider}",
        )

    if body.event_type not in ALLOWED_EVENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported event_type: {body.event_type}",
        )

    envelope = {
        "schema_version": SCHEMA_VERSION,
        "event_id": body.event_id,
        "source": provider,
        "event_type": body.event_type,
        "received_at": _utc_now_iso(),
        "payload": body.payload,
    }

    producer: KafkaProducerWrapper = request.app.state.producer
    try:
        producer.publish(key=body.event_id, value=envelope)
    except Exception:
        logger.exception("Kafka publish failed for event_id=%s", body.event_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Downstream publish failure",
        )

    logger.info(
        "accepted event_id=%s source=%s event_type=%s",
        body.event_id,
        provider,
        body.event_type,
    )

    return {"event_id": body.event_id, "status": "accepted"}