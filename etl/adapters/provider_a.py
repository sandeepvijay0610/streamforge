from datetime import timezone
from typing import Any

from errors import PermanentEventError

from .base import TransactionAdapter

IST = timezone(__import__("datetime").timedelta(hours=5, minutes=30))


class ProviderAAdapter(TransactionAdapter):
    source_name = "provider-a"
    default_tz = IST

    # Provider A uses freeform status strings; map them to event-type semantics.
    _STATUS_TO_EVENT_TYPE = {
        "success": "payment.succeeded",
        "completed": "payment.succeeded",
        "paid": "payment.succeeded",
        "ok": "payment.succeeded",
        "settled": "payment.succeeded",
        "failed": "payment.failed",
        "declined": "payment.failed",
        "refunded": "payment.refunded",
        "pending": "payment.created",
    }

    def transform(self, envelope: dict[str, Any]) -> dict[str, Any]:
        p = envelope["payload"]

        raw_status = str(p.get("result") or p.get("status") or "").strip().lower()
        event_type = self._STATUS_TO_EVENT_TYPE.get(raw_status)
        if event_type is None:
            raise PermanentEventError(
                "TRANSFORMATION_ERROR",
                f"Provider A: unmappable status {raw_status!r}",
                raw_status=raw_status,
            )

        return {
            "transaction_id": p.get("id"),
            "customer_id": p.get("customer"),
            "amount_raw": p.get("value"),
            "currency_raw": p.get("currency_code"),
            "merchant_raw": p.get("merchant_name"),
            "payment_method_raw": p.get("method"),
            "country_raw": p.get("country"),
            "occurred_at_raw": p.get("created"),
            "event_type_override": event_type,
        }