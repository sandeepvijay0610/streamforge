from datetime import timezone
from typing import Any

from errors import PermanentEventError

from .base import TransactionAdapter


class ProviderBAdapter(TransactionAdapter):
    source_name = "provider-b"
    default_tz = timezone.utc  # Provider B sends UTC or tz-aware

    _STATUS_TO_EVENT_TYPE = {
        "success": "payment.succeeded",
        "failed": "payment.failed",
        "refunded": "payment.refunded",
        "pending": "payment.created",
    }

    def transform(self, envelope: dict[str, Any]) -> dict[str, Any]:
        p = envelope["payload"]

        raw_status = str(p.get("status") or "").strip().lower()
        event_type = self._STATUS_TO_EVENT_TYPE.get(raw_status)
        if event_type is None:
            raise PermanentEventError(
                "TRANSFORMATION_ERROR",
                f"Provider B: unmappable status {raw_status!r}",
                raw_status=raw_status,
            )

        return {
            "transaction_id": p.get("transaction_id"),
            "customer_id": p.get("user_id"),
            "amount_raw": p.get("amount"),
            "currency_raw": p.get("currency"),
            "merchant_raw": p.get("merchant"),
            "payment_method_raw": p.get("payment_method"),
            "country_raw": p.get("country_code"),
            "occurred_at_raw": p.get("timestamp"),
            "event_type_override": event_type,
        }