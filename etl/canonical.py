from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

Status = Literal["PENDING", "SUCCESS", "FAILED", "REFUNDED"]
PaymentMethod = Literal["CARD", "UPI", "WALLET", "BANK_TRANSFER", "OTHER"]


class CanonicalTransaction(BaseModel):
    """The one canonical shape that all downstream consumers depend on."""

    schema_version: str
    event_id: str
    transaction_id: str
    source: str
    event_type: str

    customer_id: str

    amount_minor: int = Field(..., ge=0)
    currency: str = Field(..., min_length=3, max_length=3)

    merchant: Optional[str] = None
    payment_method: Optional[PaymentMethod] = None

    country_code: str = Field(..., min_length=2, max_length=2)
    status: Status

    occurred_at: datetime
    received_at: datetime
    processed_at: datetime

    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("currency")
    @classmethod
    def _currency_upper(cls, v: str) -> str:
        if v != v.upper():
            raise ValueError("currency must be uppercase")
        return v

    @field_validator("country_code")
    @classmethod
    def _country_upper(cls, v: str) -> str:
        if v != v.upper():
            raise ValueError("country_code must be uppercase")
        return v