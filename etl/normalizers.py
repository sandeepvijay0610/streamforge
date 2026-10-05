from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import pycountry
from dateutil import parser as dateutil_parser

from errors import PermanentEventError

SUPPORTED_CURRENCIES = {"INR", "USD", "EUR", "GBP", "SGD"}

CURRENCY_ALIASES = {
    "inr": "INR", "₹": "INR", "rupees": "INR", "rs": "INR", "rs.": "INR",
    "usd": "USD", "$": "USD", "dollars": "USD",
    "eur": "EUR", "€": "EUR",
    "gbp": "GBP", "£": "GBP",
    "sgd": "SGD", "s$": "SGD",
}

MERCHANT_ALIASES = {
    "amazon": "AMAZON",
    "amazon_india": "AMAZON",
    "amzn": "AMAZON",
    "flipkart": "FLIPKART",
    "swiggy": "SWIGGY",
    "zomato": "ZOMATO",
}

PAYMENT_METHOD_ALIASES = {
    "upi": "UPI", "gpay": "UPI", "google_pay": "UPI", "googlepay": "UPI",
    "phonepe": "UPI", "paytm": "UPI",
    "card": "CARD", "credit_card": "CARD", "debit_card": "CARD",
    "visa": "CARD", "mastercard": "CARD", "amex": "CARD",
    "wallet": "WALLET", "paypal": "WALLET", "amazon_pay": "WALLET",
    "bank": "BANK_TRANSFER", "bank_transfer": "BANK_TRANSFER",
    "net_banking": "BANK_TRANSFER", "neft": "BANK_TRANSFER", "imps": "BANK_TRANSFER",
}


def _require(value: Any, field: str) -> Any:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        raise PermanentEventError(
            "MISSING_REQUIRED_FIELD", f"Missing required field: {field}", field=field
        )
    return value


def normalize_currency(raw: Any) -> str:
    _require(raw, "currency")
    key = str(raw).strip()
    canonical = CURRENCY_ALIASES.get(key) or CURRENCY_ALIASES.get(key.lower())
    if canonical is None:
        upper = key.upper()
        if upper in SUPPORTED_CURRENCIES:
            canonical = upper
        else:
            raise PermanentEventError(
                "UNSUPPORTED_CURRENCY", f"Unsupported currency: {raw!r}", raw=raw
            )
    return canonical


def normalize_country(raw: Any) -> str:
    _require(raw, "country")
    key = str(raw).strip()

    if len(key) == 2:
        c = pycountry.countries.get(alpha_2=key.upper())
        if c:
            return c.alpha_2
    if len(key) == 3:
        c = pycountry.countries.get(alpha_3=key.upper())
        if c:
            return c.alpha_2
    try:
        c = pycountry.countries.lookup(key)
        return c.alpha_2
    except LookupError:
        raise PermanentEventError(
            "UNSUPPORTED_COUNTRY", f"Unsupported country: {raw!r}", raw=raw
        )


def normalize_merchant(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    key = str(raw).strip().lower()
    if key == "":
        return None
    return MERCHANT_ALIASES.get(key, key.upper())


def normalize_payment_method(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    key = str(raw).strip().lower().replace("-", "_").replace(" ", "_")
    if key == "":
        return None
    return PAYMENT_METHOD_ALIASES.get(key, "OTHER")


def normalize_amount_minor(raw: Any) -> int:
    _require(raw, "amount")
    try:
        d = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        raise PermanentEventError(
            "INVALID_AMOUNT", f"Cannot parse amount: {raw!r}", raw=raw
        )
    if d < 0:
        raise PermanentEventError(
            "INVALID_AMOUNT", f"Negative amount: {raw!r}", raw=raw
        )
    # V1: 2-decimal currencies only. Reject sub-cent precision explicitly.
    if d != d.quantize(Decimal("0.01")):
        raise PermanentEventError(
            "INVALID_AMOUNT",
            f"More than 2 decimal places (V1 limit): {raw!r}",
            raw=raw,
        )
    return int(d * 100)


def normalize_timestamp(raw: Any, default_tz: Optional[timezone] = None) -> datetime:
    _require(raw, "timestamp")

    # Epoch (numeric or numeric-as-string)
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        except (ValueError, OSError):
            raise PermanentEventError(
                "INVALID_TIMESTAMP", f"Invalid epoch: {raw!r}", raw=raw
            )

    s = str(raw).strip()
    try:
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except ValueError:
        pass

    try:
        dt = dateutil_parser.parse(s)
    except (ValueError, TypeError, OverflowError):
        raise PermanentEventError(
            "INVALID_TIMESTAMP", f"Cannot parse timestamp: {raw!r}", raw=raw
        )

    if dt.tzinfo is None:
        if default_tz is None:
            raise PermanentEventError(
                "INVALID_TIMESTAMP",
                f"Naive timestamp with no default timezone: {raw!r}",
                raw=raw,
            )
        dt = dt.replace(tzinfo=default_tz)

    return dt.astimezone(timezone.utc)