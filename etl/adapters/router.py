from typing import Any

from errors import PermanentEventError

from .base import TransactionAdapter
from .provider_a import ProviderAAdapter
from .provider_b import ProviderBAdapter

_ADAPTERS: dict[str, TransactionAdapter] = {
    "provider-a": ProviderAAdapter(),
    "provider-b": ProviderBAdapter(),
}


def get_adapter(source: str) -> TransactionAdapter:
    adapter = _ADAPTERS.get(source)
    if adapter is None:
        raise PermanentEventError(
            "UNKNOWN_SOURCE", f"No adapter registered for source {source!r}", source=source
        )
    return adapter