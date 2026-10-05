from abc import ABC, abstractmethod
from datetime import timezone
from typing import Any, Optional


class TransactionAdapter(ABC):
    """Provider-specific transformation into a canonical-shaped dict.

    Adapters MUST NOT mutate the raw envelope or its payload. They read
    values and return a new dict. Normalization (currency, country, etc.)
    happens in normalizers, not here — the adapter's only job is field
    mapping and provider-specific semantic interpretation.
    """

    source_name: str = ""
    default_tz: Optional[timezone] = None

    @abstractmethod
    def transform(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Return a partial canonical dict (raw values, unmapped names OK).

        The consumer will run this through normalizers and validate against
        CanonicalTransaction. The adapter should raise PermanentEventError
        for provider-specific semantic failures (e.g. an event type that
        cannot be mapped into StreamForge semantics).
        """
        raise NotImplementedError