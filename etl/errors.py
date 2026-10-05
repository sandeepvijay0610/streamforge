from typing import Any


class PermanentEventError(Exception):
    """Bad data. The event itself is invalid; it will never succeed.

    Routes to transactions.failed.v1 and marks the registry FAILED_PERMANENT.
    """

    def __init__(self, error_type: str, message: str, **context: Any) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.message = message
        self.context = context


class TransientInfrastructureError(Exception):
    """Infrastructure problem. The event is fine; the world isn't.

    Routes to transactions.retry.v1 and marks the registry FAILED_RETRYABLE.
    """

    def __init__(self, error_type: str, message: str, **context: Any) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.message = message
        self.context = context