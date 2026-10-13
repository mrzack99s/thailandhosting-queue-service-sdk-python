from __future__ import annotations

from typing import Optional


class QueueServiceError(Exception):
    """An error answer from the service (a Problem Details document, RFC 9457).

    ``code`` is the machine-readable reason, for example ``queue_not_found``,
    ``queue_full``, ``invalid_receipt`` or ``busy``; ``message`` is the
    document's ``title``.
    """

    def __init__(self, status: int, code: str, message: str, type: Optional[str] = None, instance: Optional[str] = None) -> None:
        super().__init__(f"{code} (HTTP {status}): {message}")
        self.status = status
        self.code = code
        self.message = message
        self.type = type
        self.instance = instance


class NotFoundError(QueueServiceError):
    """The queue does not exist, or the key may not reach it (``queue_not_found``)."""
