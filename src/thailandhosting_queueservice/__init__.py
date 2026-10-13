"""Python SDK for ThailandHosting Queue Service.

    from thailandhosting_queueservice import Client

    client = Client(
        access_key_id=os.environ["TH_QUEUE_ACCESS_KEY_ID"],
        secret_access_key=os.environ["TH_QUEUE_SECRET_ACCESS_KEY"],
        region="th-bkk-1",
    )
    jobs = client.queue("jobs")
    jobs.send_json({"order_id": 42})

    # A worker: receive with long polling, handle, delete.
    jobs.process(lambda m: handle(m.json()), concurrency=4)

Delivery is at least once: a message is handed out again when it is not
deleted before its visibility timeout ends, so handlers should be safe to
run twice. The client is thread-safe and should be reused.
"""

from .client import (
    DEFAULT_ENDPOINT,
    MAX_BATCH,
    MAX_WAIT,
    Client,
    DeleteResult,
    Message,
    Outgoing,
    Queue,
    QueueInfo,
    QueueStats,
    content_digest,
    sign_request,
)
from .errors import NotFoundError, QueueServiceError

__version__ = "1.0.0"

__all__ = [
    "DEFAULT_ENDPOINT",
    "MAX_BATCH",
    "MAX_WAIT",
    "Client",
    "DeleteResult",
    "Message",
    "NotFoundError",
    "Outgoing",
    "Queue",
    "QueueInfo",
    "QueueServiceError",
    "QueueStats",
    "content_digest",
    "sign_request",
]
