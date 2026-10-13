# ThailandHosting Queue Service — Python SDK

Python client for ThailandHosting Queue Service: message queues that take
slow work off your site or app. One side sends jobs, workers receive them,
and nothing is lost when a worker fails.

```
pip install thailandhosting-queue-service
```

Python 3.9+. One dependency: `urllib3` (connection pooling).

## Quick start

```python
import os
from thailandhosting_queueservice import Client, Outgoing

client = Client(
    access_key_id=os.environ["TH_QUEUE_ACCESS_KEY_ID"],
    secret_access_key=os.environ["TH_QUEUE_SECRET_ACCESS_KEY"],
    region="th-bkk-1",
)
jobs = client.queue("jobs")

# Send
jobs.send_json({"order_id": 42})
jobs.send("plain text", attributes={"kind": "email"}, delay=30)
jobs.send_batch(["a", "b", Outgoing("c", {"x": "1"})])

# A worker: long-polls, runs the handler in threads, deletes what it handled.
def handle(m):
    ship(m.json()["order_id"])  # raising leaves the message to be retried

jobs.process(handle, concurrency=8, retry_delay=30)  # runs until stop= (a threading.Event) is set
```

Create access keys in the Console (Queue Service → Access keys). Reuse one
`Client`: it is thread-safe and keeps connections open.

## Receiving by hand

```python
for m in jobs.receive(max=10, wait=20):
    try:
        handle(m.body)
    except Exception:
        continue  # handed out again after the visibility timeout
    jobs.delete(m.receipt)
```

- `wait` long-polls up to 20 seconds for a first message, so an idle worker
  makes few requests.
- A received message is hidden for the queue's visibility timeout (or
  `visibility_timeout`). Delete it with its receipt once handled; otherwise
  it is handed out again. `m.receives` counts the deliveries.
- `change_visibility(receipt, seconds)` keeps a message hidden longer while
  you work on it, or with 0 releases it at once.
- `delete_batch(receipts)` deletes up to 10 and reports each one in order.

Delivery is **at least once**: make handlers safe to run twice (check an
order ID before charging it, for example).

## Queues

```python
q = client.put_queue("jobs", visibility_timeout=120, max_receives=5, dead_letter_queue="jobs-dlq")
print(q.created)

client.list_queues()
info = client.get_queue("jobs")  # info.messages: visible, in_flight, delayed, oldest_age
jobs.purge()
client.delete_queue("jobs")
```

`put_queue` creates a queue or changes the given settings of an existing one
(the key must be allowed to manage queues). After `max_receives` deliveries
a message moves to the dead-letter queue (create it first), or is dropped
without one.

| Setting (seconds) | Default | Range |
| --- | --- | --- |
| `visibility_timeout` | 30 | 0 – 43200 |
| `retention` | 345600 (4 days) | 60 – 1209600 |
| `delay` | 0 | 0 – 900 |
| `max_receives` | 0 (never move) | 0 – 1000 |

Limits: a message is at most 256 KiB (body and up to 10 attributes), a batch
10 messages and 1 MiB.

## Errors

Error answers raise `QueueServiceError` (`NotFoundError` for 404) with the
service's `code`:

```python
from thailandhosting_queueservice import NotFoundError

try:
    client.get_queue("missing")
except NotFoundError as e:
    print(e.code)  # queue_not_found
```

Common codes: `queue_not_found`, `forbidden`, `invalid_receipt`,
`queue_full`, `too_many_in_flight`, `too_large`, `project_suspended`,
`wrong_region`, `busy`. Busy or unreachable answers are retried with
exponential backoff (`max_retries`, default 4).

## Endpoint and region

The default endpoint is `https://queues.thailandhosting.com`. Set `region`
to the queues' region; without one the service finds each queue's region.
`endpoint` may also be given with the region already in it:
`https://queues.thailandhosting.com/th-bkk-1`.

## Authentication

Every request is signed with HTTP Message Signatures (RFC 9421,
HMAC-SHA256 with the secret access key) over the method, host, path, query
and the body's Content-Digest (RFC 9530). The secret is never sent.
`sign_request` is exported for calling the API without the client.

## Tests

```
python -m unittest discover -s tests
```

Set `TH_QUEUE_TEST_ENDPOINT`, `TH_QUEUE_TEST_REGION`, `TH_QUEUE_TEST_KEY` and
`TH_QUEUE_TEST_SECRET` to also run the round trip against a real endpoint
(the key must manage queues; the test creates and deletes its own).

## License

MIT
