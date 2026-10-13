from __future__ import annotations

import base64
import hashlib
import hmac
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import quote, urlsplit

import urllib3

from .errors import NotFoundError, QueueServiceError

__all__ = ["Client", "Queue", "QueueInfo", "QueueStats", "Message", "DeleteResult", "Outgoing", "DEFAULT_ENDPOINT", "MAX_BATCH", "MAX_WAIT"]

VERSION = "1.0.0"
DEFAULT_ENDPOINT = "https://queues.thailandhosting.com"
MAX_BATCH = 10  # messages per send, receive or delete
MAX_WAIT = 20  # seconds a receive may wait for a first message
_RETRY_STATUSES = {502, 503, 504}
_PERMANENT_STATUSES = {400, 401, 403, 404, 421}


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        v = value.replace("Z", "+00:00")
        # Python < 3.11 accepts at most 6 fractional digits.
        if "." in v:
            head, rest = v.split(".", 1)
            frac = "".join(ch for ch in rest if ch.isdigit())
            v = head + "." + frac[:6] + rest[len(frac):]
        return datetime.fromisoformat(v)
    except ValueError:
        return None


@dataclass
class QueueStats:
    visible: int = 0  # ready to be received
    in_flight: int = 0  # received, not yet deleted
    delayed: int = 0  # sent with a delay still running
    bytes: int = 0
    oldest_age: int = 0  # seconds

    @classmethod
    def _from(cls, d: Mapping[str, Any]) -> "QueueStats":
        return cls(int(d.get("visible", 0)), int(d.get("inFlight", 0)), int(d.get("delayed", 0)), int(d.get("bytes", 0)), int(d.get("oldestAge", 0)))


@dataclass
class QueueInfo:
    name: str
    region: str = ""
    created_at: Optional[datetime] = None
    visibility_timeout: int = 0
    retention: int = 0
    delay: int = 0
    max_receives: int = 0
    dead_letter_queue: str = ""
    # The backlog (get_queue and Queue.info only).
    messages: Optional[QueueStats] = None
    # put_queue only: whether the queue was created rather than changed.
    created: bool = False

    @classmethod
    def _from(cls, d: Mapping[str, Any], created: bool = False) -> "QueueInfo":
        return cls(
            name=d.get("name", ""),
            region=d.get("region", ""),
            created_at=_parse_time(d.get("createdAt")),
            visibility_timeout=int(d.get("visibilityTimeout", 0)),
            retention=int(d.get("retention", 0)),
            delay=int(d.get("delay", 0)),
            max_receives=int(d.get("maxReceives", 0)),
            dead_letter_queue=d.get("deadLetterQueue", "") or "",
            messages=QueueStats._from(d["messages"]) if d.get("messages") else None,
            created=created,
        )


@dataclass
class Message:
    """A received message. Delete it with its ``receipt`` once handled."""

    id: str
    receipt: str
    body: str
    attributes: Dict[str, str] = field(default_factory=dict)
    sent_at: Optional[datetime] = None
    # How many times the message was handed out, this one included.
    receives: int = 0
    first_received_at: Optional[datetime] = None

    def json(self) -> Any:
        """The body parsed as JSON."""
        return json.loads(self.body)

    @classmethod
    def _from(cls, d: Mapping[str, Any]) -> "Message":
        return cls(
            id=d.get("id", ""),
            receipt=d.get("receipt", ""),
            body=d.get("body", "") or "",
            attributes=dict(d.get("attributes") or {}),
            sent_at=_parse_time(d.get("sentAt")),
            receives=int(d.get("receives", 0)),
            first_received_at=_parse_time(d.get("firstReceivedAt")),
        )


@dataclass
class DeleteResult:
    receipt: str
    deleted: bool
    # "invalid_receipt" or "unavailable" when not deleted.
    error: str = ""


@dataclass
class Outgoing:
    """One message of a batch."""

    body: str
    attributes: Optional[Dict[str, str]] = None
    delay: int = 0  # seconds


# ---- signing: HTTP Message Signatures (RFC 9421) ----


def content_digest(body: bytes) -> str:
    """The Content-Digest (RFC 9530) of a body: ``sha-256=:<base64>:``."""
    return "sha-256=:" + base64.b64encode(hashlib.sha256(body).digest()).decode() + ":"


def _sf_string(s: str) -> str:
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def sign_request(method: str, url: str, key_id: str, secret: str, digest: Optional[str] = None, created: Optional[int] = None) -> Dict[str, str]:
    """The Signature-Input and Signature headers of one request (HMAC-SHA256
    keyed with the secret, which itself is never sent). ``url`` is the full
    URL exactly as sent; ``digest`` the Content-Digest of a body."""
    u = urlsplit(url)
    authority = u.netloc.lower()
    for port in (":443", ":80"):
        if authority.endswith(port):
            authority = authority[: -len(port)]
    covered = '"@method" "@authority" "@path" "@query"' + (' "content-digest"' if digest is not None else "")
    params = f"({covered});created={int(time.time() if created is None else created)};keyid={_sf_string(key_id)};alg=\"hmac-sha256\""
    lines = [
        f'"@method": {method.upper()}',
        f'"@authority": {authority}',
        f'"@path": {u.path or "/"}',
        f'"@query": ?{u.query}',
    ]
    if digest is not None:
        lines.append(f'"content-digest": {digest}')
    lines.append(f'"@signature-params": {params}')
    mac = hmac.new(secret.encode(), "\n".join(lines).encode(), hashlib.sha256).digest()
    return {"Signature-Input": "th=" + params, "Signature": "th=:" + base64.b64encode(mac).decode() + ":"}


def _backoff(n: int) -> float:
    """The pause before retry n: exponential from 0.1 s, capped at 5 s, with jitter."""
    d = min(0.1 * (2 ** min(n, 6)), 5.0)
    return d / 2 + random.random() * d / 2


class Client:
    """Talks to Queue Service with one access key.

    The client is thread-safe and should be reused: it keeps connections
    open. Busy or unreachable answers are retried with backoff.
    """

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        region: Optional[str] = None,
        endpoint: str = DEFAULT_ENDPOINT,
        max_retries: int = 4,
        timeout: float = 60.0,
        user_agent: Optional[str] = None,
    ) -> None:
        """``region`` (e.g. ``th-bkk-1``) is appended to ``endpoint``; without
        one the service finds each queue's region itself. ``timeout`` must be
        longer than the longest receive wait (20 s)."""
        if not access_key_id or not secret_access_key:
            raise ValueError("access_key_id and secret_access_key are required")
        u = urlsplit((endpoint or DEFAULT_ENDPOINT).rstrip("/"))
        if u.scheme not in ("http", "https") or not u.netloc:
            raise ValueError(f"invalid endpoint {endpoint!r}")
        path = u.path.rstrip("/")
        if path.endswith("/v1"):
            path = path[:-3]
        if region:
            path += "/" + quote(region, safe="")
        self.base_url = f"{u.scheme}://{u.netloc}{path}/v1"
        self._key_id = access_key_id
        self._secret = secret_access_key
        self._retries = max(0, max_retries)
        self._ua = f"thailandhosting-queue-service-sdk-python/{VERSION}" + (f" {user_agent}" if user_agent else "")
        self._http = urllib3.PoolManager(num_pools=4, maxsize=32, retries=False, timeout=urllib3.Timeout(connect=10.0, read=timeout))

    def _call(self, method: str, path: str, payload: Any = None) -> Tuple[int, Any]:
        url = self.base_url + path
        body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode()
        attempt = 0
        while True:
            headers = {"User-Agent": self._ua, "Accept": "application/json"}
            digest = None
            if body is not None:
                digest = content_digest(body)
                headers["Content-Type"] = "application/json"
                headers["Content-Digest"] = digest
            headers.update(sign_request(method, url, self._key_id, self._secret, digest))
            try:
                resp = self._http.request(method, url, body=body, headers=headers, redirect=False)
            except urllib3.exceptions.HTTPError:
                if attempt >= self._retries:
                    raise
                time.sleep(_backoff(attempt))
                attempt += 1
                continue
            if resp.status >= 300:
                if resp.status in _RETRY_STATUSES and attempt < self._retries:
                    after = resp.headers.get("Retry-After", "")
                    time.sleep(min(int(after), 60) if after.isdigit() and int(after) > 0 else _backoff(attempt))
                    attempt += 1
                    continue
                raise self._error(resp)
            data = resp.data
            return resp.status, (json.loads(data) if data else None)

    @staticmethod
    def _error(resp: urllib3.BaseHTTPResponse) -> QueueServiceError:
        try:
            p = json.loads(resp.data or b"{}")
            if not isinstance(p, dict):
                p = {}
        except ValueError:
            p = {}
        code = p.get("code") or resp.headers.get("X-Error-Code") or f"http_{resp.status}"
        message = p.get("title") or (resp.data or b"").decode(errors="replace").strip() or str(resp.status)
        cls = NotFoundError if resp.status == 404 else QueueServiceError
        return cls(resp.status, code, message, p.get("type"), p.get("instance"))

    def list_queues(self) -> List[QueueInfo]:
        """The queues the key can reach, by name."""
        _, data = self._call("GET", "/queues")
        return [QueueInfo._from(q) for q in (data or {}).get("queues") or []]

    def put_queue(
        self,
        name: str,
        visibility_timeout: Optional[int] = None,
        retention: Optional[int] = None,
        delay: Optional[int] = None,
        max_receives: Optional[int] = None,
        dead_letter_queue: Optional[str] = None,
    ) -> QueueInfo:
        """Creates a queue, or changes the given settings of the one by that
        name (``.created`` tells which; the key must manage queues). Settings
        are whole seconds; left out, they keep their value.

        visibility_timeout 0–43200 (default 30), retention 60–1209600 (default
        4 days), delay 0–900, max_receives 0 (never move) to 1000,
        dead_letter_queue a queue of the same Project ("" for none).
        """
        settings = {
            "visibilityTimeout": visibility_timeout,
            "retention": retention,
            "delay": delay,
            "maxReceives": max_receives,
            "deadLetterQueue": dead_letter_queue,
        }
        status, data = self._call("PUT", "/queues/" + quote(name, safe=""), {k: v for k, v in settings.items() if v is not None})
        return QueueInfo._from(data, created=status == 201)

    def get_queue(self, name: str) -> QueueInfo:
        """A queue's settings and backlog."""
        _, data = self._call("GET", "/queues/" + quote(name, safe=""))
        return QueueInfo._from(data)

    def delete_queue(self, name: str) -> None:
        """Deletes a queue with every message in it."""
        self._call("DELETE", "/queues/" + quote(name, safe=""))

    def queue(self, name: str) -> "Queue":
        """A handle for sending and receiving on one queue (it makes no call by itself)."""
        return Queue(self, name)

    def close(self) -> None:
        self._http.clear()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class Queue:
    """Sends and receives on one queue."""

    def __init__(self, client: Client, name: str) -> None:
        self._c = client
        self.name = name
        self._path = "/queues/" + quote(name, safe="")

    def info(self) -> QueueInfo:
        """The queue's settings and backlog."""
        return self._c.get_queue(self.name)

    def send(self, body: str, attributes: Optional[Mapping[str, str]] = None, delay: int = 0) -> str:
        """Sends one message and returns its ID. ``delay`` hides it for that
        many seconds first (at most 900; 0 uses the queue's delay)."""
        _, data = self._c._call("POST", self._path + "/messages", _outgoing(body, attributes, delay))
        return data["id"]

    def send_json(self, value: Any, attributes: Optional[Mapping[str, str]] = None, delay: int = 0) -> str:
        """Sends ``value`` encoded as JSON."""
        return self.send(json.dumps(value, ensure_ascii=False), attributes, delay)

    def send_batch(self, messages: Sequence[Union[str, Outgoing]]) -> List[str]:
        """Sends up to 10 messages in one request (all or none) and returns their IDs in order."""
        if not 1 <= len(messages) <= MAX_BATCH:
            raise ValueError(f"send 1 to {MAX_BATCH} messages at once")
        payload = [_outgoing(m, None, 0) if isinstance(m, str) else _outgoing(m.body, m.attributes, m.delay) for m in messages]
        _, data = self._c._call("POST", self._path + "/messages", {"messages": payload})
        return list(data["ids"])

    def receive(self, max: int = 1, wait: int = 0, visibility_timeout: Optional[int] = None) -> List[Message]:
        """Takes up to ``max`` (1–10) messages, hiding them for the visibility
        timeout (or ``visibility_timeout`` seconds). Returns none when the
        queue is empty, after long-polling up to ``wait`` seconds (at most 20)."""
        payload: Dict[str, int] = {}
        if max and max != 1:
            payload["max"] = int(max)
        if wait:
            payload["wait"] = int(min(wait, MAX_WAIT))
        if visibility_timeout:
            payload["visibilityTimeout"] = int(visibility_timeout) if visibility_timeout >= 1 else 1
        _, data = self._c._call("POST", self._path + "/receive", payload)
        return [Message._from(m) for m in (data or {}).get("messages") or []]

    def delete(self, receipt: str) -> None:
        """Deletes a handled message by its receipt (deleting it again is not an error)."""
        self._c._call("DELETE", self._path + "/messages/" + quote(receipt, safe=""))

    def delete_batch(self, receipts: Sequence[str]) -> List[DeleteResult]:
        """Deletes up to 10 handled messages; each receipt gets a result, in the order given."""
        if not 1 <= len(receipts) <= MAX_BATCH:
            raise ValueError(f"delete 1 to {MAX_BATCH} receipts at once")
        _, data = self._c._call("POST", self._path + "/delete", {"receipts": list(receipts)})
        by_receipt: Dict[str, List[DeleteResult]] = {}
        for r in (data or {}).get("results") or []:
            by_receipt.setdefault(r.get("receipt", ""), []).append(DeleteResult(r.get("receipt", ""), bool(r.get("deleted")), r.get("error", "") or ""))
        out = []
        for rc in receipts:
            got = by_receipt.get(rc)
            out.append(got.pop(0) if got else DeleteResult(rc, False, "unavailable"))
        return out

    def change_visibility(self, receipt: str, timeout: int) -> None:
        """Hides a received message ``timeout`` seconds from now (to work on it
        longer), or with 0 hands it out again at once."""
        self._c._call("POST", self._path + "/visibility", {"receipt": receipt, "timeout": int(timeout)})

    def purge(self) -> None:
        """Removes every message of the queue."""
        self._c._call("POST", self._path + "/purge", {})

    def process(
        self,
        handler: Callable[[Message], Any],
        concurrency: int = 1,
        wait: int = MAX_WAIT,
        visibility_timeout: Optional[int] = None,
        retry_delay: int = 0,
        on_error: Optional[Callable[[BaseException, Optional[Message]], None]] = None,
        stop: Optional[threading.Event] = None,
    ) -> None:
        """A worker: receives with long polling and runs ``handler`` on each
        message in ``concurrency`` threads, until ``stop`` is set (it then
        returns once the running handlers finish).

        A message whose handler returns is deleted; one whose handler raises
        is left to be handed out again (after ``retry_delay`` seconds when
        set), and after the queue's max_receives it goes to its dead-letter
        queue. Busy or unreachable answers are retried with backoff; errors
        that retrying cannot fix (a missing queue, a key that may not
        receive) are raised.
        """
        stop = stop or threading.Event()
        concurrency = max(1, concurrency)
        wait = min(MAX_WAIT, wait) if wait and wait > 0 else MAX_WAIT
        slots = threading.Semaphore(concurrency)

        def report(err: BaseException, m: Optional[Message]) -> None:
            if on_error:
                try:
                    on_error(err, m)
                except Exception:
                    pass

        def run(m: Message) -> None:
            try:
                try:
                    handler(m)
                except Exception as err:
                    report(err, m)
                    if retry_delay > 0:
                        try:
                            self.change_visibility(m.receipt, retry_delay)
                        except QueueServiceError as e:
                            if e.code != "invalid_receipt":
                                report(e, m)
                        except Exception as e:
                            report(e, m)
                    return
                try:
                    self.delete(m.receipt)
                except Exception as e:
                    report(e, m)
            finally:
                slots.release()

        failures = 0
        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="queue-" + self.name) as pool:
            while not stop.is_set():
                # Take only as many messages as there are free threads, so
                # none waits hidden while its visibility timeout runs.
                while not slots.acquire(timeout=0.5):
                    if stop.is_set():
                        return
                free = 1
                while free < min(concurrency, MAX_BATCH) and slots.acquire(blocking=False):
                    free += 1
                try:
                    messages = self.receive(max=free, wait=wait, visibility_timeout=visibility_timeout)
                except QueueServiceError as err:
                    for _ in range(free):
                        slots.release()
                    if err.status in _PERMANENT_STATUSES and err.code != "project_suspended":
                        raise
                    report(err, None)
                    failures += 1
                    stop.wait(_backoff(failures))
                    continue
                except Exception as err:
                    for _ in range(free):
                        slots.release()
                    report(err, None)
                    failures += 1
                    stop.wait(_backoff(failures))
                    continue
                failures = 0
                for _ in range(free - len(messages)):
                    slots.release()
                for m in messages:
                    pool.submit(run, m)


def _outgoing(body: str, attributes: Optional[Mapping[str, str]], delay: int) -> Dict[str, Any]:
    o: Dict[str, Any] = {"body": body}
    if attributes:
        o["attributes"] = dict(attributes)
    if delay:
        o["delay"] = int(delay)
    return o
