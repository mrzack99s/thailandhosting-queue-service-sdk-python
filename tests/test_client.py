"""Unit tests, and (when TH_QUEUE_TEST_ENDPOINT is set) a round trip against a
real endpoint."""

import json
import os
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from thailandhosting_queueservice import (  # noqa: E402
    Client,
    NotFoundError,
    Outgoing,
    QueueServiceError,
    content_digest,
    sign_request,
)


class SigningTest(unittest.TestCase):
    def test_vector(self):
        # Made by the service's own signer.
        body = b'{"body":"hello"}'
        digest = content_digest(body)
        self.assertEqual(digest, "sha-256=:HaY64dHGT0VJzs5YVVsj7yU6p7ssXKbEjBKRiGPP9Ro=:")
        h = sign_request("POST", "https://queues.thailandhosting.com/th-bkk-1/v1/queues/jobs/messages?x=1", "AKID", "secret", digest, created=1767225600)
        self.assertEqual(h["Signature-Input"], 'th=("@method" "@authority" "@path" "@query" "content-digest");created=1767225600;keyid="AKID";alg="hmac-sha256"')
        self.assertEqual(h["Signature"], "th=:eOsMVDCzdaff3N9m/aWz1ijBsJ9BgdiTtotBm4vfDdo=:")
        g = sign_request("GET", "https://queues.thailandhosting.com/th-bkk-1/v1/queues", "AKID", "secret", created=1767225600)
        self.assertEqual(g["Signature"], "th=:OjjuqbpOgI6K6AMMgIsPsySRmAEGuQPAwO/wXEyBfb4=:")

    def test_endpoint(self):
        self.assertEqual(Client("k", "s").base_url, "https://queues.thailandhosting.com/v1")
        self.assertEqual(Client("k", "s", region="th-bkk-1").base_url, "https://queues.thailandhosting.com/th-bkk-1/v1")
        self.assertEqual(Client("k", "s", endpoint="https://queues.thailandhosting.com/th-bkk-1/").base_url, "https://queues.thailandhosting.com/th-bkk-1/v1")
        self.assertEqual(Client("k", "s", region="th-bkk-1", endpoint="http://127.0.0.1:7811/v1").base_url, "http://127.0.0.1:7811/th-bkk-1/v1")
        with self.assertRaises(ValueError):
            Client("k", "")


class ErrorsTest(unittest.TestCase):
    def test_errors_and_retries(self):
        calls = {"n": 0}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                assert self.headers["Signature-Input"].startswith("th=(")
                if self.path == "/th-bkk-1/v1/queues/busy":
                    calls["n"] += 1
                    if calls["n"] < 3:
                        body = b'{"type":"tag:x","title":"Busy","status":503,"code":"busy"}'
                        self.send_response(503)
                        self.send_header("Retry-After", "0")
                    else:
                        body = b'{"name":"busy","createdAt":"2026-10-14T00:00:00.123456789Z","messages":{"visible":3}}'
                        self.send_response(200)
                else:
                    body = json.dumps({"type": "tag:x", "title": "The queue does not exist.", "status": 404, "code": "queue_not_found", "instance": self.path}).encode()
                    self.send_response(404)
                    self.send_header("X-Error-Code", "queue_not_found")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            c = Client("k", "s", region="th-bkk-1", endpoint=f"http://127.0.0.1:{srv.server_port}")
            info = c.get_queue("busy")
            self.assertEqual(info.messages.visible, 3)
            self.assertEqual(calls["n"], 3)
            self.assertIsNotNone(info.created_at)
            with self.assertRaises(NotFoundError) as e:
                c.get_queue("missing")
            self.assertEqual(e.exception.code, "queue_not_found")
            self.assertEqual(e.exception.instance, "/th-bkk-1/v1/queues/missing")
        finally:
            srv.shutdown()
            srv.server_close()


@unittest.skipUnless(os.environ.get("TH_QUEUE_TEST_ENDPOINT"), "TH_QUEUE_TEST_ENDPOINT is not set")
class RoundTripTest(unittest.TestCase):
    def test_round_trip(self):
        c = Client(os.environ["TH_QUEUE_TEST_KEY"], os.environ["TH_QUEUE_TEST_SECRET"], region=os.environ.get("TH_QUEUE_TEST_REGION"), endpoint=os.environ["TH_QUEUE_TEST_ENDPOINT"])
        name = f"sdk-python-{time.time_ns()}"
        dlq = name + "-dlq"
        self.assertTrue(c.put_queue(dlq).created)
        info = c.put_queue(name, visibility_timeout=30, max_receives=2, dead_letter_queue=dlq)
        self.assertTrue(info.created)
        self.assertEqual(info.max_receives, 2)
        self.assertEqual(info.dead_letter_queue, dlq)
        try:
            self.assertFalse(c.put_queue(name, visibility_timeout=20).created)
            self.assertIn(name, [q.name for q in c.list_queues()])

            q = c.queue(name)
            self.assertTrue(q.send_json({"order": 1, "name": "สวัสดี"}, attributes={"kind": "order"}))
            self.assertEqual(len(q.send_batch(["a", "b", Outgoing("c", {"x": "1"})])), 3)
            self.assertEqual(q.info().messages.visible, 4)
            got = q.receive(max=10, wait=1)
            self.assertEqual(len(got), 4)
            self.assertEqual(got[0].json(), {"order": 1, "name": "สวัสดี"})
            self.assertEqual(got[0].attributes["kind"], "order")
            self.assertEqual(got[0].receives, 1)
            q.delete(got[0].receipt)
            q.delete(got[0].receipt)  # again: harmless
            res = q.delete_batch([got[1].receipt, got[2].receipt, "nonsense"])
            self.assertEqual([r.deleted for r in res], [True, True, False])
            self.assertEqual(res[2].error, "invalid_receipt")

            q.change_visibility(got[3].receipt, 0)
            again = q.receive(wait=2)
            self.assertEqual(len(again), 1)
            self.assertEqual(again[0].receives, 2)
            q.change_visibility(again[0].receipt, 0)
            self.assertEqual(q.receive(wait=1), [], "a message past max_receives was handed out")
            dead = c.queue(dlq).receive(wait=2)
            self.assertEqual(dead[0].body, "c")

            for i in range(25):
                q.send(str(i))
            seen = set()
            lock = threading.Lock()
            failed = threading.Event()
            stop = threading.Event()
            threading.Timer(30, stop.set).start()

            def handle(m):
                if m.body == "7" and not failed.is_set():
                    failed.set()
                    raise RuntimeError("try again")
                with lock:
                    seen.add(m.body)
                    if len(seen) == 25:
                        stop.set()

            q.process(handle, concurrency=4, wait=1, retry_delay=1, stop=stop)
            self.assertEqual(len(seen), 25)
            self.assertTrue(failed.is_set())
            left = q.info().messages
            self.assertEqual(left.visible + left.in_flight, 0)

            q.send("x")
            q.purge()
            with self.assertRaises(NotFoundError):
                c.get_queue("no-such-queue-" + name)
            with self.assertRaises(QueueServiceError) as e:
                c.queue("no-such-queue-" + name).process(lambda m: None)
            self.assertEqual(e.exception.code, "queue_not_found")
        finally:
            for n in (name, dlq):
                try:
                    c.delete_queue(n)
                except QueueServiceError:
                    pass


if __name__ == "__main__":
    unittest.main()
