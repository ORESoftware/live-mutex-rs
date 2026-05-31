"""Offline protocol round-trip tests (no broker needed).

    python3 -m unittest discover -s clients/python -p 'test_*.py'
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from network_mutex import protocol  # noqa: E402
from network_mutex.protocol import Response, ResponseType  # noqa: E402


def decode_frame(frame: bytes):
    assert frame.endswith(b"\n"), "frames must be newline-terminated"
    return json.loads(frame[:-1])


class ProtocolTests(unittest.TestCase):
    def test_lock_request_uses_camel_case_wire_fields(self):
        obj = decode_frame(
            protocol.lock_request("u-1", key="k1", ttl_ms=4000, max_holders=1, pid=123)
        )
        self.assertEqual(obj["type"], "lock")
        self.assertEqual(obj["key"], "k1")
        self.assertEqual(obj["ttl"], 4000)
        self.assertEqual(obj["max"], 1)
        self.assertEqual(obj["pid"], 123)
        self.assertNotIn("keys", obj)  # None fields stripped

    def test_composite_lock_request_sorted_by_broker_not_client(self):
        obj = decode_frame(protocol.lock_request("u-2", keys=["c", "a", "b"]))
        self.assertEqual(obj["type"], "lock")
        self.assertEqual(obj["keys"], ["c", "a", "b"])  # client does not sort

    def test_lock_request_rejects_both_key_and_keys(self):
        with self.assertRaises(ValueError):
            protocol.lock_request("u", key="k", keys=["a"])

    def test_lock_request_rejects_neither_key_nor_keys(self):
        with self.assertRaises(ValueError):
            protocol.lock_request("u")

    def test_lock_request_rejects_oversized_composite(self):
        with self.assertRaises(ValueError):
            protocol.lock_request("u", keys=["a", "b", "c", "d", "e", "f"])

    def test_register_read_request_tag(self):
        obj = decode_frame(protocol.register_read_request("u", "k"))
        self.assertEqual(obj["type"], "registerRead")

    def test_response_parses_composite_grant(self):
        resp = Response.decode(
            json.dumps(
                {
                    "type": "compositeLock",
                    "uuid": "u-1",
                    "keys": ["a", "b"],
                    "acquired": True,
                    "lockUuid": "L-1",
                    "fencingTokens": {"a": 5, "b": 12},
                }
            ).encode()
        )
        self.assertIs(resp.type, ResponseType.COMPOSITE_LOCK)
        self.assertTrue(resp.acquired)
        self.assertEqual(resp.lock_uuid, "L-1")
        self.assertEqual(resp.fencing_tokens, {"a": 5, "b": 12})

    def test_response_distinguishes_absent_from_false(self):
        resp = Response.decode(json.dumps({"type": "ok", "uuid": "u"}).encode())
        self.assertIs(resp.type, ResponseType.OK)
        self.assertIsNone(resp.acquired)  # absent, not False

    def test_unknown_response_type_raises(self):
        with self.assertRaises(ValueError):
            Response.decode(json.dumps({"type": "totallyBogus", "uuid": "u"}).encode())


if __name__ == "__main__":
    unittest.main()
