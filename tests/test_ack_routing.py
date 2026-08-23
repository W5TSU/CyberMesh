"""Unit tests for hop-layer ACK accumulation (_on_routing).

No radio / no mesh traffic — pure in-memory status machine.
"""
import time
import unittest
from collections import deque
from unittest.mock import MagicMock


def _make_client():
    """Minimal MeshtasticClient-like shell with just the ack methods."""
    # Import after path is the app dir when run via venv from ~/cybermesh
    import mesh_client as mc

    client = object.__new__(mc.MeshClient)
    client.messages = deque(maxlen=500)
    client.store = None
    client.iface = MagicMock()
    client.iface.myInfo = MagicMock()
    client.iface.myInfo.my_node_num = 0x1FA040D0  # !1fa040d0
    client.node_store = None
    return client


def _outbound(pkt_id, *, direct=False, to="^all", status="sending"):
    return {
        "id": pkt_id,
        "ts": time.time(),
        "from": "me",
        "to": to,
        "direct": direct,
        "channel": 0,
        "via_mqtt": False,
        "text": "test",
        "status": status,
        "status_reason": None,
        "heard_by": [],
    }


def _routing_packet(request_id, from_num, reason="NONE", via_mqtt=False):
    return {
        "from": from_num,
        "fromId": f"!{from_num:08x}",
        "viaMqtt": via_mqtt,
        "decoded": {
            "portnum": "ROUTING_APP",
            "requestId": request_id,
            "routing": {"errorReason": reason},
        },
    }


class TestAckRouting(unittest.TestCase):
    def test_broadcast_accumulates_multiple_hearers(self):
        """Pubsub path must accept MORE than one relay ack (library onResponse cannot)."""
        c = _make_client()
        c.messages.append(_outbound(42, direct=False))

        c._on_routing(_routing_packet(42, 0x28673F95))  # tower
        c._on_routing(_routing_packet(42, 0x85757BB4))  # ADV

        msg = c.messages[0]
        self.assertEqual(msg["status"], "delivered")
        ids = {h["id"] for h in msg["heard_by"]}
        self.assertEqual(ids, {"!28673f95", "!85757bb4"})

    def test_broadcast_implicit_ack_from_self_counts(self):
        """Firmware delivers broadcast implicit ACKs as from=self — must not skip."""
        c = _make_client()
        c.messages.append(_outbound(7, direct=False))
        c._on_routing(_routing_packet(7, 0x1FA040D0))  # self = implicit rebroadcast ACK
        self.assertEqual(c.messages[0]["status"], "delivered")
        self.assertTrue(c.messages[0]["heard_by"])
        self.assertTrue(c.messages[0]["heard_by"][0].get("implicit"))

    def test_dm_self_ack_still_skipped(self):
        c = _make_client()
        c.messages.append(_outbound(8, direct=True, to="!28673f95"))
        c._on_routing(_routing_packet(8, 0x1FA040D0))  # self
        self.assertEqual(c.messages[0]["status"], "sending")
        self.assertEqual(c.messages[0]["heard_by"], [])

    def test_max_retransmit_does_not_fail_broadcast(self):
        c = _make_client()
        c.messages.append(_outbound(9, direct=False))
        c._on_routing(_routing_packet(9, 0x1FA040D0, reason="MAX_RETRANSMIT"))
        self.assertEqual(c.messages[0]["status"], "sending")
        # Late relay ack still lands
        c._on_routing(_routing_packet(9, 0x28673F95))
        self.assertEqual(c.messages[0]["status"], "delivered")
        self.assertEqual(c.messages[0]["heard_by"][0]["id"], "!28673f95")

    def test_max_retransmit_does_not_fail_dm_before_timeout(self):
        c = _make_client()
        c.messages.append(_outbound(11, direct=True, to="!28673f95"))
        c._on_routing(_routing_packet(11, 0x1FA040D0, reason="MAX_RETRANSMIT"))
        self.assertEqual(c.messages[0]["status"], "sending")
        # Dest ack still upgrades to delivered
        c._on_routing(_routing_packet(11, 0x28673F95))
        self.assertEqual(c.messages[0]["status"], "delivered")

    def test_hard_error_fails_dm(self):
        c = _make_client()
        c.messages.append(_outbound(13, direct=True, to="!deadbeef"))
        c._on_routing(_routing_packet(13, 0x1FA040D0, reason="NO_CHANNEL"))
        self.assertEqual(c.messages[0]["status"], "failed")
        self.assertEqual(c.messages[0]["status_reason"], "NO_CHANNEL")

    def test_ignores_unrelated_request_id(self):
        c = _make_client()
        c.messages.append(_outbound(1, direct=False))
        c._on_routing(_routing_packet(999, 0x28673F95))
        self.assertEqual(c.messages[0]["status"], "sending")
        self.assertEqual(c.messages[0]["heard_by"], [])

    def test_does_not_downgrade_delivered_on_late_nak(self):
        c = _make_client()
        c.messages.append(_outbound(5, direct=False, status="delivered"))
        c.messages[0]["heard_by"] = [{"id": "!28673f95", "via_mqtt": False}]
        c._on_routing(_routing_packet(5, 0x1FA040D0, reason="MAX_RETRANSMIT"))
        self.assertEqual(c.messages[0]["status"], "delivered")


if __name__ == "__main__":
    unittest.main()
