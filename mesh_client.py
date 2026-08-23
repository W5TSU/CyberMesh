import base64
import logging
import os
import threading
import time
from collections import deque

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message
from meshtastic import BROADCAST_ADDR
from meshtastic.protobuf import channel_pb2, localonly_pb2, mesh_pb2, portnums_pb2
from meshtastic.util import genPSK256
from pubsub import pub

import meshtastic.serial_interface
import meshtastic.tcp_interface

logger = logging.getLogger("cybermesh")

MESSAGE_HISTORY_LIMIT = 500

# How long a direct message waits for an ack before we stop calling it in
# flight. The firmware retries a reliable send a few times over ~30s, so this
# is deliberately longer than that.
MESSAGE_ACK_TIMEOUT_SECS = 90

# A traceroute reply has to make the round trip hop by hop; give it well over
# the firmware's own retry window before calling it dead.
TRACEROUTE_TIMEOUT_SECS = 120

# How many persisted traceroutes to reload into the in-memory dict on
# startup — just needs to comfortably exceed the number of distinct nodes
# ever traced, since only the latest per node survives the seed.
TRACEROUTE_SEED_LIMIT = 300

# Works around meshtastic/firmware#10494: after a WiFi reconnect, the node's
# TCP API server can write into the dead old socket forever and never accept
# a new client — only a device reboot clears it, and the WiFi/TCP side can't
# be used to send that reboot (it's the thing that's wedged). Serial is a
# separate transport, unaffected by the wedge, so it's the recovery path.
WATCHDOG_DISCONNECT_THRESHOLD_SECS = 180
WATCHDOG_COOLDOWN_SECS = 300
WATCHDOG_MAX_PER_HOUR = 6

# Fields never sent to a browser in the clear. /api/config is unauthenticated
# (anything that can reach :5090 can read it), so the WiFi PSK, the MQTT
# password and the node's private key are replaced with REDACTED on the way
# out. On the way back in, a value still equal to REDACTED means "unchanged"
# and is dropped from the write — otherwise saving the Network section would
# overwrite the real PSK with the placeholder and knock the node off WiFi.
REDACTED = "••••••••"
SECRET_CONFIG_FIELDS = {"network": {"wifi_psk"}, "security": {"private_key"}}
SECRET_MODULE_FIELDS = {"mqtt": {"password"}}
# Remote-admin responses go to the browser too. Public keys are not secret, but
# admin_key is still credential-ish clutter; show that it exists without
# dumping raw key material into every phone screenshot. The user's actual
# private key and WiFi/MQTT passwords stay masked, obviously.
REMOTE_SECRET_CONFIG_FIELDS = {
    "network": {"wifi_psk"},
    "security": {"private_key", "admin_key"},
}
REMOTE_SECRET_MODULE_FIELDS = SECRET_MODULE_FIELDS

REMOTE_ADMIN_LOG_LIMIT = 80
REMOTE_ADMIN_TIMEOUT_SECS = 75

CONFIG_SECTIONS = ["device", "position", "power", "network", "display", "lora", "bluetooth", "security"]
MODULE_SECTIONS = [
    "mqtt", "serial", "external_notification", "store_forward", "range_test",
    "telemetry", "canned_message", "audio", "remote_hardware", "neighbor_info",
    "detection_sensor", "ambient_lighting", "paxcounter", "traffic_management",
]


_DROP = object()


def _json_safe(value):
    """iface.nodes entries are plain dicts, not protobuf messages — but they
    are not automatically JSON-serializable either:

    - Some fields (e.g. a freshly-seen node's macaddr/publicKey) hold raw
      bytes before the library's own base64 encoding pass reaches them.
    - Worse, entries updated from a *live* packet carry a raw protobuf under
      a "raw" key. meshtastic's `_handlePacketFromRadio` does
      `asDict["decoded"][name]["raw"] = pb`, and `_onNodeInfoReceive` /
      `_onPositionReceive` then assign that same dict straight into the node
      DB as node["user"] / node["position"]. So the first NodeInfo or Position
      heard over the air permanently poisons iface.nodes with a `User` /
      `Position` object and every later jsonify() of the node list raises
      "Object of type User is not JSON serializable" (500 -> empty node table
      and empty map). The raw protobuf is a duplicate of the dict it sits in,
      so dropping it loses nothing.

    Anything else unexpected is stringified rather than allowed to blow up the
    whole endpoint — one odd field should never take down the node list again.
    """
    if isinstance(value, Message):
        return _DROP
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if isinstance(value, dict):
        return {k: v for k, v in ((k, _json_safe(v)) for k, v in value.items()) if v is not _DROP}
    if isinstance(value, (list, tuple)):
        return [v for v in (_json_safe(v) for v in value) if v is not _DROP]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _redact(values, secret_map):
    """Blank out secret fields in a MessageToDict result, in place."""
    for section, fields in secret_map.items():
        sub = values.get(section)
        if not isinstance(sub, dict):
            continue
        for name in fields:
            if sub.get(name):  # leave genuinely-empty fields visibly empty
                sub[name] = REDACTED
    return values


def _strip_redacted(section, values, secret_map):
    """Drop untouched secret placeholders so a save can't overwrite the real
    value with the mask. Returns a copy — never mutates the caller's dict."""
    secrets = secret_map.get(section)
    if not secrets:
        return values
    return {k: v for k, v in values.items() if not (k in secrets and v == REDACTED)}


def _redact_section(section, values, secret_map):
    """Redact a single section dict (remote-admin returns one section at a time)."""
    secrets = secret_map.get(section)
    if not secrets:
        return values
    redacted = dict(values)
    for name in secrets:
        if redacted.get(name):
            redacted[name] = REDACTED
    return redacted


def _field_entry(f):
    """Classify a protobuf field for form rendering. Returns None for field
    kinds the form can't safely render (nested messages, repeated bytes) —
    those are left out of the schema and never touched by a form save."""
    if f.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        return None
    if f.is_repeated and f.type == FieldDescriptor.TYPE_BYTES:
        return None

    if f.cpp_type == FieldDescriptor.CPPTYPE_BOOL:
        base_kind = "bool"
    elif f.cpp_type == FieldDescriptor.CPPTYPE_ENUM:
        base_kind = "enum"
    elif f.type == FieldDescriptor.TYPE_BYTES:
        base_kind = "bytes"
    elif f.cpp_type == FieldDescriptor.CPPTYPE_STRING:
        base_kind = "string"
    elif f.cpp_type in (FieldDescriptor.CPPTYPE_FLOAT, FieldDescriptor.CPPTYPE_DOUBLE):
        base_kind = "float"
    else:
        base_kind = "int"

    entry = {"name": f.name, "kind": "list" if f.is_repeated else base_kind}
    if f.is_repeated:
        entry["item_kind"] = base_kind
    if base_kind == "enum":
        entry["options"] = [v.name for v in f.enum_type.values]
    return entry


def build_schema():
    """Static field metadata for every config/module_config section, derived
    from the protobuf schema itself — doesn't need a live device connection."""
    lc = localonly_pb2.LocalConfig()
    mc = localonly_pb2.LocalModuleConfig()
    schema = {"config": {}, "module_config": {}}

    for section in CONFIG_SECTIONS:
        msg = getattr(lc, section)
        schema["config"][section] = [e for f in msg.DESCRIPTOR.fields if (e := _field_entry(f))]

    for section in MODULE_SECTIONS:
        if not hasattr(mc, section):
            schema["module_config"][section] = []
            continue
        msg = getattr(mc, section)
        schema["module_config"][section] = [e for f in msg.DESCRIPTOR.fields if (e := _field_entry(f))]

    return schema


class MeshClient:
    def __init__(self, host, serial_port=None, transport="auto",
                 home_lat=None, home_lon=None, store=None, node_store=None,
                 bbs_engine=None):
        self.host = host
        self.serial_port = serial_port
        self.home_lat = home_lat
        self.home_lon = home_lon
        # "auto"  — try USB serial first, fall back to WiFi TCP
        # "serial"/"tcp" — force one transport
        self.transport_pref = transport
        self.transport = None  # which transport the live connection is using
        self.lock = threading.RLock()
        self.iface = None
        self.connected = False
        self.last_error = None
        self.store = store
        self.messages = deque(maxlen=MESSAGE_HISTORY_LIMIT)
        if store is not None:
            # Rehydrate so a restart doesn't look like the mesh went silent.
            self.messages.extend(store.recent(MESSAGE_HISTORY_LIMIT))
        self.node_store = node_store
        # Single-node BBS (v0). None when BBS_ENABLED is off. See bbs/ and
        # ~/cybermesh-bbs/DESIGN.md. Wired after construct from app.py is fine
        # too — attribute is always present so the receive path can check it.
        self.bbs_engine = bbs_engine
        self.traceroutes = {}  # node id -> latest result
        self.remote_admin_log = deque(maxlen=REMOTE_ADMIN_LOG_LIMIT)
        if node_store is not None:
            # Same rehydration idea as messages above: without this, every
            # restart (routine after a template edit — see Known Incidents)
            # blanks the Traceroutes panel and breaks "save as proven path"
            # until a fresh trace is run.
            for entry in node_store.recent_traceroutes(TRACEROUTE_SEED_LIMIT):
                self.traceroutes.setdefault(entry["to"], entry)
        self.disconnected_since = time.time()
        self.last_auto_reboot = None
        self.auto_reboot_history = deque(maxlen=WATCHDOG_MAX_PER_HOUR)
        self.last_seen = {}  # node id -> our own wall-clock time of last packet
        # node id -> when it last sent a position packet carrying no usable fix.
        # Lets a range probe tell "answered, but GPS cold" apart from "silent".
        self.last_fixless_position = {}
        pub.subscribe(self._on_receive_text, "meshtastic.receive.text")
        pub.subscribe(self._on_receive_reply, "meshtastic.receive.data.REPLY_APP")
        pub.subscribe(self._on_any_receive, "meshtastic.receive")
        pub.subscribe(self._on_telemetry, "meshtastic.receive.telemetry")
        pub.subscribe(self._on_position, "meshtastic.receive.position")
        # Routing ACK/NAK for our outbound wantAck traffic. We deliberately do
        # NOT use MeshInterface's onResponse callback for multi-ack tracking —
        # the library pops the handler on the *first* response, so a local
        # MAX_RETRANSMIT would permanently drop every later relay ack. Pubsub
        # publishes every ROUTING_APP packet, so we can accumulate hearers.
        pub.subscribe(self._on_routing, "meshtastic.receive.routing")
        pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _on_receive_text(self, packet, interface):
        try:
            # dict.get(k, default) only falls back when the key is *absent* —
            # the library sets "fromId": None outright on some packets (seen
            # for senders not yet in the local node DB), which silently sank
            # the sender's name to "Broadcast" in the UI even though the
            # numeric "from" field was right there unused.
            fromId = packet.get("fromId") or None
            if not fromId:
                num = packet.get("from")
                fromId = f"!{num:08x}" if num else None
            text = packet["decoded"]["text"]
            channel = packet.get("channel", 0)
        except (KeyError, TypeError):
            return
        toId = packet.get("toId")
        is_direct = bool(toId) and toId != BROADCAST_ADDR

        # Resolve sender long name before _record buries it
        from_name = None
        from_short = None
        try:
            if self.iface and fromId:
                node = (self.iface.nodes or {}).get(fromId)
                from_name = (node or {}).get("user", {}).get("longName")
                from_short = (node or {}).get("user", {}).get("shortName")
        except Exception:
            pass
        # Persist the name we just saw — this is what lets the UI keep showing
        # a friendly name long after the node leaves the mesh's live DB.
        if self.store is not None and fromId and from_name:
            try:
                self.store.record_node_name(fromId, from_name, from_short)
            except Exception:
                pass

        msg = {
            "id": packet.get("id"),
            "ts": time.time(),
            "from": fromId,
            "from_name": from_name,
            "to": toId,
            # A packet addressed to us specifically rather than to ^all is a
            # direct message — the UI threads those separately.
            "direct": is_direct,
            "channel": channel,
            "via_mqtt": bool(packet.get("viaMqtt")),
            "text": text,
            "status": "received",
            "status_reason": None,
            # Last-hop RF meta (not a full multi-hop route — use traceroute for that)
            **self._rf_meta_from_packet(packet),
        }
        self._record(msg)

        # BBS v0 — DM keyword "BBS" or an active session. Must not block the
        # meshtastic receive thread (same lesson as traceroute/async connect).
        if is_direct and self.bbs_engine and fromId:
            try:
                if self.bbs_engine.should_handle(fromId, text):
                    threading.Thread(
                        target=self._bbs_handle_dm,
                        args=(fromId, text),
                        daemon=True,
                        name=f"bbs-{fromId}",
                    ).start()
            except Exception:
                logger.exception("BBS should_handle failed for %s — fail open", fromId)

    def _bbs_handle_dm(self, from_id, text):
        """Worker: run BBS engine and send reply. Never called on the RX thread."""
        try:
            result = self.bbs_engine.handle(from_id, text)
        except Exception:
            logger.exception("BBS handle failed for %s", from_id)
            return
        if not result.get("handled"):
            return
        reply = result.get("reply")
        if not reply:
            return
        try:
            self.send_text(reply, destination=from_id)
            logger.info(
                "BBS reply → %s (session_active=%s): %s",
                from_id,
                result.get("session_active"),
                (reply[:80] + "…") if len(reply) > 80 else reply,
            )
        except Exception as e:
            logger.warning("BBS reply send to %s failed: %s", from_id, e)

    def _on_receive_reply(self, packet, interface=None):
        """Handle Meshtastic tapbacks/reactions (REPLY_APP).

        Firmware carries the target message id in decoded.replyId and the emoji
        as a Unicode code point in decoded.emoji. The Python library does not
        currently decode REPLY_APP specially, so we subscribe to the raw data
        topic and do the tiny bit of translation here.
        """
        try:
            decoded = packet.get("decoded", {})
            target_id = decoded.get("replyId") or decoded.get("reply_id")
            emoji_code = decoded.get("emoji")
            if not target_id or not emoji_code:
                return
            emoji = chr(int(emoji_code))
            from_id = packet.get("fromId") or None
            if not from_id:
                num = packet.get("from")
                from_id = f"!{num:08x}" if num else None
        except Exception:
            return
        self._record_reaction(target_id, from_id, emoji)

    def _on_any_receive(self, packet, interface=None):
        """Track our own last-seen time for every packet type, not just text.

        The meshtastic library's own `iface.nodes[id]["lastHeard"]` is only
        updated by a subset of packet handlers (text, nodeinfo) via rxTime —
        notably NOT position, by far the most common periodic beacon, so a
        node broadcasting nothing but routine position pings never gets its
        lastHeard bumped even though it's clearly still being heard. Confirmed
        live: a node's text message arrived and was logged within seconds,
        while the library's own lastHeard for that same node stayed hours
        stale. This tracks receipt time ourselves, independent of that.
        """
        try:
            from_id = packet.get("fromId")
            if not from_id:
                num = packet.get("from")
                if num is None:
                    return
                from_id = f"!{num:08x}"
            self.last_seen[from_id] = time.time()
        except Exception:
            pass

    def _on_telemetry(self, packet, interface=None):
        """Record our own node's self-reported channelUtilization/airUtilTx
        over time — it broadcasts its own deviceMetrics over the mesh
        periodically, same packet type any other node's telemetry arrives as.
        Feeds the dashboard's congestion-vs-ack-success chart."""
        if self.node_store is None:
            return
        try:
            if not self.iface or not self.iface.myInfo:
                return
            if packet.get("from") != self.iface.myInfo.my_node_num:
                return
            dm = packet.get("decoded", {}).get("telemetry", {}).get("deviceMetrics")
            if not dm:
                return
            self.node_store.record_telemetry(
                channel_utilization=dm.get("channelUtilization"),
                air_util_tx=dm.get("airUtilTx"),
                battery_level=dm.get("batteryLevel"),
                voltage=dm.get("voltage"),
            )
        except Exception:
            pass

    def _on_position(self, packet, interface=None):
        """Log every position packet that reaches us, with the link quality it
        arrived on — this is the coverage map's raw material.

        Deliberately NOT the same thing as the node DB's current position: the
        library overwrites that in place, so a node that beacons a precise fix
        on our private channel and a deliberately-fuzzed one on the public
        channel ends up showing whichever landed last. Keeping the packets
        themselves, tagged with `channel` and `precision_bits`, is what lets
        the UI ask for only the full-resolution fleet positions later.
        """
        if self.node_store is None:
            return
        try:
            from_id = packet.get("fromId") or None
            if not from_id:
                num = packet.get("from")
                if num is None:
                    return
                from_id = f"!{num:08x}"

            pos = (packet.get("decoded") or {}).get("position") or {}
            lat = pos.get("latitude")
            lon = pos.get("longitude")
            # A GPS with no fix reports a literal 0,0 rather than omitting the
            # field — plotting those puts a node in the Gulf of Guinea.
            if lat in (None, 0) or lon in (None, 0):
                # Not plottable, but far from meaningless: the node HEARD us and
                # answered, it just has nothing to say about where it is. Range
                # probing needs that distinction — otherwise a node sitting in
                # perfect RF range with a cold-starting GPS records as a string
                # of coverage holes, which is exactly backwards.
                self.last_fixless_position[from_id] = time.time()
                return

            # hopsAway isn't in the packet — it's the difference between the hop
            # budget the sender set and what's left. 0 means it reached us
            # directly, which for a mobile node is the real headline: no relay
            # was needed from wherever this was transmitted.
            hop_start = packet.get("hopStart")
            hop_limit = packet.get("hopLimit")
            hops = None
            if hop_start is not None and hop_limit is not None:
                hops = max(0, hop_start - hop_limit)

            self.node_store.record_position({
                "ts": time.time(),
                "node_id": from_id,
                "lat": lat,
                "lon": lon,
                "alt": pos.get("altitude"),
                "precision_bits": pos.get("precisionBits"),
                "channel": packet.get("channel", 0),
                "snr": packet.get("rxSnr"),
                "rssi": packet.get("rxRssi"),
                "hops_away": hops,
                "relay_node": packet.get("relayNode"),
                "via_mqtt": bool(packet.get("viaMqtt")),
                "pkt_id": packet.get("id"),
                # The fix's OWN timestamp, not ours. A duty-cycled GPS
                # (gps_update_interval > 10s puts it in hardsleep between
                # searches) replies with the LAST fix it managed, which can be
                # many minutes stale — at driving speed that's miles from where
                # the packet was actually sent. Without this the map plots a
                # stale fix as confidently as a fresh one.
                "pos_time": pos.get("time"),
            })

            # Position is by far the most common beacon, so it's also the best
            # opportunity to keep the persisted name map current — without this
            # a node that only ever beacons shows up as a bare hex id forever.
            if self.store is not None:
                node = (self.iface.nodes or {}).get(from_id) if self.iface else None
                user = (node or {}).get("user") or {}
                if user.get("longName"):
                    self.store.record_node_name(from_id, user["longName"], user.get("shortName"))
        except Exception:
            logger.debug("position log failed", exc_info=True)

    def _on_connection_lost(self, interface):
        logger.warning("Lost connection to %s", self.host)
        with self.lock:
            self.connected = False
            # Only stamp the *start* of the outage. Each failed reconnect
            # attempt also fires this event, so refreshing the timestamp here
            # kept resetting the clock and could starve the watchdog forever —
            # it would never see 180s of continuous downtime.
            if self.disconnected_since is None:
                self.disconnected_since = time.time()

    def _run(self):
        while not self._stop:
            with self.lock:
                already_connected = self.connected
            if not already_connected:
                self._connect()
            self._watchdog_check()
            self._expire_pending_acks()
            self._expire_traceroutes()
            time.sleep(5)

    def _transport_order(self):
        """USB serial first by default: this board's WiFi TCP API server wedges
        or refuses connections regularly (see firmware#10494 note above), while
        the USB cable to the Pi is rock solid. TCP stays as the fallback for
        when the T-Beam is unplugged and running on WiFi alone."""
        if self.transport_pref == "serial":
            return ["serial"] if self.serial_port else []
        if self.transport_pref == "tcp":
            return ["tcp"]
        return (["serial"] if self.serial_port else []) + ["tcp"]

    def _open(self, kind):
        if kind == "serial":
            return meshtastic.serial_interface.SerialInterface(devPath=self.serial_port)
        return meshtastic.tcp_interface.TCPInterface(hostname=self.host)

    def _connect(self):
        old_iface = self.iface
        if old_iface is not None:
            try:
                old_iface.close()
            except Exception:
                pass

        errors = []
        for kind in self._transport_order():
            # Built outside self.lock on purpose — a failing connect blocks for
            # ~30s and must not stall fast status reads.
            try:
                new_iface = self._open(kind)
            except Exception as e:
                errors.append(f"{kind}: {e}")
                logger.warning("Connect via %s failed: %s", kind, e)
                continue
            with self.lock:
                self.iface = new_iface
                self.transport = kind
                self.connected = True
                self.last_error = None
                self.disconnected_since = None
            logger.info("Connected via %s (%s)", kind, self.serial_port if kind == "serial" else self.host)
            # Seed the durable node_names cache from the live node DB on every
            # (re)connect — nodes we've heard from once keep their names in the
            # UI even after they drop out of the mesh's in-memory node list.
            if self.store is not None:
                try:
                    self.store.backfill_names(getattr(new_iface, "nodes", None) or {})
                except Exception as e:
                    logger.warning("Could not backfill node names: %s", e)
            return

        with self.lock:
            self.iface = None
            self.transport = None
            self.last_error = "; ".join(errors) or "no transport configured"
            self.connected = False
            if self.disconnected_since is None:
                self.disconnected_since = time.time()

    def _watchdog_check(self):
        # Only meaningful in TCP-only mode: if serial were an available
        # transport we'd already be connected over it, so a serial reboot
        # would fail for the same reason the serial connect just did.
        if not self.serial_port or "serial" in self._transport_order():
            return
        with self.lock:
            if self.connected or self.disconnected_since is None:
                return
            down_for = time.time() - self.disconnected_since
            if down_for < WATCHDOG_DISCONNECT_THRESHOLD_SECS:
                return
            if self.last_auto_reboot and (time.time() - self.last_auto_reboot) < WATCHDOG_COOLDOWN_SECS:
                return
            now = time.time()
            recent = [t for t in self.auto_reboot_history if now - t < 3600]
            if len(recent) >= WATCHDOG_MAX_PER_HOUR:
                logger.warning(
                    "Watchdog: down for %ds but already hit %d auto-reboots this hour, holding off",
                    int(down_for), len(recent),
                )
                return

        self._attempt_serial_reboot(down_for)

    def _attempt_serial_reboot(self, down_for):
        logger.warning(
            "Watchdog: TCP down for %ds (likely meshtastic/firmware#10494 wedge) — "
            "rebooting via serial %s", int(down_for), self.serial_port,
        )
        try:
            iface = meshtastic.serial_interface.SerialInterface(devPath=self.serial_port)
            try:
                iface.localNode.reboot()
            finally:
                iface.close()
            with self.lock:
                self.last_auto_reboot = time.time()
                self.auto_reboot_history.append(self.last_auto_reboot)
                self.disconnected_since = time.time()  # reset the clock while it reboots
            logger.info("Watchdog: serial reboot command sent")
        except Exception as e:
            logger.error("Watchdog: serial reboot attempt failed: %s", e)

    def status(self):
        with self.lock:
            my_node_num = None
            if self.connected and self.iface and self.iface.myInfo:
                my_node_num = self.iface.myInfo.my_node_num
            down_for = None
            if not self.connected and self.disconnected_since:
                down_for = int(time.time() - self.disconnected_since)
            return {
                "connected": self.connected,
                "transport": self.transport,
                "host": self.serial_port if self.transport == "serial" else self.host,
                "last_error": self.last_error,
                "my_node_num": my_node_num,
                "origin": self.my_position(),
                "down_for_secs": down_for,
                "last_auto_reboot": self.last_auto_reboot,
                # Only armed in TCP-only mode — see _watchdog_check().
                "watchdog_enabled": bool(self.serial_port) and "serial" not in self._transport_order(),
            }

    def node_list(self):
        with self.lock:
            if not self.connected or not self.iface:
                return []
            # iface.nodes is mutated by the meshtastic library's own receive
            # thread, which knows nothing about self.lock — snapshot it (with a
            # retry, since even list() can trip "dictionary changed size during
            # iteration") before walking it.
            for _ in range(3):
                try:
                    raw_nodes = list(self.iface.nodes.values())
                    break
                except RuntimeError:
                    time.sleep(0.05)
            else:
                raw_nodes = []
            out = [n for n in (_json_safe(n) for n in raw_nodes) if n is not _DROP]
            for n in out:
                node_id = (n.get("user") or {}).get("id")
                seen = self.last_seen.get(node_id) if node_id else None
                if seen and seen > (n.get("lastHeard") or 0):
                    n["lastHeard"] = seen
            return out

    def my_position(self):
        """Where to measure node distances from. Prefer the node's own GPS fix
        when it has one; the T-Beam is often indoors with no lock, so fall back
        to the configured home coordinates."""
        with self.lock:
            if self.connected and self.iface and self.iface.myInfo:
                me = self.iface.nodes.get(f"!{self.iface.myInfo.my_node_num:08x}") if self.iface.nodes else None
                pos = (me or {}).get("position", {})
                if pos.get("latitude") and pos.get("longitude"):
                    return {"lat": pos["latitude"], "lon": pos["longitude"], "source": "gps"}
        if self.home_lat is None or self.home_lon is None:
            return None
        return {"lat": self.home_lat, "lon": self.home_lon, "source": "configured"}

    def my_node_num(self):
        with self.lock:
            if not self.connected or not self.iface:
                return None
            return self.iface.myInfo.my_node_num if self.iface.myInfo else None

    def get_config(self):
        with self.lock:
            if not self.connected or not self.iface:
                return {}
            return _redact(MessageToDict(
                self.iface.localNode.localConfig,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            ), SECRET_CONFIG_FIELDS)

    def get_module_config(self):
        with self.lock:
            if not self.connected or not self.iface:
                return {}
            return _redact(MessageToDict(
                self.iface.localNode.moduleConfig,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            ), SECRET_MODULE_FIELDS)

    def get_schema(self):
        return build_schema()

    # ---- remote admin -----------------------------------------------------
    # These use the already-open gateway radio connection, so the web UI can
    # admin many trusted remote nodes without stopping cybermesh.service and
    # fighting over the serial port. Remote-admin is intentionally generic: it
    # reads/writes protobuf sections by name, while the browser handles the
    # "are you sure you want to brick that little goblin?" ceremony.

    def _log_remote_admin(self, node_id, action, status, detail=None):
        entry = {
            "ts": time.time(),
            "node_id": node_id,
            "action": action,
            "status": status,
            "detail": detail,
        }
        self.remote_admin_log.append(entry)
        return entry

    def get_remote_admin_log(self):
        return sorted(list(self.remote_admin_log), key=lambda e: -e["ts"])

    def _remote_node(self, node_id):
        node_id = (node_id or "").strip()
        if not node_id.startswith("!") or len(node_id) != 9:
            raise ValueError("node_id must look like !38f11130")
        if not self.connected or not self.iface:
            raise RuntimeError("Not connected")
        # requestChannels=False matters: channel download is slow and not needed
        # for normal config/admin operations. Do NOT skip NodeDB entirely — PKI
        # needs the public-key context already learned by the gateway.
        return self.iface.getNode(node_id, requestChannels=False, timeout=REMOTE_ADMIN_TIMEOUT_SECS)

    def _section_descriptor(self, node, kind, section):
        if kind == "config":
            if section not in CONFIG_SECTIONS:
                raise ValueError("unknown config section")
            return node.localConfig.DESCRIPTOR.fields_by_name.get(section)
        if kind == "module_config":
            if section not in MODULE_SECTIONS:
                raise ValueError("unknown module_config section")
            descriptor = node.moduleConfig.DESCRIPTOR.fields_by_name.get(section)
            if descriptor is None:
                raise ValueError(f"module_config.{section} is not available in this CLI/protobuf build")
            return descriptor
        raise ValueError("kind must be config or module_config")

    def _section_message(self, node, kind, section):
        return getattr(node.localConfig if kind == "config" else node.moduleConfig, section)

    def _remote_redact(self, kind, section, values):
        if kind == "config":
            return _redact_section(section, values, REMOTE_SECRET_CONFIG_FIELDS)
        return _redact_section(section, values, REMOTE_SECRET_MODULE_FIELDS)

    def remote_admin_get(self, node_id, kind, section):
        with self.lock:
            node = self._remote_node(node_id)
            descriptor = self._section_descriptor(node, kind, section)
            node.requestConfig(descriptor)
            msg = self._section_message(node, kind, section)
            values = MessageToDict(
                msg,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )
            values = self._remote_redact(kind, section, values)
            self._log_remote_admin(node_id, f"get {kind}.{section}", "ok")
            return {"node_id": node_id, "kind": kind, "section": section, "values": values}

    def remote_admin_set(self, node_id, kind, section, values):
        if not isinstance(values, dict):
            raise ValueError("values must be a JSON object")
        with self.lock:
            node = self._remote_node(node_id)
            descriptor = self._section_descriptor(node, kind, section)
            # Read first, then merge. This prevents a partial form/JSON payload
            # from clearing fields we didn't render, same guard as local config.
            node.requestConfig(descriptor)
            if kind == "config":
                values = _strip_redacted(section, values, REMOTE_SECRET_CONFIG_FIELDS)
            else:
                values = _strip_redacted(section, values, REMOTE_SECRET_MODULE_FIELDS)
            msg = self._section_message(node, kind, section)
            ParseDict(values, msg, ignore_unknown_fields=True)
            node.writeConfig(section)
            self.iface.waitForAckNak()
            self._log_remote_admin(node_id, f"set {kind}.{section}", "ok", sorted(values.keys()))
            return {"node_id": node_id, "kind": kind, "section": section, "changed": sorted(values.keys())}

    def remote_admin_action(self, node_id, action, **kwargs):
        allowed = {
            "metadata", "reboot", "shutdown", "reset_nodedb",
            "factory_reset_config", "factory_reset_full",
        }
        if action not in allowed:
            raise ValueError("unknown remote admin action")
        with self.lock:
            node = self._remote_node(node_id)
            if action == "metadata":
                node.getMetadata()
            elif action == "reboot":
                node.reboot(int(kwargs.get("secs") or 10))
                self.iface.waitForAckNak()
            elif action == "shutdown":
                node.shutdown(int(kwargs.get("secs") or 10))
                self.iface.waitForAckNak()
            elif action == "reset_nodedb":
                node.resetNodeDb()
                self.iface.waitForAckNak()
            elif action == "factory_reset_config":
                node.factoryReset(full=False)
                self.iface.waitForAckNak()
            elif action == "factory_reset_full":
                node.factoryReset(full=True)
                self.iface.waitForAckNak()
            self._log_remote_admin(node_id, action, "ok")
            return {"node_id": node_id, "action": action, "ok": True}

    def set_config_section(self, section, values):
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            values = _strip_redacted(section, values, SECRET_CONFIG_FIELDS)
            node = self.iface.localNode
            sub = getattr(node.localConfig, section)
            # Merge, don't replace: a form only submits the fields it renders
            # (e.g. never the raw crypto keys), so Clear()-ing first would
            # wipe every field the form doesn't know about.
            ParseDict(values, sub, ignore_unknown_fields=True)
            node.writeConfig(section)

    def set_module_config_section(self, section, values):
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            values = _strip_redacted(section, values, SECRET_MODULE_FIELDS)
            node = self.iface.localNode
            sub = getattr(node.moduleConfig, section)
            ParseDict(values, sub, ignore_unknown_fields=True)
            node.writeConfig(section)

    # ---- traceroute -------------------------------------------------------
    # The library's own sendTraceRoute() blocks the calling thread until the
    # reply lands and prints the result to stdout, which is useless here (it
    # would hold the lock and stall every other request). This is the same
    # request sent asynchronously, with the reply parsed into a dict the UI
    # can poll for.

    def trace_route(self, dest, hop_limit=None):
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            if hop_limit is None:
                hop_limit = self.iface.localNode.localConfig.lora.hop_limit or 3
            route = mesh_pb2.RouteDiscovery()
            self.iface.sendData(
                route,
                destinationId=dest,
                portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
                wantResponse=True,
                onResponse=self._on_traceroute,
                channelIndex=0,
                hopLimit=hop_limit,
            )
        self.traceroutes[dest] = {
            "to": dest, "ts": time.time(), "status": "pending",
            "route": [], "route_back": [], "error": None,
        }
        return self.traceroutes[dest]

    def _node_label(self, num):
        """'!abcd1234 (Long Name)' for a node number, best effort."""
        node_id = f"!{num:08x}"
        try:
            node = (self.iface.nodes or {}).get(node_id)
            name = (node or {}).get("user", {}).get("longName")
        except Exception:
            name = None
        # snr is always present (None for the endpoints, which have no
        # per-hop SNR of their own) so consumers never have to probe for it.
        return {"id": node_id, "name": name or node_id, "snr": None}

    def _hops(self, nums, snrs):
        """Zip a route with its per-hop SNR list. -128 means 'unknown'; the
        wire format stores SNR in quarter-dB steps."""
        out = []
        for i, num in enumerate(nums or []):
            snr = None
            if snrs and i < len(snrs) and snrs[i] != -128:
                snr = snrs[i] / 4
            hop = self._node_label(num)
            hop["snr"] = snr
            out.append(hop)
        return out

    def _on_traceroute(self, packet):
        try:
            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")
            src = packet.get("fromId") or self._node_label(packet.get("from", 0))["id"]
        except (AttributeError, TypeError):
            return

        if portnum == "ROUTING_APP":
            reason = decoded.get("routing", {}).get("errorReason", "NONE")
            if reason != "NONE":
                entry = self.traceroutes.get(src)
                if entry:
                    entry.update(status="failed", error=reason)
                    if self.node_store:
                        self.node_store.record_traceroute(entry)
            return

        try:
            rd = mesh_pb2.RouteDiscovery()
            rd.ParseFromString(decoded["payload"])
        except Exception as e:
            logger.warning("Traceroute parse failed: %s", e)
            return

        entry = self.traceroutes.get(src) or {"to": src, "ts": time.time()}
        # Route towards the destination, then the path the reply took back.
        entry.update(
            status="ok",
            error=None,
            hops_there=len(rd.route),
            route=[self._node_label(self.my_node_num() or 0)] +
                  self._hops(list(rd.route), list(rd.snr_towards)) +
                  [self._node_label(packet.get("from", 0))],
            route_back=(
                [self._node_label(packet.get("from", 0))] +
                self._hops(list(rd.route_back), list(rd.snr_back)) +
                [self._node_label(self.my_node_num() or 0)]
            ) if rd.route_back or rd.snr_back else [],
            completed_ts=time.time(),
        )
        self.traceroutes[src] = entry
        if self.node_store:
            self.node_store.record_traceroute(entry)
        logger.info("Traceroute to %s: %d hops", src, len(rd.route))

    def _expire_traceroutes(self):
        now = time.time()
        for entry in self.traceroutes.values():
            if entry["status"] == "pending" and now - entry["ts"] > TRACEROUTE_TIMEOUT_SECS:
                entry.update(status="failed", error="TIMEOUT (no reply)")

    def get_traceroutes(self):
        return sorted(self.traceroutes.values(), key=lambda e: -e["ts"])

    def get_traceroute(self, dest):
        return self.traceroutes.get(dest)

    # ---- channels ---------------------------------------------------------

    def add_channel(self, name):
        """Same semantics as `meshtastic --ch-add`: first free slot, random
        256-bit key, SECONDARY role."""
        name = name.strip()
        if not name:
            raise ValueError("Channel name required")
        if len(name) > 10:
            raise ValueError("Channel name must be 10 characters or fewer")
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            node = self.iface.localNode
            if node.getChannelByName(name):
                raise ValueError(f"A channel named '{name}' already exists")
            ch = node.getDisabledChannel()
            if not ch:
                raise ValueError("No free channel slots (all 8 in use)")
            settings = channel_pb2.ChannelSettings()
            settings.psk = genPSK256()
            settings.name = name
            ch.settings.CopyFrom(settings)
            ch.role = channel_pb2.Channel.Role.SECONDARY
            node.writeChannel(ch.index)
            return {"index": ch.index, "name": name}

    def set_channel_enabled(self, index, enabled):
        """Toggle a secondary channel without destroying it — DISABLED keeps
        the slot and its key, so flipping it back on restores the channel
        exactly. Channel 0 is the primary and is never touchable this way."""
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            if index == 0:
                raise ValueError("Channel 0 is the primary channel and cannot be disabled")
            node = self.iface.localNode
            ch = node.channels[index]
            if enabled and not ch.settings.name and not ch.settings.psk:
                raise ValueError(f"Channel {index} is empty — nothing to enable")
            ch.role = (channel_pb2.Channel.Role.SECONDARY if enabled
                       else channel_pb2.Channel.Role.DISABLED)
            node.writeChannel(index)
            return {"index": index, "enabled": enabled}

    def set_fixed_position(self, lat, lon, alt=0):
        """Pin the node's position and turn on position.fixed_position. Needed
        because the onboard GPS gets no lock indoors — without this the node
        never appears on its own map and never reports a position to the mesh."""
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            self.iface.localNode.setFixedPosition(float(lat), float(lon), int(alt))
        # Keep the distance origin in step with the node's new fixed position.
        self.home_lat, self.home_lon = float(lat), float(lon)

    def get_channels(self):
        with self.lock:
            if not self.connected or not self.iface:
                return []
            out = []
            for c in self.iface.localNode.channels:
                d = MessageToDict(c, preserving_proto_field_name=True)
                if d.get("settings", {}).get("psk"):
                    d["settings"]["psk"] = REDACTED
                out.append(d)
            return out

    def set_channel(self, index, values):
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            node = self.iface.localNode
            ch = node.channels[index]
            # This is a wholesale replace (Clear + parse), which is what the
            # raw-JSON channels page expects — so an untouched REDACTED psk has
            # to be swapped back for the real key *before* the clear, or saving
            # any channel would silently destroy its encryption key.
            if values.get("settings", {}).get("psk") == REDACTED:
                values = dict(values)
                values["settings"] = dict(values["settings"])
                values["settings"]["psk"] = base64.b64encode(ch.settings.psk).decode()
            ch.Clear()
            ParseDict(values, ch, ignore_unknown_fields=True)
            # Clear() also zeroes ch.index, and writeChannel() below sends this
            # whole object over the air — the firmware picks the target slot
            # from *this* embedded field, not from the index argument. Without
            # this line a write meant for channel N silently lands on channel
            # 0 (index's zero value) instead, clobbering the primary channel.
            # Cost a live PRIMARY channel during testing before being caught.
            ch.index = index
            node.writeChannel(index)

    @staticmethod
    def _emoji_codepoint(emoji: str) -> int:
        """Return the first real Unicode codepoint for Meshtastic Data.emoji."""
        for ch in (emoji or ""):
            # Skip variation selectors Telegram includes on things like ❤️.
            if ord(ch) not in (0xFE0E, 0xFE0F):
                return ord(ch)
        raise ValueError("empty emoji")

    def send_reaction(self, target_pkt_id, emoji, channel_index=0, destination=None, from_id="me"):
        """Send a Meshtastic tapback/reaction to an existing packet id."""
        codepoint = self._emoji_codepoint(emoji)
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            mesh_packet = mesh_pb2.MeshPacket()
            mesh_packet.channel = channel_index
            mesh_packet.decoded.portnum = portnums_pb2.PortNum.REPLY_APP
            mesh_packet.decoded.reply_id = int(target_pkt_id)
            mesh_packet.decoded.emoji = codepoint
            mesh_packet.id = self.iface._generatePacketId()
            mesh_packet.priority = mesh_pb2.MeshPacket.Priority.RELIABLE
            self.iface._sendPacket(
                mesh_packet,
                destinationId=destination or BROADCAST_ADDR,
                wantAck=False,
            )
        self._record_reaction(target_pkt_id, from_id, chr(codepoint))

    def send_text(self, text, channel_index=0, destination=None, want_ack=True):
        """destination is a node id like '!19da16f5'; None means broadcast.

        want_ack=True (default for UI/DMs): hop-layer *routing* acks only.
        - DM: dest (or a relay) may send a ROUTING_APP ack with our requestId.
        - Broadcast: only nodes that *rebroadcast* tend to emit those acks.
          CLIENT_MUTE / phone / pure-receiver nodes can show the text and still
          produce zero acks. So "delivered" on a broadcast means "at least one
          rebroadcasting node hop-acked," NOT "every listener got the text."

        Ack matching is done in `_on_routing` via pubsub (every ROUTING packet),
        not MeshInterface.onResponse — the library pops its response handler on
        the first ACK/NAK, which permanently dropped later relay acks whenever
        a local MAX_RETRANSMIT arrived first (the main "nobody heard it" bug).

        want_ack=False: fire-and-forget (beacons / injectors). Status "sent"
        immediately; no retransmit thrash, no false hop-layer failures.
        """
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            # No onResponse — see docstring. wantAck still set so the radio
            # actually requests hop acks; we collect them on pubsub routing.
            packet = self.iface.sendData(
                text.encode("utf-8"),
                destinationId=destination or BROADCAST_ADDR,
                portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
                wantAck=bool(want_ack),
                channelIndex=channel_index,
            )
        self._record({
            "id": getattr(packet, "id", None),
            "ts": time.time(),
            "from": "me",
            "to": destination or BROADCAST_ADDR,
            "direct": bool(destination),
            "channel": channel_index,
            "via_mqtt": False,
            "text": text,
            "status": "sending" if want_ack else "sent",
            "status_reason": None,
            "heard_by": [],
        })

    def request_position(self, destination, channel_index=0):
        """Ask a node to send us its position now, over a specific channel.

        This is not just "don't wait for the next beacon". A node answers a
        request using the precision of the channel the request ARRIVED on —
        `handleReceivedProtobuf` sets the module's precision from the request
        packet's channel, and `allocReply` reuses it — while its routine
        beacon uses the first channel with non-zero precision, which for us is
        always the public one. So asking over an encrypted fleet channel set
        to 32 bits gets an exact fix back without the public channel ever
        carrying anything but the fuzzed position.

        The firmware throttles itself to one position reply per 3 minutes, so
        polling faster than that just burns airtime for nothing.

        Deliberately NOT iface.sendPosition(): that calls waitForPosition()
        when wantResponse is set, which would block the caller — and here it
        would do so while holding the client lock, stalling every other packet
        the client handles. The reply arrives as an ordinary position packet
        and gets picked up by _on_position like any other.
        """
        if not destination:
            raise ValueError("destination node id required")
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            return self.iface.sendData(
                mesh_pb2.Position(),
                destinationId=destination,
                portNum=portnums_pb2.PortNum.POSITION_APP,
                wantAck=False,
                wantResponse=True,
                channelIndex=channel_index,
            )

    def _on_routing(self, packet, interface=None):
        """Handle every ROUTING_APP packet (ACK/NAK) related to our sends.

        Pubsub topic: meshtastic.receive.routing. Unlike MeshInterface's
        onResponse (one-shot, handler popped after first packet), this sees
        *every* hop-layer result for a requestId — so broadcast hearers can
        accumulate and a late relay ack can still upgrade status after an
        early local MAX_RETRANSMIT.
        """
        try:
            decoded = packet.get("decoded") or {}
            request_id = decoded.get("requestId")
            if request_id is None:
                return
            routing = decoded.get("routing") or {}
            reason = routing.get("errorReason", "NONE")
            # dict.get footgun: fromId can be present but None
            heard_from = packet.get("fromId") or None
            if not heard_from:
                num = packet.get("from")
                heard_from = f"!{num:08x}" if num else None
            via_mqtt = bool(packet.get("viaMqtt"))
        except (AttributeError, TypeError):
            return

        msg = self._find_outbound(request_id)
        if msg is None:
            return  # not one of ours (or already rotated out of the deque)

        if reason == "NONE" or reason is None or reason == "":
            # Broadcast "implicit ACK" (firmware FloodingRouter): when anyone
            # rebroadcasts our flood packet, the radio synthesizes a local
            # ROUTING packet to the app with from=self (see slog:
            # "Rx someone rebroadcasting for us" → fr=local, Portnum=ROUTING).
            # Skipping those as "self-acks" was the main reason Ch0 always
            # showed no relay ack while DMs to Tower delivered fine.
            is_self = False
            if self.iface and self.iface.myInfo and heard_from:
                my_str = f"!{self.iface.myInfo.my_node_num:08x}"
                is_self = heard_from == my_str

            if is_self:
                if msg.get("direct"):
                    logger.debug("Skipping DM self-ack for %s", request_id)
                    return
                # Prefer relay_node when firmware stamps the rebroadcaster.
                relay = packet.get("relayNode") or packet.get("relay_node")
                if relay:
                    try:
                        heard_from = f"!{int(relay):08x}" if int(relay) > 255 else f"!relay:{int(relay):02x}"
                    except (TypeError, ValueError):
                        heard_from = "(mesh rebroadcast)"
                else:
                    heard_from = "(mesh rebroadcast)"
                self._add_heard(
                    request_id, heard_from, via_mqtt, implicit_broadcast=True
                )
                return

            if heard_from is None:
                return
            self._add_heard(request_id, heard_from, via_mqtt)
            return

        # --- NAK path ---
        # Never downgrade a real delivery (late NAK after third-party acks).
        if msg.get("status") == "delivered":
            logger.debug("Ignoring %s on already-delivered msg %s", reason, request_id)
            return
        # Already has hearers on a broadcast — keep the positive status.
        if not msg.get("direct") and msg.get("heard_by"):
            logger.debug(
                "Ignoring %s on broadcast %s that already has hearers",
                reason, request_id,
            )
            return

        # MAX_RETRANSMIT = local hop-layer retries exhausted. For broadcasts
        # this is almost always a false "failure" (receivers without
        # rebroadcast never hop-ack). For DMs it often means the dest never
        # confirmed — but a late dest ack can still arrive, so do not freeze
        # the status at failed; leave "sending" for the soft timeout (or a
        # later success via _add_heard).
        if reason == "MAX_RETRANSMIT":
            logger.info(
                "Hop MAX_RETRANSMIT for %s %s — not marking failed "
                "(waiting for relay/dest ack or timeout)",
                "DM" if msg.get("direct") else "broadcast",
                request_id,
            )
            return

        # Hard routing errors (NO_CHANNEL, NO_ROUTE, PKI, …) — real failures.
        self._set_message_status(request_id, "failed", reason)

    def _find_outbound(self, msg_id):
        """Return our outbound message dict for pkt id, or None."""
        for m in reversed(self.messages):
            if m.get("id") == msg_id and m.get("from") == "me":
                return m
        return None

    # Back-compat alias if anything still references the old name
    def _on_ack_nak(self, packet):
        self._on_routing(packet)

    def _add_heard(self, msg_id, node_id, via_mqtt=False, *, implicit_broadcast=False):
        for m in reversed(self.messages):
            if m.get("id") == msg_id:
                # Skip genuine self-acks on DMs ("I transmitted it").
                # Do NOT skip broadcast implicit ACKs — those arrive as from=self
                # by design (see _on_routing).
                if not implicit_broadcast and node_id and self.iface and self.iface.myInfo:
                    my_num = self.iface.myInfo.my_node_num
                    my_str = f"!{my_num:08x}" if my_num else None
                    if my_str and node_id == my_str:
                        logger.debug("Skipping self-ack from %s", node_id)
                        return
                heard = m.setdefault("heard_by", [])
                is_dest = bool(m.get("direct") and m.get("to") == node_id)
                if not any(h.get("id") == node_id for h in heard):
                    entry = {
                        "id": node_id,
                        "via_mqtt": via_mqtt,
                        "is_destination": is_dest,
                    }
                    if implicit_broadcast:
                        entry["implicit"] = True
                    heard.append(entry)

                # Status logic: for DMs, only "delivered" when the actual
                # destination acks.  Relay hops show "relayed" to distinguish
                # "message is moving through the mesh" from "dest got it."
                if is_dest:
                    m["status"] = "delivered"
                    m["status_reason"] = None
                elif m.get("direct") and m["status"] != "delivered":
                    m["status"] = "relayed"
                    m["status_reason"] = None
                else:
                    # Broadcast — hop-layer / implicit rebroadcast ack.
                    # Not proof that every listener (CLIENT_MUTE / phones) got it.
                    m["status"] = "delivered"
                    m["status_reason"] = None
                m["updated_ts"] = time.time()

                logger.info(
                    "Message %s heard by %s via %s%s%s (%d total)",
                    msg_id, node_id, "MQTT" if via_mqtt else "RF",
                    " (destination)" if is_dest else "",
                    " (implicit rebroadcast)" if implicit_broadcast else "",
                    len(heard),
                )
                if self.store is not None:
                    try:
                        self.store.update_status(msg_id, m["status"], None, heard_by=heard)
                    except Exception as e:
                        logger.warning("Could not persist heard_by: %s", e)

                break

    def _record_reaction(self, msg_id, from_id, emoji):
        if msg_id is None or not from_id or not emoji:
            return
        reactions = None
        for m in reversed(self.messages):
            if m.get("id") == msg_id:
                reactions = [r for r in (m.get("reactions") or []) if r.get("from") != from_id]
                reactions.append({"from": from_id, "emoji": emoji, "ts": time.time()})
                m["reactions"] = reactions
                m["updated_ts"] = time.time()
                break
        if self.store is not None:
            try:
                reactions = self.store.record_reaction(msg_id, from_id, emoji)
            except Exception as e:
                logger.warning("Could not persist reaction: %s", e)

    @staticmethod
    def _rf_meta_from_packet(packet):
        """Extract last-hop RF quality + hop count from a meshtastic packet dict.

        hops_away = hopStart - hopLimit (same formula as position_history).
        relay_node is the last forwarder when present (num or !hex), not a full path.
        """
        snr = packet.get("rxSnr")
        rssi = packet.get("rxRssi")
        hop_start = packet.get("hopStart")
        hop_limit = packet.get("hopLimit")
        hops = None
        if hop_start is not None and hop_limit is not None:
            try:
                hops = max(0, int(hop_start) - int(hop_limit))
            except (TypeError, ValueError):
                hops = None
        relay = packet.get("relayNode")
        relay_id = None
        if relay is not None and relay != 0 and relay != "":
            if isinstance(relay, str) and relay.startswith("!"):
                relay_id = relay
            else:
                try:
                    relay_id = f"!{int(relay):08x}"
                except (TypeError, ValueError):
                    relay_id = str(relay)
        return {
            "snr": snr,
            "rssi": rssi,
            "hops_away": hops,
            "relay_node": relay_id,
        }

    def _record(self, msg):
        """Single funnel for every message in or out — keeps the in-memory
        deque and the on-disk history from drifting apart."""
        self.messages.append(msg)
        if self.store is not None:
            try:
                self.store.add(msg)
            except Exception as e:
                logger.warning("Could not persist message: %s", e)

    def _set_message_status(self, msg_id, status, reason=None):
        for m in reversed(self.messages):
            if m.get("id") == msg_id:
                m["status"] = status
                m["status_reason"] = reason
                m["updated_ts"] = time.time()
                logger.info("Message %s -> %s%s", msg_id, status,
                            f" ({reason})" if reason else "")
                break
        if self.store is not None:
            try:
                self.store.update_status(msg_id, status, reason)
            except Exception as e:
                logger.warning("Could not persist message status: %s", e)

    def _expire_pending_acks(self):
        """A send whose ack never arrives shouldn't spin forever — after the
        radio has stopped retrying, call it unacked rather than in-flight."""
        now = time.time()
        for m in self.messages:
            if m.get("status") == "sending" and now - m["ts"] > MESSAGE_ACK_TIMEOUT_SECS:
                m["status"] = "no_ack"
                m["status_reason"] = m.get("status_reason") or "ack timeout"
                m["updated_ts"] = now
                if self.store is not None and m.get("id") is not None:
                    try:
                        self.store.update_status(m.get("id"), "no_ack", m.get("status_reason"), heard_by=m.get("heard_by") or [])
                    except Exception as e:
                        logger.warning("Could not persist expired pending ack: %s", e)

    def get_messages(self):
        return list(self.messages)

    def reboot(self):
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            self.iface.localNode.reboot()

    def shutdown_device(self):
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            self.iface.localNode.shutdown()

    def factory_reset(self):
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            self.iface.localNode.factoryReset()
