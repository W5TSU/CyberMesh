import base64
import logging
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
                 home_lat=None, home_lon=None, store=None, node_store=None):
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
        self.traceroutes = {}  # node id -> latest result
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
        pub.subscribe(self._on_receive_text, "meshtastic.receive.text")
        pub.subscribe(self._on_any_receive, "meshtastic.receive")
        pub.subscribe(self._on_telemetry, "meshtastic.receive.telemetry")
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
        self._record({
            "id": packet.get("id"),
            "ts": time.time(),
            "from": fromId,
            "to": toId,
            # A packet addressed to us specifically rather than to ^all is a
            # direct message — the UI threads those separately.
            "direct": bool(toId) and toId != BROADCAST_ADDR,
            "channel": channel,
            "via_mqtt": bool(packet.get("viaMqtt")),
            "text": text,
            "status": "received",
            "status_reason": None,
        })

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

    def send_text(self, text, channel_index=0, destination=None):
        """destination is a node id like '!19da16f5'; None means broadcast.

        Both directs and broadcasts now go out with wantAck. A direct message
        has one recipient, so its ack is the whole story. A broadcast has no
        single recipient — instead, every node that hears it and rebroadcasts
        (hop_limit > 0) sends its own implicit ack back to us, from its own
        node id rather than the broadcast address. That's the only way to
        answer "did anyone hear this Ch0 transmission," so the extra airtime
        the ack request costs is worth it.

        sendData is used rather than sendText purely because sendText doesn't
        forward onResponseAckPermitted, and without that the library only
        delivers NAKs to the callback and swallows the successful acks.
        """
        with self.lock:
            if not self.connected or not self.iface:
                raise RuntimeError("Not connected")
            packet = self.iface.sendData(
                text.encode("utf-8"),
                destinationId=destination or BROADCAST_ADDR,
                portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
                wantAck=True,
                onResponse=self._on_ack_nak,
                onResponseAckPermitted=True,
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
            "status": "sending",
            "status_reason": None,
            "heard_by": [],
        })

    def _on_ack_nak(self, packet):
        """Delivery result for a message we sent, direct or broadcast.

        A direct message has one recipient, so its first ack is the whole
        story. A broadcast's acks accumulate — a different node can report in
        every time it relays the packet, sometimes seconds apart — so this
        only ever adds to heard_by, never overwrites it, and a message can
        keep gaining acks after it first shows "delivered."
        """
        try:
            decoded = packet.get("decoded", {})
            request_id = decoded.get("requestId")
            reason = decoded.get("routing", {}).get("errorReason", "NONE")
            # Same dict.get() footgun fixed in _on_receive_text: "fromId" can
            # be present but None, which silently defeats a .get(k, default).
            heard_from = packet.get("fromId") or None
            if not heard_from:
                num = packet.get("from")
                heard_from = f"!{num:08x}" if num else None
            via_mqtt = bool(packet.get("viaMqtt"))
        except (AttributeError, TypeError):
            return
        if request_id is None or heard_from is None:
            return
        if reason != "NONE":
            self._set_message_status(request_id, "failed", reason)
            return
        self._add_heard(request_id, heard_from, via_mqtt)

    def _add_heard(self, msg_id, node_id, via_mqtt=False):
        for m in reversed(self.messages):
            if m.get("id") == msg_id:
                heard = m.setdefault("heard_by", [])
                if not any(h.get("id") == node_id for h in heard):
                    heard.append({"id": node_id, "via_mqtt": via_mqtt})
                m["status"] = "delivered"
                m["status_reason"] = None
                logger.info("Message %s heard by %s via %s (%d total)",
                            msg_id, node_id, "MQTT" if via_mqtt else "RF", len(heard))
                if self.store is not None:
                    try:
                        self.store.update_status(msg_id, "delivered", None, heard_by=heard)
                    except Exception as e:
                        logger.warning("Could not persist heard_by: %s", e)
                break

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
