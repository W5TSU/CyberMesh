"""Mesh topology — trace a route to every known node and turn the results into
a node-link graph.

Every hop in a completed traceroute is a radio link someone actually used, with
the SNR the receiving end measured. Stitching the latest trace per node
together gives a picture of which nodes hear which, built from the network's
own reports rather than guessed from positions.

A sweep is deliberately one trace at a time with a gap between them. Each
traceroute is flooded across the mesh, so firing them all at once would eat
airtime for everyone on the channel, and the firmware drops traceroute
requests that arrive faster than it's willing to answer them anyway.
"""

import logging
import threading
import time

from mesh_client import TRACEROUTE_TIMEOUT_SECS

logger = logging.getLogger(__name__)

# Firmware and the phone apps both throttle traceroutes to about one per 30s;
# anything faster just produces rate-limited failures.
MIN_GAP_SECS = 30

# Relays that don't understand traceroute (or hide themselves) show up in the
# route as the broadcast address. There's no real node there to draw.
UNKNOWN_HOP = "!ffffffff"


def _edge_key(a, b):
    return (a, b) if a < b else (b, a)


def build_graph(traces, nodes, my_id=None, since_ts=None):
    """Combine traceroutes and the node list into {"nodes": [...], "links": [...]}.

    `traces` is newest-first traceroute history; only the latest per
    destination counts, so a node that moved or lost a relay isn't drawn with
    links it no longer has. Nodes we hear directly over RF (hopsAway 0) get a
    link to us with their last-packet SNR even without a trace, since that
    link is observed for free.

    Each link's SNR is its best reading in either direction: in a route list
    [me, h1, ..., dest], hop i's `snr` is what hop i measured receiving from
    hop i-1.
    """
    by_id = {}
    for n in nodes or []:
        user = n.get("user") or {}
        node_id = user.get("id")
        if not node_id:
            continue
        metrics = n.get("deviceMetrics") or {}
        by_id[node_id] = {
            "id": node_id,
            "name": user.get("longName") or node_id,
            "short": user.get("shortName") or node_id[-4:],
            "hw": user.get("hwModel"),
            "role": user.get("role") or "CLIENT",
            "battery": metrics.get("batteryLevel"),
            "hops_away": n.get("hopsAway"),
            "last_heard": n.get("lastHeard"),
            "via_mqtt": bool(n.get("viaMqtt")),
            "is_me": node_id == my_id,
            "trace": None,
        }

    def ensure(hop):
        node_id = hop.get("id")
        if node_id not in by_id:
            by_id[node_id] = {
                "id": node_id, "name": hop.get("name") or node_id,
                "short": node_id[-4:], "hw": None, "role": None, "battery": None,
                "hops_away": None, "last_heard": None, "via_mqtt": False,
                "is_me": node_id == my_id, "trace": None,
            }
        return by_id[node_id]

    links = {}

    def add_link(a, b, snr, source, ts):
        if a == b or UNKNOWN_HOP in (a, b):
            return
        link = links.setdefault(_edge_key(a, b), {
            "source": _edge_key(a, b)[0], "target": _edge_key(a, b)[1],
            "snr": None, "seen": 0, "kinds": set(), "ts": None,
        })
        link["seen"] += 1
        link["kinds"].add(source)
        if snr is not None and (link["snr"] is None or snr > link["snr"]):
            link["snr"] = snr
        if ts and (link["ts"] is None or ts > link["ts"]):
            link["ts"] = ts

    latest = {}
    for t in traces or []:
        if since_ts and (t.get("ts") or 0) < since_ts:
            continue
        # Newest first, so the first entry per destination is the latest.
        latest.setdefault(t.get("to"), t)

    for dest, t in latest.items():
        if not dest:
            continue
        ensure({"id": dest})["trace"] = t.get("status")
        if t.get("status") != "ok":
            continue
        for path in (t.get("route") or [], t.get("route_back") or []):
            for prev, hop in zip(path, path[1:]):
                if UNKNOWN_HOP in (prev.get("id"), hop.get("id")):
                    continue
                ensure(prev)
                ensure(hop)
                add_link(prev["id"], hop["id"], hop.get("snr"), "trace", t.get("ts"))

    if my_id:
        me = by_id.get(my_id)
        if me:
            me["is_me"] = True
        for n in nodes or []:
            node_id = (n.get("user") or {}).get("id")
            if (node_id and node_id != my_id and n.get("hopsAway") == 0
                    and not n.get("viaMqtt") and n.get("snr") is not None):
                if since_ts and (n.get("lastHeard") or 0) < since_ts:
                    continue
                add_link(my_id, node_id, n["snr"], "heard", n.get("lastHeard"))

    # Only draw nodes that are part of the picture: linked, traced, or us.
    linked = {l["source"] for l in links.values()} | {l["target"] for l in links.values()}
    out_nodes = [n for n in by_id.values() if n["id"] in linked or n["trace"] or n["is_me"]]
    out_links = []
    for link in links.values():
        link["kinds"] = sorted(link["kinds"])
        out_links.append(link)
    return {"nodes": out_nodes, "links": out_links}


class TopologySweep:
    """Traceroute every recently heard RF node, one at a time, in a thread."""

    def __init__(self, client, gap_secs=MIN_GAP_SECS, reply_timeout=TRACEROUTE_TIMEOUT_SECS):
        self.client = client
        self.gap_secs = max(MIN_GAP_SECS, gap_secs)
        self.reply_timeout = reply_timeout
        self._lock = threading.Lock()
        self._thread = None
        self._cancel = threading.Event()
        self._state = {
            "running": False, "started": None, "finished": None, "cancelled": False,
            "total": 0, "done": 0, "ok": 0, "failed": 0, "current": None, "max_age_hours": None,
        }

    def status(self):
        with self._lock:
            return dict(self._state)

    def targets(self, max_age_hours):
        """Nodes worth tracing: heard over RF recently, nearest first.

        MQTT-only nodes are skipped because a trace to them can't show RF
        links, and long-silent nodes because they almost certainly won't
        answer — each dead trace still floods the mesh.
        """
        my_num = self.client.my_node_num()
        my_id = f"!{my_num:08x}" if my_num is not None else None
        cutoff = time.time() - max_age_hours * 3600 if max_age_hours else None
        out = []
        for n in self.client.node_list():
            node_id = (n.get("user") or {}).get("id")
            if not node_id or node_id == my_id or n.get("viaMqtt"):
                continue
            if cutoff and (n.get("lastHeard") or 0) < cutoff:
                continue
            out.append(n)
        out.sort(key=lambda n: (n.get("hopsAway") if n.get("hopsAway") is not None else 99,
                                -(n.get("lastHeard") or 0)))
        return [(n["user"]["id"], (n.get("user") or {}).get("longName")) for n in out]

    def start(self, max_age_hours=24):
        with self._lock:
            if self._state["running"]:
                return False
            targets = self.targets(max_age_hours)
            self._cancel.clear()
            self._state.update(
                running=True, started=time.time(), finished=None, cancelled=False,
                total=len(targets), done=0, ok=0, failed=0, current=None,
                max_age_hours=max_age_hours,
            )
            self._thread = threading.Thread(target=self._run, args=(targets,), daemon=True)
            self._thread.start()
        logger.info("Topology sweep started: %d nodes", len(targets))
        return True

    def cancel(self):
        self._cancel.set()

    def _wait_for(self, node_id, sent_at):
        deadline = sent_at + self.reply_timeout
        while time.time() < deadline and not self._cancel.is_set():
            entry = self.client.get_traceroute(node_id) or {}
            if entry.get("status") in ("ok", "failed") and (entry.get("ts") or 0) >= sent_at - 1:
                return entry["status"]
            time.sleep(2)
        return "failed"

    def _run(self, targets):
        try:
            for node_id, name in targets:
                if self._cancel.is_set():
                    break
                with self._lock:
                    self._state["current"] = {"id": node_id, "name": name or node_id}
                sent_at = time.time()
                try:
                    self.client.trace_route(node_id)
                    result = self._wait_for(node_id, sent_at)
                except Exception as e:
                    logger.warning("topology trace to %s failed to send: %s", node_id, e)
                    result = "failed"
                with self._lock:
                    self._state["done"] += 1
                    self._state[result] += 1
                # Keep the gap between sends, not between replies, so a fast
                # answer doesn't let the next request go out early.
                remaining = self.gap_secs - (time.time() - sent_at)
                if remaining > 0:
                    self._cancel.wait(remaining)
        except Exception:
            logger.exception("topology sweep error")
        finally:
            with self._lock:
                self._state.update(running=False, finished=time.time(), current=None,
                                   cancelled=self._cancel.is_set())
            logger.info("Topology sweep finished: %s", self.status())
