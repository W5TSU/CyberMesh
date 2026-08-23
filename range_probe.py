"""Range probing — active coverage testing for a mobile node.

Passive position logging can only ever show where a node WAS heard. Silence is
ambiguous: out of range, or just not beaconing? This asks the question directly
instead. Each probe is a position request sent over the fleet channel, so a
reply is a confirmed round trip and a timeout is a real negative result — the
one thing passive logging can never produce.

Two properties make it cheap enough to leave running:

* The reply is fuzzed to the precision of the channel the REQUEST arrived on,
  so probing over CyberMesh (32 bits) returns an exact fix while the node's
  routine beacon on the public channel stays coarse. No privacy tradeoff.

* Probing is motion-gated using the node's own beacons, which cost us nothing.
  Meshtastic's smart-position broadcast fires when the node moves a minimum
  distance, subject to a minimum interval — so beacons arriving CLOSE TOGETHER
  mean movement, and a node parked on a desk falls back to its much slower
  forced interval. That difference is the motion sensor: no accelerometer, no
  extra airtime, and it self-calibrates to whatever the node's own config is.

Probing continues for a linger period after the last beacon, deliberately. That
window is where the interesting data is: when the node walks out of range the
beacons stop, and if we stopped with them we would never record the misses that
mark the edge of coverage.
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)

# The firmware answers at most one position request per 3 minutes, so anything
# faster is airtime spent on a guaranteed non-answer.
MIN_POLL_SECS = 200

# Beyond this, a reported position is old enough that the node has plausibly
# moved since — walking pace covers ~100m in 90s, which is the scale the whole
# coverage map works at. Points past it are flagged, not discarded: they're
# still proof the RF path worked, just not proof of *where*.
STALE_FIX_SECS = 90


class RangeProbe(threading.Thread):
    def __init__(self, client, node_store, node_id, channel=2, poll_secs=300,
                 motion_window=900, linger_secs=1200, reply_timeout=90):
        super().__init__(daemon=True)
        self.client = client
        self.node_store = node_store
        self.node_id = node_id
        self.channel = channel
        self.poll_secs = max(MIN_POLL_SECS, poll_secs)
        self.motion_window = motion_window
        self.linger_secs = linger_secs
        self.reply_timeout = reply_timeout
        self._stop = False
        self.last_poll = None
        self.last_result = None
        self.last_motion = None
        self.consecutive_misses = 0

    # -- state -------------------------------------------------------------

    @property
    def enabled(self):
        return self.node_store.get_setting("range_probe_enabled", "0") == "1"

    def set_enabled(self, on):
        self.node_store.set_setting("range_probe_enabled", "1" if on else "0")
        if on:
            # Don't make the user wait a full interval to see the first probe.
            self.last_poll = None

    def _motion_ts(self):
        """When we last saw evidence the node is moving: two of its own
        beacons arriving closer together than the motion window.

        The probe channel is excluded, and that exclusion is load-bearing.
        Our own probe replies are logged as positions like any other, and they
        arrive on the probe channel at the poll interval — which is inside the
        motion window by construction. Counting them would make every probe
        prove that another probe is warranted, and the gate would latch on
        forever after a single trip out. Only unsolicited beacons count.
        """
        since = time.time() - self.linger_secs - self.motion_window
        stamps = self.node_store.position_timestamps(
            self.node_id, since, exclude_channel=self.channel)
        latest = None
        for a, b in zip(stamps, stamps[1:]):
            if b - a <= self.motion_window:
                latest = b
        return latest

    def status(self):
        motion = self.last_motion
        active = bool(motion and time.time() - motion < self.linger_secs)
        return {
            "enabled": self.enabled,
            "node_id": self.node_id,
            "channel": self.channel,
            "active": active,
            "state": "off" if not self.enabled else ("probing" if active else "parked"),
            "poll_secs": self.poll_secs,
            "last_poll": self.last_poll,
            "last_result": self.last_result,
            "last_motion": motion,
            "consecutive_misses": self.consecutive_misses,
            "next_poll": (self.last_poll + self.poll_secs) if (self.last_poll and active) else None,
        }

    # -- probing -----------------------------------------------------------

    def probe_once(self):
        """Send one request and wait for the answer. Returns the recorded row.

        A miss still gets a position — the node's last known one — because a
        failed probe is only useful if you know roughly where it failed. It's
        flagged `stale_fix` so the map can draw it as an approximate marker
        rather than pretending it's a measured point.
        """
        sent_at = time.time()
        try:
            self.client.request_position(self.node_id, self.channel)
        except Exception as e:
            logger.warning("range probe send failed: %s", e)
            return None

        deadline = sent_at + self.reply_timeout
        reply = None
        while time.time() < deadline and not self._stop:
            reply = self.node_store.position_since(self.node_id, sent_at, channel=self.channel)
            if reply:
                break
            time.sleep(2)

        self.last_poll = sent_at
        if reply:
            self.consecutive_misses = 0
            # How old the fix itself was when it reached us. A hit is only a
            # trustworthy coverage point if the position is roughly current —
            # a duty-cycled GPS answers with its last fix, which can predate
            # the probe by the whole update interval.
            pos_time = reply.get("pos_time")
            fix_age = (reply["ts"] - pos_time) if pos_time else None
            entry = {
                "ts": sent_at, "node_id": self.node_id, "channel": self.channel,
                "result": "hit", "latency": reply["ts"] - sent_at,
                "lat": reply["lat"], "lon": reply["lon"],
                "stale_fix": bool(fix_age and fix_age > STALE_FIX_SECS),
                "fix_age": fix_age,
                "snr": reply.get("snr"), "rssi": reply.get("rssi"),
                "hops_away": reply.get("hops_away"),
            }
        else:
            # Distinguish "the radio link failed" from "the link was fine, the
            # node just has no GPS fix to report". Both look like silence to a
            # position-only check, but they mean opposite things for coverage:
            # a fixless reply is PROOF the path worked. Counting those as
            # misses paints phantom coverage holes wherever the GPS is cold —
            # which is exactly when you're most likely to be out testing.
            fixless = (self.client.last_fixless_position or {}).get(self.node_id)
            answered = bool(fixless and fixless >= sent_at)
            last = self.node_store.latest_position(self.node_id) or {}
            entry = {
                "ts": sent_at, "node_id": self.node_id, "channel": self.channel,
                "result": "nofix" if answered else "miss",
                "latency": (fixless - sent_at) if answered else None,
                "lat": last.get("lat"), "lon": last.get("lon"), "stale_fix": True,
            }
            self.consecutive_misses = 0 if answered else self.consecutive_misses + 1
        self.node_store.record_probe(entry)
        self.last_result = entry["result"]
        logger.info("range probe %s (%s)", entry["result"],
                    f"{entry['latency']:.1f}s" if entry.get("latency") else "timeout")
        return entry

    def run(self):
        while not self._stop:
            try:
                if self.enabled:
                    self.last_motion = self._motion_ts()
                    moving = bool(self.last_motion and
                                  time.time() - self.last_motion < self.linger_secs)
                    due = self.last_poll is None or \
                        time.time() - self.last_poll >= self.poll_secs
                    if moving and due:
                        self.probe_once()
            except Exception:
                logger.exception("range probe loop error")
            # Short tick so a toggle or a fresh beacon is picked up promptly;
            # the real pacing is the poll interval and the motion gate.
            for _ in range(15):
                if self._stop:
                    return
                time.sleep(1)

    def stop(self):
        self._stop = True


def create_probe(client, node_store, node_id, **kwargs):
    if not node_id:
        logger.info("Range probe disabled: no RANGE_PROBE_NODE configured")
        return None
    probe = RangeProbe(client, node_store, node_id, **kwargs)
    probe.start()
    logger.info("Range probe ready for %s on ch%s (%ss interval, motion-gated)",
                node_id, probe.channel, probe.poll_secs)
    return probe
