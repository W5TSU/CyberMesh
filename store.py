"""On-disk message history.

The in-memory deque loses every conversation on a service restart, which
happens on any config change or deploy. This keeps the same records in SQLite
alongside the app so threads survive restarts and reboots.

Deliberately its own connection per call with check_same_thread=False off:
messages arrive on the meshtastic receive thread, are sent from Flask worker
threads, and status updates land on a third — one shared connection would need
its own locking for no benefit at this volume.
"""
import json
import logging
import os
import sqlite3
import threading
import time

logger = logging.getLogger("cybermesh.store")

DEFAULT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "messages.db")
MESSAGE_ACK_TIMEOUT_SECS = 90

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    pkt_id        INTEGER,
    ts            REAL NOT NULL,
    updated_ts    REAL,
    from_id       TEXT,
    to_id         TEXT,
    direct        INTEGER NOT NULL DEFAULT 0,
    channel       INTEGER NOT NULL DEFAULT 0,
    via_mqtt      INTEGER NOT NULL DEFAULT 0,
    text          TEXT NOT NULL,
    status        TEXT,
    status_reason TEXT,
    heard_by      TEXT,
    reactions     TEXT,
    PRIMARY KEY (pkt_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (ts);
CREATE TABLE IF NOT EXISTS node_names (
    node_id    TEXT PRIMARY KEY,
    long_name  TEXT,
    short_name TEXT,
    updated_ts REAL NOT NULL
);
"""


class MessageStore:
    def __init__(self, path=DEFAULT_PATH, limit=2000):
        self.path = path
        self.limit = limit
        self.lock = threading.Lock()
        with self._conn() as c:
            c.executescript(SCHEMA)
            # CREATE TABLE IF NOT EXISTS is a no-op on a db that predates this
            # column, so add it by hand for anyone upgrading in place.
            cols = [r[1] for r in c.execute("PRAGMA table_info(messages)")]
            if "updated_ts" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN updated_ts REAL")
            if "heard_by" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN heard_by TEXT")
            if "reactions" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN reactions TEXT")
            # RF path metadata (last-hop quality + hop count) — not a full route.
            if "snr" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN snr REAL")
            if "rssi" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN rssi REAL")
            if "hops_away" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN hops_away INTEGER")
            if "relay_node" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN relay_node TEXT")
            c.execute("UPDATE messages SET updated_ts = COALESCE(updated_ts, ts)")
            cutoff = time.time() - MESSAGE_ACK_TIMEOUT_SECS
            c.execute(
                "UPDATE messages "
                "SET status = 'no_ack', status_reason = COALESCE(status_reason, 'ack timeout'), updated_ts = ? "
                "WHERE status = 'sending' AND ts < ?",
                (time.time(), cutoff),
            )
        logger.info("Message store at %s", self.path)

    def record_node_name(self, node_id, long_name, short_name=None):
        """Persist a node's name so we can resolve it even when the node is
        offline and no longer in the mesh's live node DB."""
        if not node_id or not long_name:
            return
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO node_names (node_id, long_name, short_name, updated_ts) "
                "VALUES (?,?,?,?)",
                (node_id, long_name, short_name, time.time()),
            )

    def backfill_names(self, node_map):
        """Seed the node_names cache from a live meshtastic iface.nodes dict.
        Called once at startup after the first connection is established."""
        if not node_map:
            return
        now = time.time()
        with self.lock, self._conn() as c:
            for node_id, node in node_map.items():
                user = node.get("user") or {}
                if user.get("longName"):
                    c.execute(
                        "INSERT OR REPLACE INTO node_names (node_id, long_name, short_name, updated_ts) "
                        "VALUES (?,?,?,?)",
                        (node_id, user["longName"], user.get("shortName"), now),
                    )
            # Also backfill from_id → name for any message whose sender isn't
            # in the live node map but might be in our DB already
            c.execute("""INSERT OR IGNORE INTO node_names (node_id, long_name, short_name, updated_ts)
                SELECT DISTINCT m.from_id, n.long_name, n.short_name, ?
                FROM messages m
                INNER JOIN node_names n ON n.node_id = m.from_id
                WHERE m.from_id IS NOT NULL AND m.from_id != 'me'
            """, (now,))

    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def add(self, msg):
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO messages "
                "(pkt_id, ts, updated_ts, from_id, to_id, direct, channel, via_mqtt, text, "
                " status, status_reason, heard_by, reactions, snr, rssi, hops_away, relay_node) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (msg.get("id"), msg["ts"], msg.get("updated_ts", msg["ts"]), msg.get("from"), msg.get("to"),
                 int(bool(msg.get("direct"))), msg.get("channel", 0),
                 int(bool(msg.get("via_mqtt"))), msg["text"],
                 msg.get("status"), msg.get("status_reason"),
                 json.dumps(msg.get("heard_by") or []),
                 json.dumps(msg.get("reactions") or []),
                 msg.get("snr"), msg.get("rssi"), msg.get("hops_away"),
                 msg.get("relay_node")),
            )

    def update_status(self, pkt_id, status, reason=None, heard_by=None):
        if pkt_id is None:
            return
        now = time.time()
        with self.lock, self._conn() as c:
            if heard_by is not None:
                c.execute(
                    "UPDATE messages SET status = ?, status_reason = ?, heard_by = ?, updated_ts = ? WHERE pkt_id = ?",
                    (status, reason, json.dumps(heard_by), now, pkt_id),
                )
            else:
                c.execute(
                    "UPDATE messages SET status = ?, status_reason = ?, updated_ts = ? WHERE pkt_id = ?",
                    (status, reason, now, pkt_id),
                )

    def recent(self, limit=None):
        """Oldest-first, so the UI can append straight into a conversation.
        Resolves from_name from the durable node_names cache so every
        message carries a human-readable sender name."""
        limit = limit or self.limit
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT m.*, n.long_name as from_name, n.short_name as from_short "
                "FROM messages m "
                "LEFT JOIN node_names n ON n.node_id = m.from_id "
                "ORDER BY m.ts DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_message(r) for r in reversed(rows)]

    def recent_since(self, ts, limit=None):
        limit = limit or self.limit
        with self.lock, self._conn() as c:
            new_rows = c.execute(
                "SELECT m.*, n.long_name as from_name, n.short_name as from_short "
                "FROM messages m "
                "LEFT JOIN node_names n ON n.node_id = m.from_id "
                "WHERE m.ts > ? "
                "ORDER BY m.ts ASC LIMIT ?",
                (ts, limit),
            ).fetchall()
            update_rows = c.execute(
                "SELECT m.*, n.long_name as from_name, n.short_name as from_short "
                "FROM messages m "
                "LEFT JOIN node_names n ON n.node_id = m.from_id "
                "WHERE m.ts <= ? AND COALESCE(m.updated_ts, m.ts) > ? "
                "ORDER BY COALESCE(m.updated_ts, m.ts) ASC LIMIT ?",
                (ts, ts, limit),
            ).fetchall()
        return {
            "new": [self._row_to_message(r) for r in new_rows],
            "updates": [self._row_to_message(r) for r in update_rows],
        }

    def last_message(self, direct=None, channel=None, exclude_self=False):
        """Most recent message matching the given filters, or None. Used by
        the CyberHUD MQTT publisher for "last CH0 message" / "last DM"."""
        sql = ("SELECT m.*, n.long_name as from_name, n.short_name as from_short "
               "FROM messages m LEFT JOIN node_names n ON n.node_id = m.from_id WHERE 1=1")
        params = []
        if direct is not None:
            sql += " AND m.direct = ?"
            params.append(int(direct))
        if channel is not None:
            sql += " AND m.channel = ?"
            params.append(channel)
        if exclude_self:
            sql += " AND m.from_id != 'me'"
        sql += " ORDER BY m.ts DESC LIMIT 1"
        with self.lock, self._conn() as c:
            row = c.execute(sql, params).fetchone()
        return self._row_to_message(row) if row else None

    def recent_ack_rate(self, window_s=7200, min_samples=3):
        """Percent of our own outbound sends that got acked, over a trailing
        window. None until there are enough samples to be meaningful — a
        single lucky/unlucky send shouldn't swing a displayed percentage."""
        cutoff = time.time() - window_s
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT status FROM messages "
                "WHERE from_id = 'me' AND status IN ('delivered', 'no_ack') AND ts >= ? "
                "ORDER BY ts DESC LIMIT 50",
                (cutoff,),
            ).fetchall()
        if len(rows) < min_samples:
            return None
        delivered = sum(1 for r in rows if r["status"] == "delivered")
        return round(100.0 * delivered / len(rows))

    def _row_to_message(self, r):
        keys = r.keys()
        return {
            "id": r["pkt_id"],
            "ts": r["ts"],
            "updated_ts": r["updated_ts"] or r["ts"],
            "from": r["from_id"],
            "from_name": r["from_name"] or None,
            "to": r["to_id"],
            "direct": bool(r["direct"]),
            "channel": r["channel"],
            "via_mqtt": bool(r["via_mqtt"]),
            "text": r["text"],
            "status": r["status"],
            "status_reason": r["status_reason"],
            "heard_by": json.loads(r["heard_by"]) if r["heard_by"] else [],
            "reactions": json.loads(r["reactions"]) if "reactions" in keys and r["reactions"] else [],
            "snr": r["snr"] if "snr" in keys else None,
            "rssi": r["rssi"] if "rssi" in keys else None,
            "hops_away": r["hops_away"] if "hops_away" in keys else None,
            "relay_node": r["relay_node"] if "relay_node" in keys else None,
        }

    def record_reaction(self, pkt_id, from_id, emoji):
        """Attach or update one node's emoji tapback on an existing message."""
        if pkt_id is None or not from_id or not emoji:
            return []
        with self.lock, self._conn() as c:
            row = c.execute("SELECT reactions FROM messages WHERE pkt_id = ? ORDER BY ts DESC LIMIT 1", (pkt_id,)).fetchone()
            reactions = json.loads(row["reactions"]) if row and row["reactions"] else []
            reactions = [r for r in reactions if r.get("from") != from_id]
            reactions.append({"from": from_id, "emoji": emoji, "ts": time.time()})
            c.execute(
                "UPDATE messages SET reactions = ?, updated_ts = ? WHERE pkt_id = ?",
                (json.dumps(reactions), time.time(), pkt_id),
            )
            return reactions

    def name_map(self):
        """Return a dict of node_id -> display_name for every node we've
        ever seen. Used by the frontend to keep showing friendly names
        even when a node is no longer in the live mesh DB."""
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT node_id, long_name, short_name FROM node_names"
            ).fetchall()
        return {
            r["node_id"]: (r["short_name"] + " " if r["short_name"] else "") + r["long_name"]
            for r in rows
        }

    def prune(self, keep=None):
        keep = keep or self.limit
        with self.lock, self._conn() as c:
            c.execute(
                "DELETE FROM messages WHERE rowid NOT IN "
                "(SELECT rowid FROM messages ORDER BY ts DESC LIMIT ?)", (keep,)
            )


NODE_SCHEMA = """
CREATE TABLE IF NOT EXISTS node_meta (
    node_id    TEXT PRIMARY KEY,
    favorite   INTEGER NOT NULL DEFAULT 0,
    notes      TEXT,
    updated_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS saved_traceroutes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id    TEXT NOT NULL,
    ts         REAL NOT NULL,
    route      TEXT NOT NULL,
    route_back TEXT,
    hops_there INTEGER,
    note       TEXT
);
CREATE INDEX IF NOT EXISTS idx_saved_traceroutes_node ON saved_traceroutes (node_id);
CREATE TABLE IF NOT EXISTS traceroute_history (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id    TEXT NOT NULL,
    ts         REAL NOT NULL,
    status     TEXT NOT NULL,
    route      TEXT,
    route_back TEXT,
    hops_there INTEGER,
    error      TEXT
);
CREATE INDEX IF NOT EXISTS idx_traceroute_history_node ON traceroute_history (node_id);
CREATE TABLE IF NOT EXISTS telemetry_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                  REAL NOT NULL,
    channel_utilization REAL,
    air_util_tx         REAL,
    battery_level       INTEGER,
    voltage             REAL
);
CREATE INDEX IF NOT EXISTS idx_telemetry_history_ts ON telemetry_history (ts);
CREATE TABLE IF NOT EXISTS position_history (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             REAL NOT NULL,
    node_id        TEXT NOT NULL,
    lat            REAL NOT NULL,
    lon            REAL NOT NULL,
    alt            INTEGER,
    precision_bits INTEGER,
    channel        INTEGER,
    snr            REAL,
    rssi           INTEGER,
    hops_away      INTEGER,
    relay_node     INTEGER,
    via_mqtt       INTEGER NOT NULL DEFAULT 0,
    pkt_id         INTEGER,
    pos_time       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_position_history_node_ts ON position_history (node_id, ts);
CREATE INDEX IF NOT EXISTS idx_position_history_ts ON position_history (ts);
CREATE TABLE IF NOT EXISTS position_probes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL NOT NULL,
    node_id      TEXT NOT NULL,
    channel      INTEGER,
    result       TEXT NOT NULL,
    latency      REAL,
    lat          REAL,
    lon          REAL,
    stale_fix    INTEGER NOT NULL DEFAULT 0,
    snr          REAL,
    rssi         INTEGER,
    hops_away    INTEGER,
    fix_age      REAL
);
CREATE INDEX IF NOT EXISTS idx_position_probes_ts ON position_probes (ts);
CREATE TABLE IF NOT EXISTS app_settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# Every completed trace gets auto-recorded (unlike saved_traceroutes, which is
# a deliberate "proven path" pick) so this is capped and pruned on write,
# rather than kept forever like the hand-curated table above.
TRACEROUTE_HISTORY_LIMIT = 300

# Our own node's self-reported telemetry arrives every ~15-30 min, so this
# comfortably covers months of history before the oldest rows get pruned.
TELEMETRY_HISTORY_LIMIT = 5000

# Every position packet the mesh delivers to us gets a row. Measured on this
# mesh (~90 nodes visible) that's ~2,600 rows/day, so this is roughly two and a
# half months of coverage history at ~90 bytes a row — a few MB.
POSITION_HISTORY_LIMIT = 200000

# Pruning scans the whole table, and at 200k rows that's silly to do on every
# one of ~2 inserts a minute. Trimming in batches keeps the table within a
# rounding error of the limit for a tiny fraction of the work.
POSITION_PRUNE_EVERY = 500


class NodeStore:
    """Favorites, notes, and saved traceroutes — keyed by node id (`!abcd1234`).

    Separate from MessageStore's `messages` table (different lifecycle: this
    data is edited by hand and should never be pruned), but shares the same
    on-disk file since it's the same "survive a restart" concern.
    """

    def __init__(self, path=DEFAULT_PATH):
        self.path = path
        self.lock = threading.Lock()
        with self._conn() as c:
            c.executescript(NODE_SCHEMA)
            # CREATE TABLE IF NOT EXISTS won't add columns to a table that
            # already exists, so new columns need an explicit migration.
            # Duplicate-column is the expected steady state, not an error.
            for table, col, decl in (
                ("position_history", "pos_time", "INTEGER"),
                ("position_probes", "fix_age", "REAL"),
            ):
                try:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
                    logger.info("Added %s.%s", table, col)
                except sqlite3.OperationalError as e:
                    if "duplicate column" not in str(e).lower():
                        raise
        logger.info("Node store at %s", self.path)

    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def set_meta(self, node_id, favorite=None, notes=None):
        """Merge-update favorite/notes for a node. Either field may be omitted
        to leave it unchanged. Rows that end up at the default state (not
        favorited, no notes) are deleted rather than left as empty clutter."""
        with self.lock, self._conn() as c:
            row = c.execute(
                "SELECT favorite, notes FROM node_meta WHERE node_id = ?", (node_id,)
            ).fetchone()
            cur_fav = bool(row["favorite"]) if row else False
            cur_notes = (row["notes"] if row else "") or ""
            new_fav = cur_fav if favorite is None else bool(favorite)
            new_notes = cur_notes if notes is None else notes
            if not new_fav and not new_notes.strip():
                c.execute("DELETE FROM node_meta WHERE node_id = ?", (node_id,))
            else:
                c.execute(
                    "INSERT INTO node_meta (node_id, favorite, notes, updated_ts) VALUES (?,?,?,?) "
                    "ON CONFLICT(node_id) DO UPDATE SET favorite=excluded.favorite, "
                    "notes=excluded.notes, updated_ts=excluded.updated_ts",
                    (node_id, int(new_fav), new_notes, time.time()),
                )
        return {"node_id": node_id, "favorite": new_fav, "notes": new_notes}

    def all_meta(self):
        with self.lock, self._conn() as c:
            rows = c.execute("SELECT * FROM node_meta").fetchall()
        return {
            r["node_id"]: {
                "favorite": bool(r["favorite"]),
                "notes": r["notes"] or "",
                "updated_ts": r["updated_ts"],
            } for r in rows
        }

    def save_traceroute(self, node_id, route, route_back, hops_there, note=None):
        with self.lock, self._conn() as c:
            cur = c.execute(
                "INSERT INTO saved_traceroutes (node_id, ts, route, route_back, hops_there, note) "
                "VALUES (?,?,?,?,?,?)",
                (node_id, time.time(), json.dumps(route or []),
                 json.dumps(route_back or []), hops_there, note),
            )
            return cur.lastrowid

    def saved_traceroutes(self, node_id=None):
        with self.lock, self._conn() as c:
            if node_id:
                rows = c.execute(
                    "SELECT * FROM saved_traceroutes WHERE node_id = ? ORDER BY ts DESC", (node_id,)
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM saved_traceroutes ORDER BY ts DESC"
                ).fetchall()
        return [{
            "id": r["id"],
            "node_id": r["node_id"],
            "ts": r["ts"],
            "route": json.loads(r["route"]) if r["route"] else [],
            "route_back": json.loads(r["route_back"]) if r["route_back"] else [],
            "hops_there": r["hops_there"],
            "note": r["note"] or "",
        } for r in rows]

    def delete_traceroute(self, trace_id):
        with self.lock, self._conn() as c:
            c.execute("DELETE FROM saved_traceroutes WHERE id = ?", (trace_id,))

    def record_traceroute(self, entry):
        """Auto-persist a completed trace from the client's in-memory tracking
        dict, so the live Traceroutes panel (and 'save as proven path', which
        reads the most recent completed trace for a node) survive a service
        restart instead of going blank until the next manual trace."""
        node_id = entry.get("to")
        if not node_id or entry.get("status") not in ("ok", "failed"):
            return
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT INTO traceroute_history "
                "(node_id, ts, status, route, route_back, hops_there, error) "
                "VALUES (?,?,?,?,?,?,?)",
                (node_id, entry.get("completed_ts") or entry.get("ts") or time.time(),
                 entry["status"], json.dumps(entry.get("route") or []),
                 json.dumps(entry.get("route_back") or []),
                 entry.get("hops_there"), entry.get("error")),
            )
            c.execute(
                "DELETE FROM traceroute_history WHERE id NOT IN "
                "(SELECT id FROM traceroute_history ORDER BY ts DESC LIMIT ?)",
                (TRACEROUTE_HISTORY_LIMIT,),
            )

    def recent_traceroutes(self, limit=100):
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT * FROM traceroute_history ORDER BY ts DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._trace_row(r) for r in rows]

    def latest_traceroute(self, node_id):
        """Most recent successful trace to a node — the persisted fallback
        for 'save as proven path' when the client's own in-memory copy was
        lost to a restart."""
        with self.lock, self._conn() as c:
            row = c.execute(
                "SELECT * FROM traceroute_history WHERE node_id = ? AND status = 'ok' "
                "ORDER BY ts DESC LIMIT 1", (node_id,)
            ).fetchone()
        return self._trace_row(row) if row else None

    @staticmethod
    def _trace_row(r):
        return {
            "to": r["node_id"],
            "ts": r["ts"],
            "completed_ts": r["ts"],
            "status": r["status"],
            "route": json.loads(r["route"]) if r["route"] else [],
            "route_back": json.loads(r["route_back"]) if r["route_back"] else [],
            "hops_there": r["hops_there"],
            "error": r["error"],
        }

    def record_telemetry(self, channel_utilization=None, air_util_tx=None,
                          battery_level=None, voltage=None):
        """Our own node's self-reported channel/airtime utilization, sampled
        whenever it broadcasts its own telemetry over the mesh (same packet
        type any node's deviceMetrics arrives as) — lets the dashboard chart
        congestion over time against ack success rate."""
        if channel_utilization is None and air_util_tx is None:
            return
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT INTO telemetry_history "
                "(ts, channel_utilization, air_util_tx, battery_level, voltage) "
                "VALUES (?,?,?,?,?)",
                (time.time(), channel_utilization, air_util_tx, battery_level, voltage),
            )
            c.execute(
                "DELETE FROM telemetry_history WHERE id NOT IN "
                "(SELECT id FROM telemetry_history ORDER BY ts DESC LIMIT ?)",
                (TELEMETRY_HISTORY_LIMIT,),
            )

    def recent_telemetry(self, since_ts=None, limit=3000):
        with self.lock, self._conn() as c:
            if since_ts:
                rows = c.execute(
                    "SELECT ts, channel_utilization, air_util_tx, battery_level, voltage "
                    "FROM telemetry_history WHERE ts >= ? ORDER BY ts ASC LIMIT ?",
                    (since_ts, limit),
                ).fetchall()
            else:
                rows = list(reversed(c.execute(
                    "SELECT ts, channel_utilization, air_util_tx, battery_level, voltage "
                    "FROM telemetry_history ORDER BY ts DESC LIMIT ?", (limit,)
                ).fetchall()))
        return [dict(r) for r in rows]

    def record_position(self, entry):
        """One row per position packet that actually reached us.

        This is deliberately a log of *receptions*, not a node's current
        location: the same node sitting still still gets a new row every
        beacon, because "we heard it again from there, at this SNR, this many
        hops out" is the measurement. That's what makes the coverage map
        meaningful — the dots are proof of a working path home, not just
        breadcrumbs.
        """
        pkt_id = entry.get("pkt_id")
        with self.lock, self._conn() as c:
            if pkt_id:
                # The mesh can hand us the same packet twice (two neighbours
                # relaying the same beacon). Same id from the same node inside
                # an hour is that, not a second real reception.
                dup = c.execute(
                    "SELECT 1 FROM position_history "
                    "WHERE node_id = ? AND pkt_id = ? AND ts > ? LIMIT 1",
                    (entry["node_id"], pkt_id, time.time() - 3600),
                ).fetchone()
                if dup:
                    return False
            c.execute(
                "INSERT INTO position_history "
                "(ts, node_id, lat, lon, alt, precision_bits, channel, snr, rssi, "
                " hops_away, relay_node, via_mqtt, pkt_id, pos_time) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (entry.get("ts") or time.time(), entry["node_id"],
                 entry["lat"], entry["lon"], entry.get("alt"),
                 entry.get("precision_bits"), entry.get("channel"),
                 entry.get("snr"), entry.get("rssi"), entry.get("hops_away"),
                 entry.get("relay_node"), 1 if entry.get("via_mqtt") else 0,
                 pkt_id, entry.get("pos_time")),
            )
            self._position_writes = getattr(self, "_position_writes", 0) + 1
            if self._position_writes % POSITION_PRUNE_EVERY == 0:
                c.execute(
                    "DELETE FROM position_history WHERE id NOT IN "
                    "(SELECT id FROM position_history ORDER BY ts DESC LIMIT ?)",
                    (POSITION_HISTORY_LIMIT,),
                )
        return True

    def recent_positions(self, node_id=None, since_ts=None, min_precision=None,
                          rf_only=False, limit=5000):
        sql = ("SELECT ts, node_id, lat, lon, alt, precision_bits, channel, snr, "
               "rssi, hops_away, relay_node, via_mqtt, pos_time FROM position_history WHERE 1=1")
        params = []
        if node_id:
            sql += " AND node_id = ?"
            params.append(node_id)
        if since_ts:
            sql += " AND ts >= ?"
            params.append(since_ts)
        if min_precision:
            # NULL precision means the sender's firmware didn't report it —
            # treat that as "unknown", which fails a minimum-precision filter.
            sql += " AND precision_bits >= ?"
            params.append(min_precision)
        if rf_only:
            sql += " AND via_mqtt = 0"
        # Newest-first for the LIMIT (so a cap trims the *oldest*), then flipped
        # back to chronological so the trail draws in travel order.
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self.lock, self._conn() as c:
            rows = list(reversed(c.execute(sql, params).fetchall()))
        return [dict(r) for r in rows]

    def latest_position(self, node_id):
        with self.lock, self._conn() as c:
            row = c.execute(
                "SELECT ts, lat, lon, precision_bits, channel FROM position_history "
                "WHERE node_id = ? ORDER BY ts DESC LIMIT 1", (node_id,)
            ).fetchone()
        return dict(row) if row else None

    def position_since(self, node_id, since_ts, channel=None):
        """The first position from this node logged after `since_ts` — how a
        probe finds out whether its request was answered, without needing a
        callback threaded back through the mesh client."""
        sql = "SELECT ts, lat, lon, precision_bits, channel, snr, rssi, hops_away, pos_time " \
              "FROM position_history WHERE node_id = ? AND ts > ?"
        params = [node_id, since_ts]
        if channel is not None:
            sql += " AND channel = ?"
            params.append(channel)
        sql += " ORDER BY ts ASC LIMIT 1"
        with self.lock, self._conn() as c:
            row = c.execute(sql, params).fetchone()
        return dict(row) if row else None

    def position_timestamps(self, node_id, since_ts, exclude_channel=None):
        sql = "SELECT ts FROM position_history WHERE node_id = ? AND ts >= ?"
        params = [node_id, since_ts]
        if exclude_channel is not None:
            sql += " AND (channel IS NULL OR channel != ?)"
            params.append(exclude_channel)
        sql += " ORDER BY ts ASC"
        with self.lock, self._conn() as c:
            rows = c.execute(sql, params).fetchall()
        return [r["ts"] for r in rows]

    def record_probe(self, entry):
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT INTO position_probes "
                "(ts, node_id, channel, result, latency, lat, lon, stale_fix, snr, rssi, hops_away, fix_age) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (entry.get("ts") or time.time(), entry["node_id"], entry.get("channel"),
                 entry["result"], entry.get("latency"), entry.get("lat"), entry.get("lon"),
                 1 if entry.get("stale_fix") else 0, entry.get("snr"), entry.get("rssi"),
                 entry.get("hops_away"), entry.get("fix_age")),
            )

    def recent_probes(self, node_id=None, since_ts=None, limit=3000):
        sql = ("SELECT ts, node_id, channel, result, latency, lat, lon, stale_fix, "
               "snr, rssi, hops_away, fix_age FROM position_probes WHERE 1=1")
        params = []
        if node_id:
            sql += " AND node_id = ?"
            params.append(node_id)
        if since_ts:
            sql += " AND ts >= ?"
            params.append(since_ts)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self.lock, self._conn() as c:
            rows = list(reversed(c.execute(sql, params).fetchall()))
        return [dict(r) for r in rows]

    def get_setting(self, key, default=None):
        with self.lock, self._conn() as c:
            row = c.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key, value):
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT INTO app_settings (key, value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )

    def position_nodes(self, since_ts=None):
        """Which nodes have logged positions, newest activity first — feeds the
        dashboard's node picker so it only ever offers nodes with real data."""
        sql = ("SELECT node_id, COUNT(*) AS points, MAX(ts) AS last_ts, "
               "MAX(precision_bits) AS best_precision FROM position_history")
        params = []
        if since_ts:
            sql += " WHERE ts >= ?"
            params.append(since_ts)
        sql += " GROUP BY node_id ORDER BY last_ts DESC"
        with self.lock, self._conn() as c:
            rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
