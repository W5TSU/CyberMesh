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

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    pkt_id        INTEGER,
    ts            REAL NOT NULL,
    from_id       TEXT,
    to_id         TEXT,
    direct        INTEGER NOT NULL DEFAULT 0,
    channel       INTEGER NOT NULL DEFAULT 0,
    via_mqtt      INTEGER NOT NULL DEFAULT 0,
    text          TEXT NOT NULL,
    status        TEXT,
    status_reason TEXT,
    heard_by      TEXT,
    PRIMARY KEY (pkt_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (ts);
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
            if "heard_by" not in cols:
                c.execute("ALTER TABLE messages ADD COLUMN heard_by TEXT")
        logger.info("Message store at %s", self.path)

    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def add(self, msg):
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO messages "
                "(pkt_id, ts, from_id, to_id, direct, channel, via_mqtt, text, status, status_reason, heard_by) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (msg.get("id"), msg["ts"], msg.get("from"), msg.get("to"),
                 int(bool(msg.get("direct"))), msg.get("channel", 0),
                 int(bool(msg.get("via_mqtt"))), msg["text"],
                 msg.get("status"), msg.get("status_reason"),
                 json.dumps(msg.get("heard_by") or [])),
            )

    def update_status(self, pkt_id, status, reason=None, heard_by=None):
        if pkt_id is None:
            return
        with self.lock, self._conn() as c:
            if heard_by is not None:
                c.execute(
                    "UPDATE messages SET status = ?, status_reason = ?, heard_by = ? WHERE pkt_id = ?",
                    (status, reason, json.dumps(heard_by), pkt_id),
                )
            else:
                c.execute(
                    "UPDATE messages SET status = ?, status_reason = ? WHERE pkt_id = ?",
                    (status, reason, pkt_id),
                )

    def recent(self, limit=None):
        """Oldest-first, so the UI can append straight into a conversation."""
        limit = limit or self.limit
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT * FROM messages ORDER BY ts DESC LIMIT ?", (limit,)
            ).fetchall()
        return [{
            "id": r["pkt_id"],
            "ts": r["ts"],
            "from": r["from_id"],
            "to": r["to_id"],
            "direct": bool(r["direct"]),
            "channel": r["channel"],
            "via_mqtt": bool(r["via_mqtt"]),
            "text": r["text"],
            "status": r["status"],
            "status_reason": r["status_reason"],
            "heard_by": json.loads(r["heard_by"]) if r["heard_by"] else [],
        } for r in reversed(rows)]

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
"""

# Every completed trace gets auto-recorded (unlike saved_traceroutes, which is
# a deliberate "proven path" pick) so this is capped and pruned on write,
# rather than kept forever like the hand-curated table above.
TRACEROUTE_HISTORY_LIMIT = 300

# Our own node's self-reported telemetry arrives every ~15-30 min, so this
# comfortably covers months of history before the oldest rows get pruned.
TELEMETRY_HISTORY_LIMIT = 5000


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
