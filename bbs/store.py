"""SQLite store for single-node CyberMesh BBS (v0)."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Optional

logger = logging.getLogger("cybermesh.bbs.store")

DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bbs.db"
)

SESSION_IDLE_SECS = 15 * 60

SCHEMA = """
CREATE TABLE IF NOT EXISTS bbs_boards (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL UNIQUE,
    description   TEXT NOT NULL DEFAULT '',
    moderated     INTEGER NOT NULL DEFAULT 0,
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS bbs_posts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    board_id      INTEGER NOT NULL,
    author_node   TEXT NOT NULL,
    subject       TEXT NOT NULL,
    body          TEXT NOT NULL,
    timestamp     REAL NOT NULL,
    status        TEXT NOT NULL,
    deleted_by    TEXT,
    FOREIGN KEY (board_id) REFERENCES bbs_boards(id)
);
CREATE INDEX IF NOT EXISTS idx_bbs_posts_board ON bbs_posts (board_id, status, timestamp);
CREATE TABLE IF NOT EXISTS bbs_mail (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    to_node       TEXT NOT NULL,
    from_node     TEXT NOT NULL,
    subject       TEXT NOT NULL,
    body          TEXT NOT NULL,
    read          INTEGER NOT NULL DEFAULT 0,
    timestamp     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bbs_mail_to ON bbs_mail (to_node, read, timestamp);
CREATE TABLE IF NOT EXISTS bbs_sessions (
    node_id       TEXT PRIMARY KEY,
    state         TEXT NOT NULL,
    context_json  TEXT NOT NULL DEFAULT '{}',
    updated_at    REAL NOT NULL
);
"""


class BBSStore:
    def __init__(self, path: str = DEFAULT_PATH):
        self.path = path
        self.lock = threading.Lock()
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self._conn() as c:
            c.executescript(SCHEMA)
        self._seed_general()
        logger.info("BBS store at %s", self.path)

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def _seed_general(self) -> None:
        with self.lock, self._conn() as c:
            n = c.execute("SELECT COUNT(*) AS n FROM bbs_boards").fetchone()["n"]
            if n == 0:
                c.execute(
                    "INSERT INTO bbs_boards (name, description, moderated, created_at) "
                    "VALUES (?,?,?,?)",
                    ("General", "Default board", 0, time.time()),
                )

    # ── boards ──────────────────────────────────────────────────────────

    def list_boards(self) -> list[dict]:
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT b.*, "
                "(SELECT COUNT(*) FROM bbs_posts p "
                " WHERE p.board_id = b.id AND p.status = 'visible') AS post_count "
                "FROM bbs_boards b ORDER BY b.id"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_board(self, board_id: int) -> Optional[dict]:
        with self.lock, self._conn() as c:
            r = c.execute("SELECT * FROM bbs_boards WHERE id = ?", (board_id,)).fetchone()
            return dict(r) if r else None

    def create_board(self, name: str, description: str = "", moderated: bool = False) -> int:
        with self.lock, self._conn() as c:
            cur = c.execute(
                "INSERT INTO bbs_boards (name, description, moderated, created_at) "
                "VALUES (?,?,?,?)",
                (name, description or "", 1 if moderated else 0, time.time()),
            )
            return int(cur.lastrowid)

    # ── posts ───────────────────────────────────────────────────────────

    def list_posts(self, board_id: int, status: str = "visible",
                   limit: int = 50) -> list[dict]:
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT * FROM bbs_posts WHERE board_id = ? AND status = ? "
                "ORDER BY timestamp DESC LIMIT ?",
                (board_id, status, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_post(self, post_id: int) -> Optional[dict]:
        with self.lock, self._conn() as c:
            r = c.execute("SELECT * FROM bbs_posts WHERE id = ?", (post_id,)).fetchone()
            return dict(r) if r else None

    def add_post(self, board_id: int, author_node: str, subject: str,
                 body: str, moderated: bool = False) -> int:
        status = "pending_approval" if moderated else "visible"
        with self.lock, self._conn() as c:
            cur = c.execute(
                "INSERT INTO bbs_posts "
                "(board_id, author_node, subject, body, timestamp, status, deleted_by) "
                "VALUES (?,?,?,?,?,?,NULL)",
                (board_id, author_node, subject, body, time.time(), status),
            )
            return int(cur.lastrowid)

    def delete_post(self, post_id: int, deleted_by: str) -> bool:
        with self.lock, self._conn() as c:
            cur = c.execute(
                "UPDATE bbs_posts SET status = 'deleted', deleted_by = ? "
                "WHERE id = ? AND status != 'deleted'",
                (deleted_by, post_id),
            )
            return cur.rowcount > 0

    def approve_post(self, post_id: int) -> bool:
        with self.lock, self._conn() as c:
            cur = c.execute(
                "UPDATE bbs_posts SET status = 'visible' "
                "WHERE id = ? AND status = 'pending_approval'",
                (post_id,),
            )
            return cur.rowcount > 0

    def count_posts_since(self, author_node: str, since: float) -> int:
        with self.lock, self._conn() as c:
            r = c.execute(
                "SELECT COUNT(*) AS n FROM bbs_posts "
                "WHERE author_node = ? AND timestamp > ? AND status != 'deleted'",
                (author_node, since),
            ).fetchone()
            return int(r["n"])

    # ── mail ────────────────────────────────────────────────────────────

    def add_mail(self, to_node: str, from_node: str, subject: str, body: str) -> int:
        with self.lock, self._conn() as c:
            cur = c.execute(
                "INSERT INTO bbs_mail (to_node, from_node, subject, body, read, timestamp) "
                "VALUES (?,?,?,?,0,?)",
                (to_node, from_node, subject, body, time.time()),
            )
            return int(cur.lastrowid)

    def list_mail(self, to_node: str, unread_only: bool = False,
                  limit: int = 50) -> list[dict]:
        with self.lock, self._conn() as c:
            if unread_only:
                rows = c.execute(
                    "SELECT * FROM bbs_mail WHERE to_node = ? AND read = 0 "
                    "ORDER BY timestamp DESC LIMIT ?",
                    (to_node, limit),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM bbs_mail WHERE to_node = ? "
                    "ORDER BY timestamp DESC LIMIT ?",
                    (to_node, limit),
                ).fetchall()
            return [dict(r) for r in rows]

    def count_unread_mail(self, to_node: str) -> int:
        with self.lock, self._conn() as c:
            r = c.execute(
                "SELECT COUNT(*) AS n FROM bbs_mail WHERE to_node = ? AND read = 0",
                (to_node,),
            ).fetchone()
            return int(r["n"])

    def get_mail(self, mail_id: int) -> Optional[dict]:
        with self.lock, self._conn() as c:
            r = c.execute("SELECT * FROM bbs_mail WHERE id = ?", (mail_id,)).fetchone()
            return dict(r) if r else None

    def mark_mail_read(self, mail_id: int) -> None:
        with self.lock, self._conn() as c:
            c.execute("UPDATE bbs_mail SET read = 1 WHERE id = ?", (mail_id,))

    def delete_mail(self, mail_id: int, for_node: str) -> bool:
        """Recipient can delete their own mail."""
        with self.lock, self._conn() as c:
            cur = c.execute(
                "DELETE FROM bbs_mail WHERE id = ? AND to_node = ?",
                (mail_id, for_node),
            )
            return cur.rowcount > 0

    def count_mail_since(self, from_node: str, since: float) -> int:
        with self.lock, self._conn() as c:
            r = c.execute(
                "SELECT COUNT(*) AS n FROM bbs_mail "
                "WHERE from_node = ? AND timestamp > ?",
                (from_node, since),
            ).fetchone()
            return int(r["n"])

    def list_all_mail(self, limit: int = 100) -> list[dict]:
        """Sysop view — all mail regardless of recipient."""
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT * FROM bbs_mail ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def sysop_delete_mail(self, mail_id: int) -> bool:
        with self.lock, self._conn() as c:
            cur = c.execute("DELETE FROM bbs_mail WHERE id = ?", (mail_id,))
            return cur.rowcount > 0

    def count_pending_posts(self) -> int:
        with self.lock, self._conn() as c:
            r = c.execute(
                "SELECT COUNT(*) AS n FROM bbs_posts WHERE status = 'pending_approval'"
            ).fetchone()
            return int(r["n"])

    # ── sessions ────────────────────────────────────────────────────────

    def get_session(self, node_id: str) -> Optional[dict]:
        with self.lock, self._conn() as c:
            r = c.execute(
                "SELECT * FROM bbs_sessions WHERE node_id = ?", (node_id,)
            ).fetchone()
            if not r:
                return None
            d = dict(r)
            if time.time() - d["updated_at"] > SESSION_IDLE_SECS:
                c.execute("DELETE FROM bbs_sessions WHERE node_id = ?", (node_id,))
                return None
            try:
                d["context"] = json.loads(d.get("context_json") or "{}")
            except json.JSONDecodeError:
                d["context"] = {}
            return d

    def set_session(self, node_id: str, state: str, context: Optional[dict] = None) -> None:
        ctx = json.dumps(context or {})
        now = time.time()
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT INTO bbs_sessions (node_id, state, context_json, updated_at) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(node_id) DO UPDATE SET "
                "state=excluded.state, context_json=excluded.context_json, "
                "updated_at=excluded.updated_at",
                (node_id, state, ctx, now),
            )

    def touch_session(self, node_id: str) -> None:
        with self.lock, self._conn() as c:
            c.execute(
                "UPDATE bbs_sessions SET updated_at = ? WHERE node_id = ?",
                (time.time(), node_id),
            )

    def end_session(self, node_id: str) -> None:
        with self.lock, self._conn() as c:
            c.execute("DELETE FROM bbs_sessions WHERE node_id = ?", (node_id,))

    def active_session_nodes(self) -> list[str]:
        cutoff = time.time() - SESSION_IDLE_SECS
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT node_id FROM bbs_sessions WHERE updated_at >= ?",
                (cutoff,),
            ).fetchall()
            return [r["node_id"] for r in rows]

    def list_active_sessions(self) -> list[dict]:
        cutoff = time.time() - SESSION_IDLE_SECS
        with self.lock, self._conn() as c:
            rows = c.execute(
                "SELECT node_id, state, context_json, updated_at FROM bbs_sessions "
                "WHERE updated_at >= ? ORDER BY updated_at DESC",
                (cutoff,),
            ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                try:
                    d["context"] = json.loads(d.get("context_json") or "{}")
                except json.JSONDecodeError:
                    d["context"] = {}
                del d["context_json"]
                out.append(d)
            return out
