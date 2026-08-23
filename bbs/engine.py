"""BBS DM menu state machine (v0).

Pure-ish: takes text in, returns reply string or None. No radio I/O.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Callable, Optional

from . import protocol as proto
from .store import BBSStore, SESSION_IDLE_SECS

logger = logging.getLogger("cybermesh.bbs.engine")

# Defaults match DESIGN.md; override via env at engine construction time
DEFAULT_POST_RATE = 5
DEFAULT_MAIL_RATE = 10


class BBSEngine:
    def __init__(
        self,
        store: BBSStore,
        node_resolver: Optional[Callable[[str], Optional[str]]] = None,
        post_rate: Optional[int] = None,
        mail_rate: Optional[int] = None,
        display_name: str = "CyberMesh BBS",
    ):
        self.store = store
        self.node_resolver = node_resolver or (lambda token: None)
        self.post_rate = post_rate if post_rate is not None else int(
            os.environ.get("BBS_POST_RATE_LIMIT", DEFAULT_POST_RATE)
        )
        self.mail_rate = mail_rate if mail_rate is not None else int(
            os.environ.get("BBS_MAIL_RATE_LIMIT", DEFAULT_MAIL_RATE)
        )
        self.display_name = display_name

    # ── public entry ────────────────────────────────────────────────────

    def should_handle(self, from_id: str, text: str) -> bool:
        """Cheap gate for mesh_client — no side effects beyond session read."""
        if not from_id:
            return False
        if proto.is_bare_token(text, "BBS"):
            return True
        return self.store.get_session(from_id) is not None

    def handle(self, from_id: str, text: str) -> dict:
        """Process one inbound DM.

        Returns:
          {
            "reply": str | None,
            "session_active": bool,
            "handled": bool,   # True if this was BBS traffic
          }
        """
        text = text if text is not None else ""
        try:
            return self._handle(from_id, text)
        except Exception:
            logger.exception("BBS engine error for %s", from_id)
            # Fail open: not handled → caller treats as normal DM
            return {"reply": None, "session_active": False, "handled": False}

    def _handle(self, from_id: str, text: str) -> dict:
        # New session
        if proto.is_bare_token(text, "BBS"):
            self.store.set_session(from_id, "main", {})
            return {
                "reply": self._main_menu(from_id),
                "session_active": True,
                "handled": True,
            }

        sess = self.store.get_session(from_id)
        if not sess:
            return {"reply": None, "session_active": False, "handled": False}

        state = sess["state"]
        ctx = dict(sess.get("context") or {})

        # Global quit
        if proto.is_bare_token(text, "X", "BYE", "QUIT"):
            self.store.end_session(from_id)
            return {
                "reply": "BBS session ended. DM BBS anytime.",
                "session_active": False,
                "handled": True,
            }

        # Compose states swallow most input as body lines
        if state in ("mail_compose", "post_compose"):
            return self._handle_compose(from_id, text, state, ctx)

        # Global help / back
        if proto.is_bare_token(text, "?", "HELP"):
            reply = self._screen(from_id, state, ctx)
            self.store.set_session(from_id, state, ctx)
            return {"reply": reply, "session_active": True, "handled": True}

        if proto.is_bare_token(text, "0", "BACK"):
            state, ctx, reply = self._go_back(from_id, state, ctx)
            self.store.set_session(from_id, state, ctx)
            return {"reply": reply, "session_active": True, "handled": True}

        # Dispatch by state
        handler = {
            "main": self._on_main,
            "mail_menu": self._on_mail_menu,
            "mail_list": self._on_mail_list,
            "mail_read": self._on_mail_read,
            "boards_menu": self._on_boards_menu,
            "board": self._on_board,
            "post_list": self._on_post_list,
            "post_read": self._on_post_read,
        }.get(state)

        if not handler:
            self.store.set_session(from_id, "main", {})
            return {
                "reply": self._main_menu(from_id),
                "session_active": True,
                "handled": True,
            }

        state, ctx, reply = handler(from_id, text, ctx)
        if state is None:
            # ended
            self.store.end_session(from_id)
            return {"reply": reply, "session_active": False, "handled": True}
        self.store.set_session(from_id, state, ctx)
        return {"reply": reply, "session_active": True, "handled": True}

    # ── screens ─────────────────────────────────────────────────────────

    def _main_menu(self, from_id: str) -> str:
        n = self.store.count_unread_mail(from_id)
        mail_line = f"M) Mail ({n} new)" if n else "M) Mail"
        return proto.fit(
            f"{self.display_name}\n"
            f"{mail_line}\n"
            f"B) Boards\n"
            f"?) Help  X) Bye"
        )

    def _screen(self, from_id: str, state: str, ctx: dict) -> str:
        if state == "main":
            return self._main_menu(from_id)
        if state == "mail_menu":
            return self._mail_menu_text(from_id)
        if state == "boards_menu":
            return self._boards_menu_text()
        if state == "board":
            return self._board_text(ctx)
        if state == "post_list":
            return self._post_list_page(ctx, page=ctx.get("page", 0))
        if state == "post_read":
            return self._post_read_text(ctx)
        if state == "mail_list":
            return self._mail_list_page(from_id, ctx, page=ctx.get("page", 0))
        if state == "mail_read":
            return self._mail_read_text(ctx)
        return self._main_menu(from_id)

    def _go_back(self, from_id: str, state: str, ctx: dict):
        if state == "main":
            return "main", {}, self._main_menu(from_id)
        if state in ("mail_menu", "boards_menu"):
            return "main", {}, self._main_menu(from_id)
        if state in ("mail_list", "mail_read", "mail_compose"):
            return "mail_menu", {}, self._mail_menu_text(from_id)
        if state == "board":
            return "boards_menu", {}, self._boards_menu_text()
        if state in ("post_list", "post_compose"):
            bid = ctx.get("board_id")
            return "board", {"board_id": bid}, self._board_text({"board_id": bid})
        if state == "post_read":
            bid = ctx.get("board_id")
            nctx = {"board_id": bid, "page": 0}
            return "post_list", nctx, self._post_list_page(nctx, 0)
        return "main", {}, self._main_menu(from_id)

    # ── main ────────────────────────────────────────────────────────────

    def _on_main(self, from_id: str, text: str, ctx: dict):
        cmd, _ = proto.parse_command(text)
        if cmd in ("M", "MAIL"):
            return "mail_menu", {}, self._mail_menu_text(from_id)
        if cmd in ("B", "BOARDS"):
            return "boards_menu", {}, self._boards_menu_text()
        return "main", {}, (
            "Unknown. ? for menu.\n" + self._main_menu(from_id)
        )

    # ── mail ────────────────────────────────────────────────────────────

    def _mail_menu_text(self, from_id: str) -> str:
        unread = self.store.count_unread_mail(from_id)
        total = len(self.store.list_mail(from_id, limit=500))
        return proto.fit(
            f"MAIL ({unread} new / {total} total)\n"
            f"1) Read next unread\n"
            f"L) List all\n"
            f"W <to> <subj> Write\n"
            f"0) Back"
        )

    def _on_mail_menu(self, from_id: str, text: str, ctx: dict):
        if proto.is_bare_token(text, "1"):
            unread = self.store.list_mail(from_id, unread_only=True, limit=1)
            if not unread:
                return "mail_menu", {}, "No unread mail.\n" + self._mail_menu_text(from_id)
            m = unread[0]
            self.store.mark_mail_read(m["id"])
            nctx = {"mail_id": m["id"], "body_offset": 0}
            return "mail_read", nctx, self._mail_read_text(nctx)

        cmd, args = proto.parse_command(text)
        if cmd in ("L", "LIST"):
            nctx = {"page": 0}
            return "mail_list", nctx, self._mail_list_page(from_id, nctx, 0)

        wcmd, to_tok, subj = proto.split_write_mail(text)
        if wcmd:
            if not to_tok or not subj:
                return "mail_menu", {}, "Usage: W <to> <subject>\n" + self._mail_menu_text(from_id)
            if proto.utf8_len(subj) > proto.SUBJECT_MAX:
                return "mail_menu", {}, "Subject too long (max 100 bytes)."
            # Rate limit
            since = time.time() - 3600
            if self.store.count_mail_since(from_id, since) >= self.mail_rate:
                return "mail_menu", {}, f"Mail rate limit ({self.mail_rate}/hr). Try later."
            dest = self._resolve_node(to_tok)
            if not dest:
                return "mail_menu", {}, (
                    f"Unknown node '{to_tok}'. Use exact name or !nodeid."
                )
            nctx = {
                "compose_to": dest,
                "compose_subject": subj,
                "compose_body": "",
            }
            return "mail_compose", nctx, proto.fit(
                f"Composing to {to_tok}\n"
                f"Subj: {subj}\n"
                f"Type message. SEND when done, CANCEL to abort."
            )

        return "mail_menu", {}, "Unknown. ? for menu.\n" + self._mail_menu_text(from_id)

    def _mail_list_page(self, from_id: str, ctx: dict, page: int) -> str:
        items = self.store.list_mail(from_id, limit=50)
        # store newest-first; reverse for classic oldest-first list numbers? keep newest first
        lines = []
        id_map = []
        for i, m in enumerate(items, 1):
            flag = "*" if not m["read"] else " "
            # short date
            subj = m["subject"][:40]
            lines.append(f"{i}){flag}{m['from_node'][:12]}: {subj}")
            id_map.append(m["id"])
        ctx["id_map"] = id_map
        ctx["page"] = page
        pages = proto.paginate_lines("MAIL", lines, footer="0) Back")
        # v0: single packed page only (paginate_lines may return multi; join first)
        if not pages:
            return "No mail.\n0) Back"
        # For multi-page, respect page index
        page = max(0, min(page, len(pages) - 1))
        ctx["page_count"] = len(pages)
        return pages[page]

    def _on_mail_list(self, from_id: str, text: str, ctx: dict):
        if proto.is_bare_token(text, ">", "MORE"):
            page = ctx.get("page", 0) + 1
            ctx["page"] = page
            return "mail_list", ctx, self._mail_list_page(from_id, ctx, page)
        if text.strip().isdigit():
            n = int(text.strip())
            id_map = ctx.get("id_map") or []
            if n < 1 or n > len(id_map):
                return "mail_list", ctx, "Invalid selection. ? for menu."
            mid = id_map[n - 1]
            self.store.mark_mail_read(mid)
            nctx = {"mail_id": mid, "body_offset": 0}
            return "mail_read", nctx, self._mail_read_text(nctx)
        return "mail_list", ctx, "Unknown. ? for menu."

    def _mail_read_text(self, ctx: dict) -> str:
        m = self.store.get_mail(ctx.get("mail_id", -1))
        if not m:
            return "Mail gone.\n0) Back"
        offset = ctx.get("body_offset", 0)
        body = m["body"][offset:]
        header = f"From: {m['from_node']}\nSubj: {m['subject']}\n"
        room = proto.WORKING_BUDGET - proto.utf8_len(header) - 20
        chunk = proto.fit_with_more(body, max(20, room))
        more = proto.utf8_len(body) > room
        ctx["body_has_more"] = more
        if more:
            # advance offset by what we showed without [more]
            shown = chunk.replace("[more]", "")
            ctx["next_offset"] = offset + len(shown)
        lines = header + chunk
        footer = "\nD) Del  " + (">) More  " if more else "") + "0) Back"
        return proto.fit(lines + footer)

    def _on_mail_read(self, from_id: str, text: str, ctx: dict):
        if proto.is_bare_token(text, ">", "MORE") and ctx.get("body_has_more"):
            ctx["body_offset"] = ctx.get("next_offset", 0)
            return "mail_read", ctx, self._mail_read_text(ctx)
        if proto.is_bare_token(text, "D", "DELETE"):
            mid = ctx.get("mail_id")
            self.store.delete_mail(mid, from_id)
            return "mail_menu", {}, "Deleted.\n" + self._mail_menu_text(from_id)
        return "mail_read", ctx, "Unknown. ? for menu.\n" + self._mail_read_text(ctx)

    # ── boards ──────────────────────────────────────────────────────────

    def _boards_menu_text(self) -> str:
        boards = self.store.list_boards()
        lines = []
        id_map = []
        for i, b in enumerate(boards, 1):
            lines.append(f"{i}) {b['name']} ({b['post_count']} posts)")
            id_map.append(b["id"])
        # stash on a throwaway — caller must set context; use empty and re-list on select
        pages = proto.paginate_lines("BOARDS", lines, footer="0) Back")
        return pages[0] if pages else "No boards.\n0) Back"

    def _on_boards_menu(self, from_id: str, text: str, ctx: dict):
        if text.strip().isdigit():
            n = int(text.strip())
            boards = self.store.list_boards()
            if n < 1 or n > len(boards):
                return "boards_menu", {}, "Invalid selection. ? for menu."
            b = boards[n - 1]
            nctx = {"board_id": b["id"]}
            return "board", nctx, self._board_text(nctx)
        return "boards_menu", {}, "Unknown. ? for menu.\n" + self._boards_menu_text()

    def _board_text(self, ctx: dict) -> str:
        b = self.store.get_board(ctx.get("board_id", -1))
        if not b:
            return "Board gone.\n0) Back"
        return proto.fit(
            f"{b['name'].upper()}\n"
            f"L) List posts\n"
            f"N <subj> New post\n"
            f"0) Back"
        )

    def _on_board(self, from_id: str, text: str, ctx: dict):
        cmd, _ = proto.parse_command(text)
        if cmd in ("L", "LIST"):
            nctx = {"board_id": ctx.get("board_id"), "page": 0}
            return "post_list", nctx, self._post_list_page(nctx, 0)

        ncmd, subj = proto.split_new_post(text)
        if ncmd:
            if not subj:
                return "board", ctx, "Usage: N <subject>"
            if proto.utf8_len(subj) > proto.SUBJECT_MAX:
                return "board", ctx, "Subject too long (max 100 bytes)."
            since = time.time() - 3600
            if self.store.count_posts_since(from_id, since) >= self.post_rate:
                return "board", ctx, f"Post rate limit ({self.post_rate}/hr). Try later."
            b = self.store.get_board(ctx.get("board_id", -1))
            if not b:
                return "boards_menu", {}, "Board gone.\n" + self._boards_menu_text()
            nctx = {
                "board_id": b["id"],
                "compose_subject": subj,
                "compose_body": "",
            }
            return "post_compose", nctx, proto.fit(
                f"Posting to {b['name']}\n"
                f"Subj: {subj}\n"
                f"Type message. SEND when done, CANCEL to abort."
            )

        return "board", ctx, "Unknown. ? for menu.\n" + self._board_text(ctx)

    def _post_list_page(self, ctx: dict, page: int) -> str:
        bid = ctx.get("board_id")
        b = self.store.get_board(bid) if bid else None
        title = (b["name"] if b else "BOARD") + " posts"
        posts = self.store.list_posts(bid, status="visible", limit=50)
        lines = []
        id_map = []
        for i, p in enumerate(posts, 1):
            subj = p["subject"][:36]
            author = (p["author_node"] or "?")[:10]
            lines.append(f"{i}) {author}: {subj}")
            id_map.append(p["id"])
        ctx["id_map"] = id_map
        ctx["page"] = page
        pages = proto.paginate_lines(title, lines, footer="0) Back")
        if not pages:
            return "No posts.\n0) Back"
        page = max(0, min(page, len(pages) - 1))
        ctx["page_count"] = len(pages)
        return pages[page]

    def _on_post_list(self, from_id: str, text: str, ctx: dict):
        if proto.is_bare_token(text, ">", "MORE"):
            page = ctx.get("page", 0) + 1
            ctx["page"] = page
            return "post_list", ctx, self._post_list_page(ctx, page)
        if text.strip().isdigit():
            n = int(text.strip())
            id_map = ctx.get("id_map") or []
            if n < 1 or n > len(id_map):
                return "post_list", ctx, "Invalid selection. ? for menu."
            nctx = {
                "board_id": ctx.get("board_id"),
                "post_id": id_map[n - 1],
                "body_offset": 0,
            }
            return "post_read", nctx, self._post_read_text(nctx)
        return "post_list", ctx, "Unknown. ? for menu."

    def _post_read_text(self, ctx: dict) -> str:
        p = self.store.get_post(ctx.get("post_id", -1))
        if not p or p["status"] == "deleted":
            return "Post gone.\n0) Back"
        offset = ctx.get("body_offset", 0)
        body = p["body"][offset:]
        header = f"From: {p['author_node']}\nSubj: {p['subject']}\n"
        room = proto.WORKING_BUDGET - proto.utf8_len(header) - 24
        chunk = proto.fit_with_more(body, max(20, room))
        more = proto.utf8_len(body) > room
        ctx["body_has_more"] = more
        if more:
            shown = chunk.replace("[more]", "")
            ctx["next_offset"] = offset + len(shown)
        can_del = True  # footer always shows D; engine checks author on action
        footer = "\n" + ("D) Del  " if can_del else "") + (">) More  " if more else "") + "0) Back"
        return proto.fit(header + chunk + footer)

    def _on_post_read(self, from_id: str, text: str, ctx: dict):
        if proto.is_bare_token(text, ">", "MORE") and ctx.get("body_has_more"):
            ctx["body_offset"] = ctx.get("next_offset", 0)
            return "post_read", ctx, self._post_read_text(ctx)
        if proto.is_bare_token(text, "D", "DELETE"):
            p = self.store.get_post(ctx.get("post_id", -1))
            if not p:
                return "post_list", {"board_id": ctx.get("board_id"), "page": 0}, "Gone."
            if p["author_node"] != from_id:
                return "post_read", ctx, "Only author can delete.\n" + self._post_read_text(ctx)
            self.store.delete_post(p["id"], "author")
            nctx = {"board_id": ctx.get("board_id"), "page": 0}
            return "post_list", nctx, "Deleted.\n" + self._post_list_page(nctx, 0)
        return "post_read", ctx, "Unknown. ? for menu."

    # ── compose (mail + post) ───────────────────────────────────────────

    def _handle_compose(self, from_id: str, text: str, state: str, ctx: dict) -> dict:
        if proto.is_bare_token(text, "CANCEL"):
            if state == "mail_compose":
                self.store.set_session(from_id, "mail_menu", {})
                return {
                    "reply": "Cancelled.\n" + self._mail_menu_text(from_id),
                    "session_active": True,
                    "handled": True,
                }
            bid = ctx.get("board_id")
            nctx = {"board_id": bid}
            self.store.set_session(from_id, "board", nctx)
            return {
                "reply": "Cancelled.\n" + self._board_text(nctx),
                "session_active": True,
                "handled": True,
            }

        if proto.is_bare_token(text, "SEND"):
            body = (ctx.get("compose_body") or "").strip()
            if not body:
                return {
                    "reply": "Empty body. Type text or CANCEL.",
                    "session_active": True,
                    "handled": True,
                }
            if state == "mail_compose":
                self.store.add_mail(
                    to_node=ctx["compose_to"],
                    from_node=from_id,
                    subject=ctx["compose_subject"],
                    body=body,
                )
                self.store.set_session(from_id, "mail_menu", {})
                return {
                    "reply": "Mail sent.\n" + self._mail_menu_text(from_id),
                    "session_active": True,
                    "handled": True,
                }
            # post
            b = self.store.get_board(ctx.get("board_id", -1))
            if not b:
                self.store.set_session(from_id, "boards_menu", {})
                return {
                    "reply": "Board gone.\n" + self._boards_menu_text(),
                    "session_active": True,
                    "handled": True,
                }
            self.store.add_post(
                board_id=b["id"],
                author_node=from_id,
                subject=ctx["compose_subject"],
                body=body,
                moderated=bool(b.get("moderated")),
            )
            status_note = " (pending approval)" if b.get("moderated") else ""
            nctx = {"board_id": b["id"]}
            self.store.set_session(from_id, "board", nctx)
            return {
                "reply": f"Posted{status_note}.\n" + self._board_text(nctx),
                "session_active": True,
                "handled": True,
            }

        # Body line (including accidental SEND in longer text)
        body = ctx.get("compose_body") or ""
        addition = text if not body else ("\n" + text)
        if proto.utf8_len(body + addition) > proto.BODY_MAX:
            return {
                "reply": "Body full (4KiB). SEND or CANCEL.",
                "session_active": True,
                "handled": True,
            }
        ctx["compose_body"] = body + addition
        self.store.set_session(from_id, state, ctx)
        # Quiet ack — save airtime; only confirm every few lines would be nicer
        # but silent confuses users on LoRa. One short ack:
        nlines = ctx["compose_body"].count("\n") + 1
        return {
            "reply": proto.fit(f"…{nlines} line(s). SEND or CANCEL."),
            "session_active": True,
            "handled": True,
        }

    def _resolve_node(self, token: str) -> Optional[str]:
        """Exact match via optional resolver; also accept !hex as-is."""
        t = (token or "").strip()
        if not t:
            return None
        if t.startswith("!") and len(t) >= 3:
            return t  # trust explicit node id form
        resolved = self.node_resolver(t)
        if resolved:
            return resolved
        # If resolver returns None, still accept token as node id when it
        # looks like a hex node id without bang
        return None
