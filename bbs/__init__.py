"""CyberMesh BBS — single-node Meshtastic bulletin board (v0).

See ~/cybermesh-bbs/DESIGN.md for the full product plan. v0 is in-process,
no federation, feature-flagged via BBS_ENABLED.
"""
from __future__ import annotations

import os
import logging

from .engine import BBSEngine
from .store import BBSStore

logger = logging.getLogger("cybermesh.bbs")

__all__ = ["BBSEngine", "BBSStore", "bbs_enabled", "create_engine"]


def bbs_enabled() -> bool:
    return os.environ.get("BBS_ENABLED", "0").strip() in ("1", "true", "True", "yes")


def create_engine(db_path: str | None = None, node_resolver=None) -> BBSEngine:
    """Build a BBSEngine. node_resolver(to_token) -> node_id | None optional."""
    path = db_path or os.environ.get(
        "BBS_DB",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bbs.db"),
    )
    store = BBSStore(path)
    return BBSEngine(store, node_resolver=node_resolver)
