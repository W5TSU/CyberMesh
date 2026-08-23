"""LoRa-safe text helpers for the BBS DM menu.

Hard payload limit is meshtastic DATA_PAYLOAD_LEN (233). We target ~220 bytes
UTF-8 so a trailing newline or footer never blows the send.
"""
from __future__ import annotations

# Confirmed against meshtastic 2.7.11 in cybermesh venv
HARD_LIMIT = 233
WORKING_BUDGET = 220

SUBJECT_MAX = 100
BODY_MAX = 4096


def utf8_len(text: str) -> int:
    return len(text.encode("utf-8"))


def fit(text: str, budget: int = WORKING_BUDGET) -> str:
    """Truncate text to budget bytes without splitting a UTF-8 codepoint."""
    raw = text.encode("utf-8")
    if len(raw) <= budget:
        return text
    # Leave room for "[more]" marker when truncating body lines
    cut = raw[:budget]
    while cut:
        try:
            return cut.decode("utf-8")
        except UnicodeDecodeError:
            cut = cut[:-1]
    return ""


def fit_with_more(text: str, budget: int = WORKING_BUDGET) -> str:
    """Truncate and append [more] if needed, still within budget."""
    if utf8_len(text) <= budget:
        return text
    marker = "[more]"
    room = budget - utf8_len(marker)
    if room < 8:
        return fit(text, budget)
    return fit(text, room) + marker


def paginate_lines(header: str, items: list[str], footer: str = "",
                   budget: int = WORKING_BUDGET) -> list[str]:
    """Pack numbered-style item lines into one or more reply pages.

    Returns a list of full page strings. Each page is <= budget bytes.
    Caller owns numbering inside `items` (e.g. '1) foo').
    """
    pages: list[str] = []
    i = 0
    n = len(items)
    while True:
        more_left = False
        lines = [header] if header else []
        start = i
        while i < n:
            candidate = items[i]
            trial = "\n".join(lines + [candidate] + ([footer] if footer else []))
            # Reserve space for a possible ">) More" line if more items remain
            remaining_after = i + 1 < n
            more_line = ">) More" if remaining_after else ""
            trial_with_more = "\n".join(
                lines + [candidate] + ([more_line] if more_line else []) + ([footer] if footer else [])
            )
            if utf8_len(trial_with_more) > budget and lines != ([header] if header else []):
                more_left = True
                break
            if utf8_len(trial_with_more) > budget and lines == ([header] if header else []):
                # Single item alone is too big — hard-truncate it
                lines.append(fit_with_more(candidate, budget - utf8_len(header) - 1
                                           - (utf8_len(footer) + 1 if footer else 0)
                                           - 8))
                i += 1
                more_left = i < n
                break
            lines.append(candidate)
            i += 1
        if more_left or (i < n and i == start):
            # If we made no progress, force one truncated item to avoid loop
            if i == start and i < n:
                lines.append(fit_with_more(items[i], max(20, budget - 40)))
                i += 1
            if i < n:
                lines.append(">) More")
        if footer:
            lines.append(footer)
        page = "\n".join(lines)
        pages.append(fit(page, budget) if utf8_len(page) > budget else page)
        if i >= n:
            break
    return pages or [fit(header or "(empty)", budget)]


def parse_command(text: str) -> tuple[str, list[str]]:
    """Return (CMD_UPPER, args). Empty text → ('', [])."""
    if text is None:
        return "", []
    stripped = text.strip()
    if not stripped:
        return "", []
    parts = stripped.split()
    return parts[0].upper(), parts[1:]


def is_bare_token(text: str, *tokens: str) -> bool:
    """True if entire trimmed message equals one of tokens (case-insensitive)."""
    t = (text or "").strip().upper()
    return t in {x.upper() for x in tokens}


def split_write_mail(text: str) -> tuple[str | None, str | None, str | None]:
    """Parse 'W <to> <subject…>' → (cmd, to, subject) or (None, None, None)."""
    stripped = text.strip()
    parts = stripped.split(maxsplit=2)
    if len(parts) < 1:
        return None, None, None
    cmd = parts[0].upper()
    if cmd not in ("W", "WRITE"):
        return None, None, None
    if len(parts) < 3:
        return cmd, None, None
    return cmd, parts[1], parts[2]


def split_new_post(text: str) -> tuple[str | None, str | None]:
    """Parse 'N <subject…>' → (cmd, subject)."""
    stripped = text.strip()
    parts = stripped.split(maxsplit=1)
    if not parts:
        return None, None
    cmd = parts[0].upper()
    if cmd not in ("N", "NEW"):
        return None, None
    if len(parts) < 2:
        return cmd, None
    return cmd, parts[1]
