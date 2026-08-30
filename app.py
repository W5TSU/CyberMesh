import logging
import os
import threading
import time

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, send_from_directory

from mesh_client import MeshClient
from range_probe import create_probe
from store import DEFAULT_PATH as DEFAULT_DB_PATH, MessageStore, NodeStore

load_dotenv()

# CARTO basemap key — blank falls back to unkeyed tiles (watermarked).
# Get your own free key: https://carto.com/basemaps/apikey
CARTO_KEY = os.getenv("CARTO_KEY", "")

logging.basicConfig(level=logging.INFO)

HOST = os.environ.get("MESHTASTIC_HOST", "192.168.1.100")
PORT = int(os.environ.get("PORT", 5090))
SERIAL_PORT = os.environ.get("MESHTASTIC_SERIAL_PORT", "/dev/ttyACM0")
TRANSPORT = os.environ.get("MESHTASTIC_TRANSPORT", "auto")

if SERIAL_PORT and not os.path.exists(SERIAL_PORT):
    logging.warning("Serial port %s not present — falling back to TCP only", SERIAL_PORT)
    SERIAL_PORT = None

def _float_env(name):
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return None


# Distance filtering needs an origin. The T-Beam's own GPS is used when it has
# a fix; indoors it doesn't, so these are the fallback.
HOME_LAT = _float_env("HOME_LAT")
HOME_LON = _float_env("HOME_LON")

# Range measurements need the T-Beam's REAL location, not the grid-square
# centre HOME_LAT/HOME_LON uses. Those two stay deliberately approximate
# because they also feed the node list's distance filter and the map origin,
# where a coarse value is fine and a precise one is needless exposure. Every
# figure on the Range tab is a distance from the antenna, so ~1 km of error
# there is the difference between "made it across the neighbourhood" and
# "didn't". Falls back to HOME_* when unset.
RANGE_HOME_LAT = _float_env("RANGE_HOME_LAT") or HOME_LAT
RANGE_HOME_LON = _float_env("RANGE_HOME_LON") or HOME_LON

app = Flask(__name__)
store = MessageStore(os.environ.get("MESSAGE_DB") or DEFAULT_DB_PATH)
node_store = NodeStore(os.environ.get("MESSAGE_DB") or DEFAULT_DB_PATH)
client = MeshClient(HOST, serial_port=SERIAL_PORT, transport=TRANSPORT,
                    home_lat=HOME_LAT, home_lon=HOME_LON, store=store,
                    node_store=node_store)

# CyberMesh BBS v0 — single-node, in-process. See ~/cybermesh-bbs/DESIGN.md
# and bbs/. Off by default unless BBS_ENABLED=1 in .env.
try:
    from bbs import bbs_enabled, create_engine as create_bbs_engine

    if bbs_enabled():
        def _bbs_resolve_node(token):
            """Exact short/long/!id match only — never fuzzy (private mail)."""
            t = (token or "").strip()
            if not t:
                return None
            if t.startswith("!") and len(t) >= 3:
                return t
            t_lower = t.lower()
            matches = []
            # Live mesh DB first
            try:
                with client.lock:
                    nodes = dict(client.iface.nodes or {}) if client.iface else {}
                for nid, n in nodes.items():
                    user = (n or {}).get("user") or {}
                    short = (user.get("shortName") or "").strip()
                    longn = (user.get("longName") or "").strip()
                    if short.lower() == t_lower or longn.lower() == t_lower:
                        matches.append(nid)
            except Exception:
                logging.getLogger("cybermesh").exception("BBS live node resolve failed")
            # Durable name cache (seen-even-when-offline)
            try:
                with store.lock, store._conn() as c:
                    rows = c.execute(
                        "SELECT node_id, long_name, short_name FROM node_names"
                    ).fetchall()
                for r in rows:
                    short = (r["short_name"] or "").strip()
                    longn = (r["long_name"] or "").strip()
                    if short.lower() == t_lower or longn.lower() == t_lower:
                        if r["node_id"] not in matches:
                            matches.append(r["node_id"])
            except Exception:
                logging.getLogger("cybermesh").exception("BBS name-cache resolve failed")
            if len(matches) == 1:
                return matches[0]
            return None  # zero or ambiguous — engine rejects with clear error

        client.bbs_engine = create_bbs_engine(node_resolver=_bbs_resolve_node)
        logging.getLogger("cybermesh").info(
            "BBS enabled (db=%s)", getattr(client.bbs_engine.store, "path", "?")
        )
    else:
        logging.getLogger("cybermesh").info("BBS disabled (set BBS_ENABLED=1 to turn on)")
except Exception:
    logging.getLogger("cybermesh").exception("BBS failed to init — continuing without it")

def _int_env(name, default):
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


# Range probing is motion-gated and persists its on/off state in the db, so it
# starts in whatever state it was left in rather than resetting on restart.
range_probe = create_probe(
    client, node_store,
    os.environ.get("RANGE_PROBE_NODE"),
    channel=_int_env("RANGE_PROBE_CHANNEL", 2),
    poll_secs=_int_env("RANGE_PROBE_SECS", 300),
    motion_window=_int_env("RANGE_PROBE_MOTION_WINDOW", 900),
    linger_secs=_int_env("RANGE_PROBE_LINGER", 1200),
)

CONFIG_SECTIONS = ["device", "position", "power", "network", "display", "lora", "bluetooth", "security"]
MODULE_SECTIONS = [
    "mqtt", "serial", "external_notification", "store_forward", "range_test",
    "telemetry", "canned_message", "audio", "remote_hardware", "neighbor_info",
    "detection_sensor", "ambient_lighting", "paxcounter", "traffic_management",
]


@app.context_processor
def _inject_carto_key():
    """Every map template builds its tile URL from this."""
    return {"carto_key": CARTO_KEY}


@app.route("/service-worker.js")
def service_worker():
    resp = send_from_directory(app.static_folder, "service-worker.js", mimetype="application/javascript")
    # The SW script's own byte-diff update check is the only thing that ever
    # notices a bumped CACHE version — a stray Cache-Control on *this specific*
    # file (proxy or browser) can mask a real cache-name bump indefinitely,
    # which is exactly the bug that shipped two template edits stale in a row.
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/")
def dashboard():
    return render_template("dashboard.html", active="dashboard")


@app.route("/messages")
def messages_page():
    return render_template("messages.html", active="messages")


@app.route("/config")
def config_page():
    return render_template("config.html", active="config", sections=CONFIG_SECTIONS, module_sections=MODULE_SECTIONS)


@app.route("/channels")
def channels_page():
    return render_template("channels.html", active="channels")


@app.route("/bbs")
def bbs_page():
    return render_template("bbs.html", active="bbs")


def _bbs_store():
    """Return BBS store or None if BBS is off / failed to init."""
    eng = getattr(client, "bbs_engine", None)
    return eng.store if eng is not None else None


@app.route("/api/bbs/status")
def api_bbs_status():
    eng = getattr(client, "bbs_engine", None)
    if eng is None:
        return jsonify({"enabled": False, "db": None, "pending": 0, "sessions": 0})
    st = eng.store
    return jsonify({
        "enabled": True,
        "db": st.path,
        "pending": st.count_pending_posts(),
        "sessions": len(st.list_active_sessions()),
        "boards": len(st.list_boards()),
    })


@app.route("/api/bbs/boards", methods=["GET", "POST"])
def api_bbs_boards():
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled (set BBS_ENABLED=1)"}), 503
    if request.method == "GET":
        return jsonify(st.list_boards())
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    if len(name) > 40:
        return jsonify({"error": "name too long (max 40)"}), 400
    try:
        bid = st.create_board(
            name,
            description=(data.get("description") or "").strip(),
            moderated=bool(data.get("moderated")),
        )
    except Exception as e:
        # UNIQUE name collision, etc.
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True, "id": bid})


@app.route("/api/bbs/boards/<int:board_id>/posts", methods=["GET", "POST"])
def api_bbs_board_posts(board_id):
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled"}), 503
    if not st.get_board(board_id):
        return jsonify({"error": "board not found"}), 404
    if request.method == "GET":
        status = (request.args.get("status") or "visible").strip()
        if status not in ("visible", "pending_approval", "deleted"):
            return jsonify({"error": "bad status"}), 400
        return jsonify(st.list_posts(board_id, status=status, limit=100))
    data = request.get_json(force=True) or {}
    subject = (data.get("subject") or "").strip()
    body = (data.get("body") or "").strip()
    if not subject or not body:
        return jsonify({"error": "subject and body required"}), 400
    board = st.get_board(board_id)
    # Sysop posts as the local node when known
    author = data.get("author_node") or (
        f"!{client.my_node_num():08x}" if client.my_node_num() else "sysop"
    )
    pid = st.add_post(
        board_id, author, subject, body,
        moderated=bool(board.get("moderated")) and not data.get("force_visible"),
    )
    # Sysop compose from web: force visible unless they left moderated intentionally
    if data.get("force_visible", True) and board.get("moderated"):
        st.approve_post(pid)
    return jsonify({"ok": True, "id": pid})


@app.route("/api/bbs/posts/<int:post_id>/approve", methods=["POST"])
def api_bbs_post_approve(post_id):
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled"}), 503
    if not st.approve_post(post_id):
        return jsonify({"error": "not pending or missing"}), 400
    return jsonify({"ok": True})


@app.route("/api/bbs/posts/<int:post_id>/reject", methods=["POST"])
def api_bbs_post_reject(post_id):
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled"}), 503
    if not st.delete_post(post_id, "sysop"):
        return jsonify({"error": "missing or already deleted"}), 400
    return jsonify({"ok": True})


@app.route("/api/bbs/posts/<int:post_id>", methods=["DELETE"])
def api_bbs_post_delete(post_id):
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled"}), 503
    if not st.delete_post(post_id, "sysop"):
        return jsonify({"error": "missing or already deleted"}), 400
    return jsonify({"ok": True})


@app.route("/api/bbs/mail", methods=["GET"])
def api_bbs_mail():
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled"}), 503
    return jsonify(st.list_all_mail(limit=100))


@app.route("/api/bbs/mail/<int:mail_id>", methods=["DELETE"])
def api_bbs_mail_delete(mail_id):
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled"}), 503
    if not st.sysop_delete_mail(mail_id):
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/bbs/sessions", methods=["GET"])
def api_bbs_sessions():
    st = _bbs_store()
    if st is None:
        return jsonify({"error": "BBS disabled"}), 503
    return jsonify(st.list_active_sessions())


@app.route("/api/status")
def api_status():
    return jsonify(client.status())


@app.route("/api/nodes")
def api_nodes():
    return jsonify(client.node_list())


@app.route("/api/remote-admin/schema")
def api_remote_admin_schema():
    return jsonify({
        "config_sections": CONFIG_SECTIONS,
        "module_sections": MODULE_SECTIONS,
        "schema": client.get_schema(),
        "actions": [
            "metadata", "reboot", "shutdown", "reset_nodedb",
            "factory_reset_config", "factory_reset_full",
        ],
    })


@app.route("/api/remote-admin/log")
def api_remote_admin_log():
    return jsonify(client.get_remote_admin_log())


def _remote_admin_guard(data):
    """Tiny CSRF/same-origin speed bump for radio-admin POSTs.

    CyberMesh is Tailscale-only, not public internet, but these endpoints can
    reboot/reset remote radios. A custom header forces browser fetch/XHR from
    our UI instead of a random cross-site form post; writes/actions still also
    require exact node-id confirmation.
    """
    node_id = (data.get("node_id") or "").strip()
    if request.headers.get("X-CyberMesh-Remote-Admin") != node_id:
        return node_id, ({"error": "missing remote-admin UI guard header"}, 403)
    return node_id, None


@app.route("/api/remote-admin/get", methods=["POST"])
def api_remote_admin_get():
    data = request.get_json(force=True)
    node_id, guard = _remote_admin_guard(data)
    if guard:
        body, status = guard
        return jsonify(body), status
    try:
        result = client.remote_admin_get(
            node_id,
            data.get("kind") or "config",
            data.get("section") or "device",
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, **result})


@app.route("/api/remote-admin/set", methods=["POST"])
def api_remote_admin_set():
    data = request.get_json(force=True)
    node_id, guard = _remote_admin_guard(data)
    if guard:
        body, status = guard
        return jsonify(body), status
    if data.get("confirm") != node_id:
        return jsonify({"error": "confirmation must exactly match node id"}), 400
    try:
        result = client.remote_admin_set(
            node_id,
            data.get("kind") or "config",
            data.get("section") or "device",
            data.get("values") or {},
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, **result})


@app.route("/api/remote-admin/action", methods=["POST"])
def api_remote_admin_action():
    data = request.get_json(force=True)
    node_id, guard = _remote_admin_guard(data)
    if guard:
        body, status = guard
        return jsonify(body), status
    action = (data.get("action") or "").strip()
    if action != "metadata" and data.get("confirm") != node_id:
        return jsonify({"error": "confirmation must exactly match node id"}), 400
    try:
        result = client.remote_admin_action(node_id, action, secs=data.get("secs"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, **result})


@app.route("/api/messages", methods=["GET", "POST"])
def api_messages():
    if request.method == "POST":
        data = request.get_json(force=True)
        text = data.get("text", "").strip()
        channel = int(data.get("channel", 0))
        destination = (data.get("to") or "").strip() or None
        if not text:
            return jsonify({"error": "empty message"}), 400
        try:
            client.send_text(text, channel, destination=destination)
        except Exception as e:
            return jsonify({"error": str(e)}), 502
        return jsonify({"ok": True})
    name_map = {}
    if client.store is not None:
        try:
            name_map = client.store.name_map()
        except Exception:
            pass
    return jsonify({"messages": client.get_messages(), "name_map": name_map})


@app.route("/api/messages/since/<float:ts>")
@app.route("/api/messages/since/<ts>")
def api_messages_since(ts):
    ts = float(ts)
    name_map = {}
    if client.store is not None:
        try:
            name_map = client.store.name_map()
        except Exception:
            pass
        try:
            delta = client.store.recent_since(ts)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        return jsonify({**delta, "name_map": name_map})

    messages = client.get_messages()
    new_messages = [m for m in messages if (m.get("ts") or 0) > ts]
    updates = [m for m in messages if (m.get("ts") or 0) <= ts and (m.get("updated_ts") or m.get("ts") or 0) > ts]
    return jsonify({"new": new_messages, "updates": updates, "name_map": name_map})


@app.route("/api/messages/<int:pkt_id>/reaction", methods=["POST"])
def api_message_reaction(pkt_id):
    data = request.get_json(force=True)
    emoji = (data.get("emoji") or "").strip()
    if not emoji:
        return jsonify({"error": "emoji required"}), 400
    target = next((m for m in reversed(client.get_messages()) if m.get("id") == pkt_id), None)
    if not target:
        return jsonify({"error": "message not found"}), 404
    destination = None
    if target.get("direct"):
        destination = target.get("to") if target.get("from") == "me" else target.get("from")
    try:
        client.send_reaction(pkt_id, emoji, channel_index=target.get("channel", 0),
                             destination=destination)
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True})


@app.route("/api/config")
def api_config():
    return jsonify({"config": client.get_config(), "module_config": client.get_module_config()})


@app.route("/api/config/schema")
def api_config_schema():
    return jsonify(client.get_schema())


@app.route("/api/config/<section>", methods=["POST"])
def api_config_set(section):
    data = request.get_json(force=True)
    try:
        if section in CONFIG_SECTIONS:
            client.set_config_section(section, data)
        elif section in MODULE_SECTIONS:
            client.set_module_config_section(section, data)
        else:
            return jsonify({"error": "unknown section"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True})


@app.route("/api/position/fixed", methods=["POST"])
def api_set_fixed_position():
    data = request.get_json(force=True)
    try:
        lat, lon = float(data["lat"]), float(data["lon"])
    except (KeyError, TypeError, ValueError):
        return jsonify({"error": "lat and lon are required"}), 400
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return jsonify({"error": "lat/lon out of range"}), 400
    try:
        client.set_fixed_position(lat, lon, int(data.get("alt", 0)))
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "lat": lat, "lon": lon})


@app.route("/api/telemetry-history")
def api_telemetry_history():
    since = request.args.get("since", type=float)
    return jsonify(node_store.recent_telemetry(since_ts=since))


@app.route("/api/position-trail")
def api_position_trail():
    """Logged position receptions, for the coverage map.

    Every point is somewhere a packet from that node actually made it home, so
    the layer answers "where was I heard" directly — and the gaps between
    consecutive points are the honest unknown: either out of range, or simply
    not beaconing.
    """
    node = request.args.get("node") or None
    hours = request.args.get("hours", type=float)
    since = (time.time() - hours * 3600) if hours else None
    points = node_store.recent_positions(
        node_id=node,
        since_ts=since,
        min_precision=request.args.get("min_precision", type=int),
        rf_only=request.args.get("rf_only") == "1",
        limit=request.args.get("limit", default=5000, type=int),
    )
    names = store.name_map()
    nodes = node_store.position_nodes(since_ts=since)
    for n in nodes:
        n["name"] = names.get(n["node_id"])
    return jsonify({"points": points, "nodes": nodes})


@app.route("/range")
def range_page():
    return render_template("range.html", active="range")


def _no_probe():
    return jsonify({"ok": False, "error": "RANGE_PROBE_NODE not configured"}), 400


@app.route("/api/range/status")
def api_range_status():
    if not range_probe:
        return jsonify({"enabled": False, "state": "unconfigured", "node_id": None})
    return jsonify(range_probe.status())


@app.route("/api/range/enable", methods=["POST"])
def api_range_enable():
    if not range_probe:
        return _no_probe()
    data = request.get_json(force=True, silent=True) or {}
    range_probe.set_enabled(bool(data.get("enabled")))
    return jsonify(range_probe.status())


@app.route("/api/range/probe-now", methods=["POST"])
def api_range_probe_now():
    """Fire one probe regardless of the motion gate — for standing in a spot
    and asking "can I reach home from here" without waiting for the timer.

    Dispatched to a thread rather than awaited: a probe waits up to the reply
    timeout, and holding an HTTP request open that long would hang the browser
    on exactly the marginal-link case this is most useful for. The result shows
    up on the next status/probes poll.
    """
    if not range_probe:
        return _no_probe()
    threading.Thread(target=range_probe.probe_once, daemon=True).start()
    return jsonify({"ok": True, "queued": True})


@app.route("/api/range/probes")
def api_range_probes():
    hours = request.args.get("hours", type=float)
    since = (time.time() - hours * 3600) if hours else None
    node = request.args.get("node") or (range_probe.node_id if range_probe else None)
    # Distances are measured from RANGE_HOME_* — the T-Beam's surveyed
    # position. Not the node's self-reported one, which is fuzzed to its own
    # channel precision (reads 35.5/-97.5), and not HOME_*, which is a grid
    # square centre. Either would put kilometres of error into every figure.
    return jsonify({
        "probes": node_store.recent_probes(node_id=node, since_ts=since),
        "status": range_probe.status() if range_probe else None,
        "home": {"lat": RANGE_HOME_LAT, "lon": RANGE_HOME_LON} if RANGE_HOME_LAT and RANGE_HOME_LON else None,
    })


@app.route("/api/request-position", methods=["POST"])
def api_request_position():
    """Ask a node for its position over a chosen channel.

    Channel matters: the reply is fuzzed to the precision of the channel the
    request came in on, so asking over CyberMesh gets full precision back
    while the public channel keeps seeing only the coarse beacon.
    """
    data = request.get_json(force=True, silent=True) or {}
    node = data.get("node")
    channel = data.get("channel", 0)
    try:
        packet = client.request_position(node, int(channel))
    except (RuntimeError, ValueError) as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    return jsonify({"ok": True, "id": getattr(packet, "id", None)})


@app.route("/api/channels")
def api_channels():
    return jsonify(client.get_channels())


@app.route("/api/traceroute", methods=["GET", "POST"])
def api_traceroute():
    if request.method == "POST":
        data = request.get_json(force=True)
        dest = (data.get("to") or "").strip()
        if not dest:
            return jsonify({"error": "destination required"}), 400
        try:
            result = client.trace_route(dest, data.get("hop_limit"))
        except Exception as e:
            return jsonify({"error": str(e)}), 502
        return jsonify(result)
    return jsonify(client.get_traceroutes())


@app.route("/api/nodes/meta")
def api_nodes_meta():
    return jsonify(node_store.all_meta())


@app.route("/api/nodes/<node_id>/meta", methods=["POST"])
def api_node_meta_set(node_id):
    data = request.get_json(force=True)
    favorite = data.get("favorite")
    if favorite is not None:
        favorite = bool(favorite)
    notes = data.get("notes")
    result = node_store.set_meta(node_id, favorite=favorite, notes=notes)
    return jsonify({"ok": True, **result})


@app.route("/api/traceroute/saved")
def api_traceroute_saved():
    return jsonify(node_store.saved_traceroutes(request.args.get("node_id")))


@app.route("/api/traceroute/save", methods=["POST"])
def api_traceroute_save():
    data = request.get_json(force=True)
    node_id = (data.get("node_id") or "").strip()
    if not node_id:
        return jsonify({"error": "node_id required"}), 400
    trace = client.get_traceroute(node_id)
    if not trace or trace.get("status") != "ok":
        return jsonify({"error": "no completed traceroute to that node yet — trace it first"}), 400
    trace_id = node_store.save_traceroute(
        node_id, trace.get("route"), trace.get("route_back"),
        trace.get("hops_there"), data.get("note"),
    )
    return jsonify({"ok": True, "id": trace_id})


@app.route("/api/traceroute/saved/<int:trace_id>", methods=["DELETE"])
def api_traceroute_delete(trace_id):
    node_store.delete_traceroute(trace_id)
    return jsonify({"ok": True})


@app.route("/api/channels/add", methods=["POST"])
def api_channels_add():
    data = request.get_json(force=True)
    try:
        result = client.add_channel(data.get("name", ""))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, **result})


@app.route("/api/channels/<int:index>/enabled", methods=["POST"])
def api_channels_enabled(index):
    data = request.get_json(force=True)
    try:
        result = client.set_channel_enabled(index, bool(data.get("enabled")))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, **result})


@app.route("/api/channels/<int:index>", methods=["POST"])
def api_channels_set(index):
    data = request.get_json(force=True)
    try:
        client.set_channel(index, data)
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True})


@app.route("/api/admin/<action>", methods=["POST"])
def api_admin(action):
    try:
        if action == "reboot":
            client.reboot()
        elif action == "shutdown":
            client.shutdown_device()
        elif action == "factory_reset":
            client.factory_reset()
        else:
            return jsonify({"error": "unknown action"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True})


if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=PORT)
