import logging
import os

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, send_from_directory

from mesh_client import MeshClient
from store import DEFAULT_PATH as DEFAULT_DB_PATH, MessageStore, NodeStore

load_dotenv()

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

app = Flask(__name__)
store = MessageStore(os.environ.get("MESSAGE_DB") or DEFAULT_DB_PATH)
node_store = NodeStore(os.environ.get("MESSAGE_DB") or DEFAULT_DB_PATH)
client = MeshClient(HOST, serial_port=SERIAL_PORT, transport=TRANSPORT,
                    home_lat=HOME_LAT, home_lon=HOME_LON, store=store,
                    node_store=node_store)

CONFIG_SECTIONS = ["device", "position", "power", "network", "display", "lora", "bluetooth", "security"]
MODULE_SECTIONS = [
    "mqtt", "serial", "external_notification", "store_forward", "range_test",
    "telemetry", "canned_message", "audio", "remote_hardware", "neighbor_info",
    "detection_sensor", "ambient_lighting", "paxcounter", "traffic_management",
]


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


@app.route("/api/status")
def api_status():
    return jsonify(client.status())


@app.route("/api/nodes")
def api_nodes():
    return jsonify(client.node_list())


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
    return jsonify(client.get_messages())


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
