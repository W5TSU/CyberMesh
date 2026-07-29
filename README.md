# CyberMesh

A self-hosted web dashboard for a Meshtastic node — full remote control from a browser, without relying on the official phone app's BLE range or a laptop tether.

Built because most Meshtastic hardware (classic ESP32-based boards like the T-Beam) has **no built-in web UI of its own** — only the raw TCP/serial API. CyberMesh is the missing web frontend: a small Flask app that holds one persistent connection to your node and exposes everything through a browser.

## Features

- **Live node map & list** — position, battery, SNR, hops, last-heard, RF vs. MQTT source, distance/time/favorite filters, sortable columns
- **Messaging** — broadcast and direct messages, per-channel feeds, delivery status with RF-vs-MQTT ack path breakdown, unread indicators, persisted history that survives restarts
- **Traceroute** — async (the underlying library's own traceroute call blocks and prints to stdout, which doesn't work in a web app), with saved "proven path" routes per node
- **Config & channels** — every `LocalConfig`/`ModuleConfig` section and every channel, exposed as generic editable JSON rather than hand-built forms, so it covers the whole device without reimplementing each settings page. Secrets (WiFi PSK, private key, MQTT password, channel PSKs) are masked in the UI.
- **Channel health** — tracks the node's own self-reported channel utilization and airtime over time, correlated against message ack success rate, so you can see whether delivery failures line up with RF congestion
- **Admin actions** — reboot / shutdown / factory reset
- **Installable PWA** — manifest, icons, offline app-shell caching, safe-area padding for notched phones

## Requirements

- Python 3.9+
- A Meshtastic node reachable over USB serial and/or WiFi (TCP API on port 4403)
- The [`meshtastic`](https://pypi.org/project/meshtastic/) Python library talks the same protobuf protocol whether you connect over BLE, USB serial, or WiFi — CyberMesh uses serial and/or TCP (BLE isn't supported here)

## Setup

```bash
git clone https://github.com/WY6Y/CyberMesh.git
cd CyberMesh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env: set MESHTASTIC_HOST and/or MESHTASTIC_SERIAL_PORT for your node

python3 app.py
```

Then open `http://localhost:5090/` (or whatever `PORT` you set in `.env`).

### Running as a service

A sample systemd unit is in `packaging/cybermesh.service.example` — copy it to `/etc/systemd/system/cybermesh.service`, fix the paths/user, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cybermesh
```

Put it behind a reverse proxy (Caddy, nginx, etc.) if you want TLS or a friendly hostname — CyberMesh itself speaks plain HTTP and has **no authentication of its own**, so don't expose it directly to the internet. A VPN (Tailscale, WireGuard) or a proxy with its own auth layer in front is the intended setup.

## Architecture

- `app.py` — Flask app (routes, API)
- `mesh_client.py` — owns a single background thread holding the persistent connection to the node (auto-reconnect, watchdog, message/telemetry tracking)
- `store.py` — SQLite persistence (message history, node notes/favorites, saved traceroutes, telemetry history) — all in one file next to the app, no external database needed
- `templates/` / `static/` — server-rendered pages + the PWA shell

## License

MIT — see [LICENSE](LICENSE).
