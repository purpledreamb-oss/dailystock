"""
Vercel Serverless Function - Pick Vote
GET /api/vote?date=2026-04-06&market=us          -> get vote counts
GET /api/vote?date=2026-04-06&market=us&vote=up   -> cast vote (up/down)
"""

import os
import json
import requests
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

KV_REST_API_URL = os.getenv("KV_REST_API_URL")
KV_REST_API_TOKEN = os.getenv("KV_REST_API_TOKEN")

KV_HEADERS = {}
if KV_REST_API_TOKEN:
    KV_HEADERS = {
        "Authorization": f"Bearer {KV_REST_API_TOKEN}",
        "Content-Type": "application/json",
    }


def kv_command(*args):
    if not KV_REST_API_URL or not KV_REST_API_TOKEN:
        return None
    try:
        resp = requests.post(
            KV_REST_API_URL, headers=KV_HEADERS, json=list(args), timeout=10
        )
        if resp.status_code == 200:
            return resp.json().get("result")
    except Exception:
        pass
    return None


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        date = params.get("date", [""])[0]
        market = params.get("market", [""])[0].lower()
        vote = params.get("vote", [""])[0].lower()

        if not date or market not in ("us", "tw"):
            self._send_json(400, {"error": "Missing date or market parameter"})
            return

        key_up = f"vote:{date}:{market}:up"
        key_down = f"vote:{date}:{market}:down"

        # Cast vote
        if vote in ("up", "down"):
            key = key_up if vote == "up" else key_down
            kv_command("INCR", key)

        # Return counts
        up = int(kv_command("GET", key_up) or 0)
        down = int(kv_command("GET", key_down) or 0)
        total = up + down

        self._send_json(200, {
            "date": date,
            "market": market,
            "up": up,
            "down": down,
            "total": total,
            "up_pct": round(up / total * 100) if total > 0 else 50,
        })

    def _send_json(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode("utf-8"))
