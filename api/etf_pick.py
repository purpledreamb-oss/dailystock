"""
Vercel Serverless Function - Daily ETF Pick Reader
GET /api/etf_pick              -> today's ETF picks
GET /api/etf_pick?date=2026-04-05  -> specific date
GET /api/etf_pick?list=true    -> available dates
"""

import os
import json
import requests
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta

KV_REST_API_URL = os.getenv("KV_REST_API_URL")
KV_REST_API_TOKEN = os.getenv("KV_REST_API_TOKEN")
TW_TZ = timezone(timedelta(hours=8))

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

        # List mode
        if params.get("list", [""])[0] == "true":
            dates = []
            now = datetime.now(TW_TZ)
            for i in range(30):
                d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
                cached = kv_command("GET", f"etf_pick:{d}")
                if cached:
                    dates.append(d)
            self._send_json(200, {"dates": dates})
            return

        # Get specific date or today
        date = params.get("date", [""])[0]
        if not date:
            latest = kv_command("GET", "etf_pick:latest")
            if latest:
                date = latest
            else:
                date = datetime.now(TW_TZ).strftime("%Y-%m-%d")

        cached = kv_command("GET", f"etf_pick:{date}")
        if cached:
            try:
                data = json.loads(cached) if isinstance(cached, str) else cached
                self._send_json(200, data)
                return
            except (json.JSONDecodeError, TypeError):
                pass

        self._send_json(404, {"error": f"No ETF pick data for {date}"})

    def _send_json(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=300")
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode("utf-8"))
