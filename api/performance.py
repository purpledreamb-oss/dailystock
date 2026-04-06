"""
Vercel Serverless Function - Performance Tracking
GET /api/performance -> returns historical pick performance with current prices
"""

import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta
import re

KV_REST_API_URL = os.getenv("KV_REST_API_URL")
KV_REST_API_TOKEN = os.getenv("KV_REST_API_TOKEN")
TW_TZ = timezone(timedelta(hours=8))
PERF_CACHE_TTL = 14400  # 4 hours

KV_HEADERS = {}
if KV_REST_API_TOKEN:
    KV_HEADERS = {
        "Authorization": f"Bearer {KV_REST_API_TOKEN}",
        "Content-Type": "application/json",
    }

YAHOO_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


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


def is_taiwan_stock(symbol):
    return bool(re.match(r"^\d{4,6}[A-Z]?$", symbol))


def fetch_current_price(symbol):
    """Fetch current price from Yahoo Finance."""
    try:
        yahoo_sym = f"{symbol}.TW" if is_taiwan_stock(symbol) else symbol
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?range=5d&interval=1d"
        resp = requests.get(url, headers={"User-Agent": YAHOO_UA}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            result = data.get("chart", {}).get("result", [])
            if result:
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice")
                if price:
                    return round(price, 2)
    except Exception:
        pass
    return None


def build_performance_data():
    """Build performance data for all historical picks."""
    # Get dates list
    dates_raw = kv_command("GET", "pick:dates")
    if not dates_raw:
        # Fallback: scan last 90 days
        now = datetime.now(TW_TZ)
        dates_list = []
        for i in range(90):
            d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            if kv_command("GET", f"pick:{d}"):
                dates_list.append(d)
        dates_list.sort()
    else:
        try:
            dates_list = json.loads(dates_raw) if isinstance(dates_raw, str) else dates_raw
        except Exception:
            dates_list = []

    if not dates_list:
        return {"picks": [], "summary": {}}

    # Load all picks
    picks = []
    symbols_to_fetch = set()
    for date in dates_list:
        raw = kv_command("GET", f"pick:{date}")
        if not raw:
            continue
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            continue
        for market in ["us", "tw"]:
            p = data.get(market)
            if not p:
                continue
            sym = p.get("symbol")
            pick_price = p.get("pick_price") or p.get("price")
            if sym and pick_price:
                picks.append({
                    "date": date,
                    "market": market.upper(),
                    "symbol": sym,
                    "name": p.get("name_zh") or p.get("name", sym),
                    "score": p.get("score", 0),
                    "pick_price": pick_price,
                })
                symbols_to_fetch.add(sym)

    # Fetch current prices in parallel
    current_prices = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch_current_price, sym): sym for sym in symbols_to_fetch}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                price = future.result()
                if price:
                    current_prices[sym] = price
            except Exception:
                pass

    # Calculate returns
    today = datetime.now(TW_TZ).strftime("%Y-%m-%d")
    results = []
    wins = 0
    total_return = 0

    for p in picks:
        current = current_prices.get(p["symbol"])
        if current and p["pick_price"] > 0:
            ret = round((current - p["pick_price"]) / p["pick_price"] * 100, 2)
            days = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(p["date"], "%Y-%m-%d")).days
            p["current_price"] = current
            p["return_pct"] = ret
            p["days_held"] = days
            if ret > 0:
                wins += 1
            total_return += ret
        else:
            p["current_price"] = None
            p["return_pct"] = None
            p["days_held"] = None
        results.append(p)

    # Sort by date descending
    results.sort(key=lambda x: x["date"], reverse=True)

    valid = [r for r in results if r["return_pct"] is not None]
    total = len(valid)
    summary = {
        "total_picks": total,
        "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
        "avg_return": round(total_return / total, 2) if total > 0 else 0,
        "best": max(valid, key=lambda x: x["return_pct"]) if valid else None,
        "worst": min(valid, key=lambda x: x["return_pct"]) if valid else None,
    }

    return {"picks": results, "summary": summary}


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        # Check cache
        cached = kv_command("GET", "perf:cache")
        if cached:
            try:
                data = json.loads(cached) if isinstance(cached, str) else cached
                self._send_json(200, data)
                return
            except Exception:
                pass

        # Build fresh
        result = build_performance_data()

        # Cache for 4 hours
        try:
            kv_command("SET", "perf:cache", json.dumps(result, ensure_ascii=False), "EX", PERF_CACHE_TTL)
        except Exception:
            pass

        self._send_json(200, result)

    def _send_json(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=300")
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode("utf-8"))
