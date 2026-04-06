"""
Vercel Serverless Function - Stock Chart (OHLCV) API
GET /api/chart?symbol=2330&range=3mo&interval=1d
"""

import os
import json
import re
import requests
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta

KV_REST_API_URL = os.getenv("KV_REST_API_URL")
KV_REST_API_TOKEN = os.getenv("KV_REST_API_TOKEN")

KV_HEADERS = {}
if KV_REST_API_TOKEN:
    KV_HEADERS = {
        "Authorization": f"Bearer {KV_REST_API_TOKEN}",
        "Content-Type": "application/json",
    }

VALID_RANGES = {"1mo", "3mo", "6mo", "1y", "2y", "5y"}
VALID_INTERVALS = {"1d", "1wk", "1mo"}
CACHE_TTL = 300  # 5 minutes


def kv_command(*args):
    """Execute Upstash Redis REST API command"""
    if not KV_REST_API_URL or not KV_REST_API_TOKEN:
        return None
    try:
        resp = requests.post(
            KV_REST_API_URL,
            headers=KV_HEADERS,
            json=list(args),
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("result")
    except Exception:
        pass
    return None


def is_taiwan_stock(symbol):
    """Check if a symbol is a Taiwan stock (digits with optional letter suffix, e.g. 2330, 00937B)"""
    return bool(re.match(r"^\d{4,6}[A-Z]?$", symbol))


def to_yahoo_symbol(symbol):
    """Convert symbol to Yahoo Finance format"""
    if is_taiwan_stock(symbol):
        return f"{symbol}.TW"
    return symbol


def fetch_chart_from_yahoo(symbol, range_val, interval):
    """Fetch OHLCV data from Yahoo Finance v8 chart API"""
    yahoo_symbol = to_yahoo_symbol(symbol)
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
        f"?range={range_val}&interval={interval}"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def parse_yahoo_chart(raw, original_symbol):
    """Parse Yahoo Finance chart response into TradingView format"""
    chart = raw.get("chart", {})
    result = chart.get("result", [])
    if not result:
        return None

    data_block = result[0]
    meta = data_block.get("meta", {})
    timestamps = data_block.get("timestamp", [])
    indicators = data_block.get("indicators", {})
    quotes = indicators.get("quote", [{}])[0]

    opens = quotes.get("open", [])
    highs = quotes.get("high", [])
    lows = quotes.get("low", [])
    closes = quotes.get("close", [])
    volumes = quotes.get("volume", [])

    # Build OHLCV array, filtering out null values
    ohlcv = []
    for i, ts in enumerate(timestamps):
        o = opens[i] if i < len(opens) else None
        h = highs[i] if i < len(highs) else None
        lo = lows[i] if i < len(lows) else None
        c = closes[i] if i < len(closes) else None
        v = volumes[i] if i < len(volumes) else None

        # Skip entries with any null OHLC value
        if any(val is None for val in (o, h, lo, c)):
            continue

        date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        entry = {
            "time": date_str,
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(lo, 2),
            "close": round(c, 2),
        }
        if v is not None:
            entry["volume"] = v
        ohlcv.append(entry)

    # Extract name from meta
    name = meta.get("shortName") or meta.get("symbol", original_symbol)

    return {
        "symbol": original_symbol,
        "name": name,
        "data": ohlcv,
        "meta": {
            "currency": meta.get("currency", ""),
            "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
            "regularMarketPrice": meta.get("regularMarketPrice"),
            "chartPreviousClose": meta.get("chartPreviousClose"),
        },
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        symbol = params.get("symbol", [""])[0].strip().upper()
        range_val = params.get("range", ["3mo"])[0]
        interval = params.get("interval", ["1d"])[0]

        # Validate parameters
        if not symbol:
            self._send_json(400, {"error": "Missing required parameter: symbol"})
            return

        if range_val not in VALID_RANGES:
            self._send_json(400, {"error": f"Invalid range. Must be one of: {', '.join(sorted(VALID_RANGES))}"})
            return

        if interval not in VALID_INTERVALS:
            self._send_json(400, {"error": f"Invalid interval. Must be one of: {', '.join(sorted(VALID_INTERVALS))}"})
            return

        cache_key = f"chart:{symbol}:{range_val}:{interval}"

        # Try cache first
        cached = kv_command("GET", cache_key)
        if cached:
            try:
                data = json.loads(cached) if isinstance(cached, str) else cached
                self._send_json(200, data, cache_hit=True)
                return
            except (json.JSONDecodeError, TypeError):
                pass

        # Fetch from Yahoo Finance
        try:
            raw = fetch_chart_from_yahoo(symbol, range_val, interval)
            data = parse_yahoo_chart(raw, symbol)
            if not data:
                self._send_json(404, {"error": f"No chart data found for symbol: {symbol}"})
                return

            # Cache for 5 minutes
            kv_command("SET", cache_key, json.dumps(data, ensure_ascii=False), "EX", CACHE_TTL)

            self._send_json(200, data, cache_hit=False)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 502
            self._send_json(status, {"error": f"Yahoo Finance API error: {status}"})
        except requests.exceptions.Timeout:
            self._send_json(504, {"error": "Yahoo Finance API timeout"})
        except Exception as e:
            self._send_json(500, {"error": f"Internal error: {str(e)}"})

    def do_OPTIONS(self):
        """Handle CORS preflight"""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _send_json(self, status_code, data, cache_hit=None):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        if cache_hit is True:
            self.send_header("Cache-Control", "public, max-age=60")
            self.send_header("X-Cache", "HIT")
        elif cache_hit is False:
            self.send_header("Cache-Control", "public, max-age=60")
            self.send_header("X-Cache", "MISS")
        else:
            self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
