"""
Vercel Serverless Function - Stock Quote Proxy API
GET /api/quote?symbols=2330,2317,AAPL,^DJI  -> real-time stock quotes
"""

import os
import json
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    """Execute Upstash Redis REST API command."""
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


# ---------------------------------------------------------------------------
# TWSE / OTC real-time helpers
# ---------------------------------------------------------------------------

TWSE_REALTIME_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"


def _build_twse_ex_ch(codes, market="tse"):
    """Build ex_ch param like tse_2330.tw|tse_2317.tw"""
    return "|".join(f"{market}_{c}.tw" for c in codes)


def _fetch_twse_realtime(codes):
    """
    Fetch real-time quotes from TWSE for a list of numeric stock codes.
    Try TSE first; for codes that return no data, retry with OTC.
    Returns dict keyed by stock code.
    """
    if not codes:
        return {}

    results = {}
    remaining = list(codes)

    # --- Try TSE listed first ---
    ex_ch = _build_twse_ex_ch(remaining, "tse")
    try:
        resp = requests.get(
            TWSE_REALTIME_URL,
            params={"ex_ch": ex_ch},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        data = resp.json()
        for item in data.get("msgArray", []):
            code = item.get("c", "")
            if code in remaining:
                parsed = _parse_twse_item(item)
                if parsed is not None:
                    results[code] = parsed
                    remaining.remove(code)
    except Exception:
        pass

    # --- Retry remaining codes as OTC ---
    if remaining:
        ex_ch = _build_twse_ex_ch(remaining, "otc")
        try:
            resp = requests.get(
                TWSE_REALTIME_URL,
                params={"ex_ch": ex_ch},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            data = resp.json()
            for item in data.get("msgArray", []):
                code = item.get("c", "")
                if code in remaining:
                    parsed = _parse_twse_item(item)
                    if parsed is not None:
                        results[code] = parsed
        except Exception:
            pass

    return results


def _parse_twse_item(item):
    """Parse a single TWSE msgArray item into unified format."""
    try:
        name = item.get("n", "").strip()
        code = item.get("c", "")
        raw_price = item.get("z", "-")
        yesterday = item.get("y", "0")
        raw_volume = item.get("v", "0")

        # z == "-" means no trade yet, fall back to yesterday close
        if raw_price == "-" or raw_price == "":
            price = float(yesterday)
        else:
            price = float(raw_price)

        prev_close = float(yesterday) if yesterday and yesterday != "-" else 0.0
        change = round(price - prev_close, 2)
        pct = round((change / prev_close) * 100, 2) if prev_close else 0.0

        # TWSE volume unit is 張 (= 1000 shares); report as-is
        volume = int(raw_volume) if raw_volume and raw_volume != "-" else 0

        return {
            "name": name,
            "price": price,
            "change": change,
            "pct": pct,
            "volume": volume,
        }
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Yahoo Finance helper
# ---------------------------------------------------------------------------

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _fetch_yahoo_single(symbol):
    """Fetch a single Yahoo Finance quote. Returns (symbol, data_dict) or (symbol, None)."""
    try:
        resp = requests.get(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={"range": "1d", "interval": "1m"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        data = resp.json()
        meta = data["chart"]["result"][0]["meta"]
        price = meta.get("regularMarketPrice", 0)
        prev_close = meta.get("chartPreviousClose", 0)
        name = meta.get("shortName", symbol)

        change = round(price - prev_close, 2)
        pct = round((change / prev_close) * 100, 2) if prev_close else 0.0

        volume = 0
        try:
            indicators = data["chart"]["result"][0].get("indicators", {})
            vol_series = indicators.get("quote", [{}])[0].get("volume", [])
            volume = sum(v for v in vol_series if v is not None)
        except Exception:
            pass

        return symbol, {
            "name": name,
            "price": round(price, 2),
            "change": change,
            "pct": pct,
            "volume": volume,
        }
    except Exception:
        return symbol, None


def _fetch_yahoo_quotes(symbols):
    """Fetch quotes from Yahoo Finance in parallel. Returns dict."""
    results = {}
    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(_fetch_yahoo_single, sym): sym for sym in symbols}
        for f in as_completed(futures):
            sym, data = f.result()
            if data:
                results[sym] = data
    return results


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

def _is_tw_stock(symbol):
    """Taiwan stocks: digits with optional letter suffix (e.g. 2330, 00878, 00937B)."""
    import re
    return bool(re.match(r"^\d{4,6}[A-Z]?$", symbol))


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        raw_symbols = params.get("symbols", [""])[0]
        if not raw_symbols:
            self._send_json(400, {"error": "Missing 'symbols' query parameter"})
            return

        symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()]
        if not symbols:
            self._send_json(400, {"error": "No valid symbols provided"})
            return

        # --- Check KV cache ---
        symbols_key = ",".join(sorted(symbols))
        cache_key = "quote:" + hashlib.md5(symbols_key.encode()).hexdigest()

        cached = kv_command("GET", cache_key)
        if cached:
            try:
                payload = json.loads(cached) if isinstance(cached, str) else cached
                self._send_json(200, payload, cache=True)
                return
            except (json.JSONDecodeError, TypeError):
                pass

        # --- Split into TW vs US/intl ---
        tw_codes = [s for s in symbols if _is_tw_stock(s)]
        us_symbols = [s for s in symbols if not _is_tw_stock(s)]

        data = {}

        # Fetch TW stocks from TWSE realtime
        tw_fallback = []
        if tw_codes:
            tw_results = _fetch_twse_realtime(tw_codes)
            for code in tw_codes:
                result = tw_results.get(code)
                if result and result["change"] != 0:
                    data[code] = result
                elif result:
                    # Market closed (z="-"), use Yahoo as fallback for change/volume
                    tw_fallback.append(code)
                    data[code] = result  # Keep as default in case Yahoo also fails
                else:
                    tw_fallback.append(code)

        # Fetch US/intl stocks + TW fallback via Yahoo (parallel)
        yahoo_symbols = list(us_symbols)
        if tw_fallback:
            yahoo_symbols.extend([f"{c}.TW" for c in tw_fallback])

        if yahoo_symbols:
            us_results = _fetch_yahoo_quotes(yahoo_symbols)
            for sym, val in us_results.items():
                if sym.endswith(".TW"):
                    # Map back to plain code, merge with TWSE name
                    code = sym.replace(".TW", "")
                    if code in data and data[code].get("name"):
                        val["name"] = data[code]["name"]  # Keep Chinese name from TWSE
                    data[code] = val
                else:
                    data[sym] = val

        now = datetime.now(TW_TZ).isoformat()

        payload = {
            "data": data,
            "updated": now,
        }

        # --- Store in KV cache for 60 seconds ---
        try:
            kv_command("SET", cache_key, json.dumps(payload), "EX", 60)
        except Exception:
            pass

        self._send_json(200, payload, cache=False)

    def _send_json(self, status, body, cache=False):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        if cache:
            self.send_header("Cache-Control", "public, max-age=60")
        else:
            self.send_header("Cache-Control", "public, max-age=60")
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode("utf-8"))
