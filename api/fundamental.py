"""
Vercel Serverless Function - Stock Fundamental Data API
GET /api/fundamental?symbol=2330
"""

import os
import json
import re
import requests
from concurrent.futures import ThreadPoolExecutor
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

CACHE_TTL = 3600  # 1 hour

YAHOO_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


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


def fetch_yahoo_chart_meta(symbol):
    """Fetch chart meta from Yahoo Finance (contains fundamental-like data)"""
    yahoo_symbol = to_yahoo_symbol(symbol)
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
        f"?range=1d&interval=1d"
    )
    headers = {"User-Agent": YAHOO_UA}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    chart = data.get("chart", {})
    result = chart.get("result", [])
    if not result:
        return None
    return result[0].get("meta", {})


def fetch_twse_revenue(symbol):
    """
    Fetch monthly revenue from TWSE for the last 6 months.
    Returns list of {"month": "YYYY-MM", "value": int} or empty list on failure.
    """
    revenue_list = []
    now = datetime.now(tz=timezone(timedelta(hours=8)))  # Taiwan time
    try:
        for months_ago in range(0, 6):
            dt = now.replace(day=1) - timedelta(days=months_ago * 30)
            date_str = dt.strftime("%Y%m01")
            url = (
                f"https://www.twse.com.tw/exchangeReport/FMSRFK"
                f"?response=json&date={date_str}&stockNo={symbol}"
            )
            headers = {"User-Agent": YAHOO_UA}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            body = resp.json()
            if body.get("stat") != "OK":
                continue
            data_rows = body.get("data", [])
            for row in data_rows:
                # Row format varies; typically [year, month, revenue, ...]
                if len(row) >= 3:
                    try:
                        year = int(row[0]) + 1911  # ROC year to AD
                        month = int(row[1])
                        value_str = str(row[2]).replace(",", "")
                        value = int(value_str)
                        month_str = f"{year}-{month:02d}"
                        # Avoid duplicates
                        if not any(r["month"] == month_str for r in revenue_list):
                            revenue_list.append({"month": month_str, "value": value})
                    except (ValueError, IndexError):
                        continue
    except Exception:
        pass
    # Sort by month descending
    revenue_list.sort(key=lambda r: r["month"], reverse=True)
    return revenue_list


def _fetch_twse_one_day(symbol, dt):
    """Fetch institutional data for a single day. Returns dict or None."""
    try:
        date_str = dt.strftime("%Y%m%d")
        date_display = dt.strftime("%Y-%m-%d")
        url = (
            f"https://www.twse.com.tw/fund/T86"
            f"?response=json&date={date_str}&selectType=ALL"
        )
        resp = requests.get(url, headers={"User-Agent": YAHOO_UA}, timeout=8)
        if resp.status_code != 200:
            return None
        body = resp.json()
        if body.get("stat") != "OK":
            return None
        for row in body.get("data", []):
            code = str(row[0]).strip()
            if code == symbol and len(row) >= 7:
                def parse_int(s):
                    return int(str(s).replace(",", "").replace(" ", ""))
                try:
                    return {
                        "date": date_display,
                        "foreign": parse_int(row[4]),
                        "investment_trust": parse_int(row[7]),
                        "dealer": parse_int(row[10]),
                    }
                except (ValueError, IndexError):
                    try:
                        return {
                            "date": date_display,
                            "foreign": parse_int(row[2]),
                            "investment_trust": parse_int(row[3]),
                            "dealer": parse_int(row[4]),
                        }
                    except (ValueError, IndexError):
                        pass
    except Exception:
        pass
    return None


def fetch_twse_institutional(symbol):
    """
    Fetch up to 10 trading days of institutional data in parallel.
    """
    now = datetime.now(tz=timezone(timedelta(hours=8)))
    # Collect weekday dates to try (up to 15 weekdays to cover holidays)
    dates = []
    for days_back in range(0, 22):
        dt = now - timedelta(days=days_back)
        if dt.weekday() < 5:
            dates.append(dt)
        if len(dates) >= 15:
            break

    # Fetch all days in parallel
    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_twse_one_day, symbol, dt): dt for dt in dates}
        for f in futures:
            r = f.result()
            if r:
                results.append(r)

    if not results:
        return None
    results.sort(key=lambda r: r["date"])
    return results[-10:]  # Keep last 10


def _fetch_yahoo_data(symbol):
    """Fetch all Yahoo data (chart meta + quoteSummary) in one flow. Returns dict."""
    data = {}
    yahoo_symbol = to_yahoo_symbol(symbol)

    try:
        # Chart meta for price / 52wk (fast, no auth needed)
        meta = fetch_yahoo_chart_meta(symbol)
        if meta:
            data["name"] = meta.get("shortName") or meta.get("longName") or meta.get("symbol", symbol)
            data["fiftyTwoWeekHigh"] = meta.get("fiftyTwoWeekHigh")
            data["fiftyTwoWeekLow"] = meta.get("fiftyTwoWeekLow")
            data["regularMarketPrice"] = meta.get("regularMarketPrice")
            data["chartPreviousClose"] = meta.get("chartPreviousClose")
    except Exception:
        pass

    try:
        session = requests.Session()
        session.headers.update({"User-Agent": YAHOO_UA})
        session.get("https://fc.yahoo.com", timeout=5, allow_redirects=True)
        crumb_resp = session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=5)
        crumb = crumb_resp.text.strip() if crumb_resp.status_code == 200 else ""

        if crumb:
            url = (
                f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{yahoo_symbol}"
                f"?modules=defaultKeyStatistics,summaryDetail,price,incomeStatementHistoryQuarterly,incomeStatementHistory"
                f"&crumb={crumb}"
            )
            resp = session.get(url, timeout=10)
            if resp.status_code == 200:
                body = resp.json()
                qs_result = body.get("quoteSummary", {}).get("result", [])
                if qs_result:
                    modules = qs_result[0]
                    summary = modules.get("summaryDetail", {})
                    key_stats = modules.get("defaultKeyStatistics", {})
                    price_mod = modules.get("price", {})

                    def raw_val(obj, key):
                        v = obj.get(key, {})
                        return v.get("raw") if isinstance(v, dict) else v

                    data["pe"] = raw_val(summary, "trailingPE") or raw_val(key_stats, "trailingPE")
                    data["pb"] = raw_val(key_stats, "priceToBook")
                    data["eps"] = raw_val(key_stats, "trailingEps") or raw_val(summary, "trailingEps")
                    data["dividendYield"] = raw_val(summary, "dividendYield")
                    if data.get("dividendYield") is not None:
                        data["dividendYield"] = round(data["dividendYield"] * 100, 2)
                    data["marketCap"] = raw_val(price_mod, "marketCap") or raw_val(summary, "marketCap")

                    if not data.get("name"):
                        data["name"] = price_mod.get("shortName") or price_mod.get("longName") or symbol

                    # Annual revenue (up to 4 years)
                    rev_list = []
                    inc_a = modules.get("incomeStatementHistory", {})
                    for stmt in inc_a.get("incomeStatementHistory", []):
                        end_date = stmt.get("endDate", {}).get("fmt", "")
                        total_rev = stmt.get("totalRevenue", {})
                        rev_raw = total_rev.get("raw") if isinstance(total_rev, dict) else None
                        if end_date and rev_raw:
                            year = end_date.split("-")[0]
                            rev_list.append({"month": year, "value": rev_raw, "type": "annual"})

                    # Quarterly revenue (recent 4 quarters)
                    inc_q = modules.get("incomeStatementHistoryQuarterly", {})
                    for stmt in inc_q.get("incomeStatementHistory", []):
                        end_date = stmt.get("endDate", {}).get("fmt", "")
                        total_rev = stmt.get("totalRevenue", {})
                        rev_raw = total_rev.get("raw") if isinstance(total_rev, dict) else None
                        if end_date and rev_raw:
                            parts = end_date.split("-")
                            if len(parts) >= 2:
                                month = int(parts[1])
                                quarter = (month - 1) // 3 + 1
                                label = f"{parts[0]}-Q{quarter}"
                                rev_list.append({"month": label, "value": rev_raw, "type": "quarterly"})
                    if rev_list:
                        data["revenue"] = rev_list
    except Exception:
        pass

    return data


def build_fundamental_data(symbol):
    """Build fundamental data object for a given symbol."""
    market = "TW" if is_taiwan_stock(symbol) else "US"
    result = {
        "symbol": symbol,
        "name": None,
        "market": market,
        "pe": None,
        "pb": None,
        "dividendYield": None,
        "marketCap": None,
        "eps": None,
        "fiftyTwoWeekHigh": None,
        "fiftyTwoWeekLow": None,
        "regularMarketPrice": None,
        "revenue": [],
        "institutional": None,
    }

    # Run Yahoo and TWSE in parallel
    with ThreadPoolExecutor(max_workers=2) as pool:
        yahoo_future = pool.submit(_fetch_yahoo_data, symbol)
        inst_future = pool.submit(fetch_twse_institutional, symbol) if market == "TW" else None

        yahoo_data = yahoo_future.result()
        result.update({k: v for k, v in yahoo_data.items() if v is not None})

        if inst_future:
            try:
                result["institutional"] = inst_future.result()
            except Exception:
                pass

    if not result["name"]:
        result["name"] = symbol

    return result


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        symbol = params.get("symbol", [""])[0].strip().upper()

        if not symbol:
            self._send_json(400, {"error": "Missing required parameter: symbol"})
            return

        cache_key = f"fundamental:{symbol}"
        nocache = params.get("nocache", [""])[0] == "1"

        # Try cache first
        cached = None if nocache else kv_command("GET", cache_key)
        if cached:
            try:
                data = json.loads(cached) if isinstance(cached, str) else cached
                self._send_json(200, data, cache_hit=True)
                return
            except (json.JSONDecodeError, TypeError):
                pass

        # Build fundamental data
        try:
            data = build_fundamental_data(symbol)

            # Cache for 1 hour
            kv_command("SET", cache_key, json.dumps(data, ensure_ascii=False), "EX", CACHE_TTL)

            self._send_json(200, data, cache_hit=False)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 502
            self._send_json(status, {"error": f"Data source API error: {status}"})
        except requests.exceptions.Timeout:
            self._send_json(504, {"error": "Data source API timeout"})
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
            self.send_header("Cache-Control", "public, max-age=300")
            self.send_header("X-Cache", "HIT")
        elif cache_hit is False:
            self.send_header("Cache-Control", "public, max-age=300")
            self.send_header("X-Cache", "MISS")
        else:
            self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
