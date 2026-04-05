"""
Vercel Serverless Function - Stock Fundamental Data API
GET /api/fundamental?symbol=2330
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
    """Check if a symbol is a Taiwan stock (pure digits, e.g. 2330)"""
    return bool(re.match(r"^\d{4,6}$", symbol))


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


def fetch_twse_institutional(symbol):
    """
    Fetch latest institutional trading data from TWSE.
    Returns {"foreign": int, "investment_trust": int, "dealer": int} or None.
    """
    now = datetime.now(tz=timezone(timedelta(hours=8)))
    # Try today and previous 5 days (in case of holidays)
    for days_back in range(0, 6):
        try:
            dt = now - timedelta(days=days_back)
            date_str = dt.strftime("%Y%m%d")
            url = (
                f"https://www.twse.com.tw/fund/T86"
                f"?response=json&date={date_str}&selectType=ALL"
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
                # Row[0] is stock code, strip whitespace
                code = str(row[0]).strip()
                if code == symbol and len(row) >= 7:
                    def parse_int(s):
                        return int(str(s).replace(",", "").replace(" ", ""))
                    # Columns: code, name, foreign_buy, foreign_sell, foreign_net,
                    #          investment_trust_net, dealer_net (layout may vary)
                    # Common layout: [code, name, foreign_net, inv_trust_net, dealer_net, total]
                    # The exact columns depend on TWSE format; try to extract nets
                    try:
                        foreign_net = parse_int(row[4])       # 外資淨買賣
                        trust_net = parse_int(row[7])          # 投信淨買賣
                        dealer_net = parse_int(row[10])        # 自營商淨買賣
                        return {
                            "foreign": foreign_net,
                            "investment_trust": trust_net,
                            "dealer": dealer_net,
                        }
                    except (ValueError, IndexError):
                        # Try simpler column layout
                        try:
                            foreign_net = parse_int(row[2])
                            trust_net = parse_int(row[3])
                            dealer_net = parse_int(row[4])
                            return {
                                "foreign": foreign_net,
                                "investment_trust": trust_net,
                                "dealer": dealer_net,
                            }
                        except (ValueError, IndexError):
                            pass
            # If we got a valid response but stock not found, stop trying older dates
            if data_rows:
                break
        except Exception:
            continue
    return None


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

    # 1. Yahoo Finance chart meta (works for both TW and US)
    try:
        meta = fetch_yahoo_chart_meta(symbol)
        if meta:
            result["name"] = meta.get("shortName") or meta.get("longName") or meta.get("symbol", symbol)
            result["fiftyTwoWeekHigh"] = meta.get("fiftyTwoWeekHigh")
            result["fiftyTwoWeekLow"] = meta.get("fiftyTwoWeekLow")
            result["regularMarketPrice"] = meta.get("regularMarketPrice")
            result["chartPreviousClose"] = meta.get("chartPreviousClose")
    except Exception:
        pass

    # 2. Yahoo Finance quoteSummary for PE, PB, EPS, marketCap, dividendYield
    #    Requires crumb-based auth since Yahoo blocked unauthenticated access
    try:
        yahoo_symbol = to_yahoo_symbol(symbol)
        session = requests.Session()
        session.headers.update({"User-Agent": YAHOO_UA})

        # Get cookies from Yahoo
        session.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)

        # Get crumb
        crumb_resp = session.get(
            "https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10
        )
        crumb = crumb_resp.text.strip() if crumb_resp.status_code == 200 else ""

        if crumb:
            url = (
                f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{yahoo_symbol}"
                f"?modules=defaultKeyStatistics,summaryDetail,price,incomeStatementHistoryQuarterly&crumb={crumb}"
            )
            resp = session.get(url, timeout=15)
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
                        if isinstance(v, dict):
                            return v.get("raw")
                        return v

                    result["pe"] = raw_val(summary, "trailingPE") or raw_val(key_stats, "trailingPE")
                    result["pb"] = raw_val(key_stats, "priceToBook")
                    result["eps"] = raw_val(key_stats, "trailingEps") or raw_val(summary, "trailingEps")
                    result["dividendYield"] = raw_val(summary, "dividendYield")
                    if result["dividendYield"] is not None:
                        result["dividendYield"] = round(result["dividendYield"] * 100, 2)

                    mc = raw_val(price_mod, "marketCap") or raw_val(summary, "marketCap")
                    result["marketCap"] = mc

                    # Fill name from price module if not yet set
                    if not result["name"]:
                        result["name"] = (
                            price_mod.get("shortName")
                            or price_mod.get("longName")
                            or symbol
                        )

                    # Quarterly revenue from income statement
                    inc_q = modules.get("incomeStatementHistoryQuarterly", {})
                    stmts = inc_q.get("incomeStatementHistory", [])
                    rev_list = []
                    for stmt in stmts:
                        end_date = stmt.get("endDate", {}).get("fmt", "")
                        total_rev = stmt.get("totalRevenue", {})
                        rev_raw = total_rev.get("raw") if isinstance(total_rev, dict) else None
                        if end_date and rev_raw:
                            # Convert "2025-12-31" to "2025-Q4" format
                            parts = end_date.split("-")
                            if len(parts) >= 2:
                                month = int(parts[1])
                                quarter = (month - 1) // 3 + 1
                                label = f"{parts[0]}-Q{quarter}"
                                rev_list.append({"month": label, "value": rev_raw})
                    if rev_list:
                        result["revenue"] = rev_list

    except Exception:
        pass

    # 3. Taiwan-specific data
    if market == "TW":

        # Institutional trading from TWSE
        try:
            result["institutional"] = fetch_twse_institutional(symbol)
        except Exception:
            pass

    # Fallback name
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
