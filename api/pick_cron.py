"""
Vercel Serverless Function - Daily Stock Pick Cron
GET /api/pick_cron  -> screen stocks, score, AI recommend, store in KV

Flow:
1. Verify CRON_SECRET
2. Check trading day
3. Fetch technical + fundamental data for 30 US + 30 TW candidates
4. Score all candidates
5. Pick top US + top TW
6. Generate AI recommendation text
7. Store in KV (pick:{date}, pick:latest)
"""

import os
import json
import re
import math
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta

ANTHROPIC_API_KEY = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
CRON_SECRET = os.getenv("CRON_SECRET")
KV_REST_API_URL = os.getenv("KV_REST_API_URL")
KV_REST_API_TOKEN = os.getenv("KV_REST_API_TOKEN")

TW_TZ = timezone(timedelta(hours=8))
PICK_TTL = 604800  # 7 days
LATEST_TTL = 90000  # 25 hours

KV_HEADERS = {}
if KV_REST_API_TOKEN:
    KV_HEADERS = {
        "Authorization": f"Bearer {KV_REST_API_TOKEN}",
        "Content-Type": "application/json",
    }

YAHOO_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# ---------------------------------------------------------------------------
# Candidate stock pools
# ---------------------------------------------------------------------------
US_CANDIDATES = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "JPM", "V",
    "UNH", "MA", "HD", "COST", "PG", "JNJ", "ABBV", "CRM", "AMD", "NFLX",
    "PEP", "KO", "MRK", "LLY", "ORCL", "CSCO", "ACN", "ADBE", "TXN", "QCOM",
]

TW_CANDIDATES = [
    "2330", "2317", "2454", "2412", "2308", "3711", "2881", "2882", "2891", "2303",
    "2886", "2884", "3008", "2357", "2382", "6505", "1301", "1303", "2002", "1216",
    "5871", "2207", "3034", "2327", "4904", "2395", "3037", "6669", "2379", "3231",
]

# ---------------------------------------------------------------------------
# Holiday lists (update annually)
# ---------------------------------------------------------------------------
TW_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-27", "2026-01-28", "2026-01-29", "2026-01-30",
    "2026-02-28", "2026-04-03", "2026-04-04", "2026-04-05", "2026-05-01",
    "2026-05-31", "2026-06-01", "2026-10-05", "2026-10-06", "2026-10-07",
}
US_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
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


def is_trading_day(date_str, market):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if dt.weekday() >= 5:
        return False
    if market == "TW":
        return date_str not in TW_HOLIDAYS_2026
    return date_str not in US_HOLIDAYS_2026


def is_taiwan_stock(symbol):
    return bool(re.match(r"^\d{4,6}$", symbol))


def to_yahoo_symbol(symbol):
    if is_taiwan_stock(symbol):
        return f"{symbol}.TW"
    return symbol


# ---------------------------------------------------------------------------
# Technical indicator calculations (Python port)
# ---------------------------------------------------------------------------

def calc_sma(closes, period):
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    if len(gains) < period:
        return None
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def calc_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return None, None, None

    def ema(data, period):
        k = 2 / (period + 1)
        result = [data[0]]
        for i in range(1, len(data)):
            result.append(data[i] * k + result[-1] * (1 - k))
        return result

    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)
    histogram = [m - s for m, s in zip(macd_line, signal_line)]
    return macd_line[-1], signal_line[-1], histogram[-1]


def calc_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_chart_data(symbol, session=None):
    """Fetch 6mo daily OHLCV from Yahoo Finance."""
    yahoo_sym = to_yahoo_symbol(symbol)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?range=6mo&interval=1d"
    s = session or requests
    try:
        resp = s.get(url, headers={"User-Agent": YAHOO_UA}, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None
        meta = result[0].get("meta", {})
        ts = result[0].get("timestamp", [])
        q = result[0].get("indicators", {}).get("quote", [{}])[0]
        opens = q.get("open", [])
        highs = q.get("high", [])
        lows = q.get("low", [])
        closes = q.get("close", [])
        volumes = q.get("volume", [])
        # Filter nulls
        valid = []
        for i in range(len(ts)):
            c = closes[i] if i < len(closes) else None
            h = highs[i] if i < len(highs) else None
            lo = lows[i] if i < len(lows) else None
            v = volumes[i] if i < len(volumes) else None
            if c is not None and h is not None and lo is not None:
                valid.append({"close": c, "high": h, "low": lo, "open": opens[i], "volume": v or 0})
        return {
            "symbol": symbol,
            "name": meta.get("shortName") or meta.get("symbol", symbol),
            "price": meta.get("regularMarketPrice", 0),
            "prev_close": meta.get("chartPreviousClose", 0),
            "high52": meta.get("fiftyTwoWeekHigh"),
            "low52": meta.get("fiftyTwoWeekLow"),
            "candles": valid,
        }
    except Exception:
        return None


def fetch_fundamental_batch(symbols, session):
    """Fetch PE/PB/EPS/dividendYield/marketCap for multiple symbols using shared session+crumb."""
    results = {}
    try:
        session.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)
        crumb_resp = session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        crumb = crumb_resp.text.strip() if crumb_resp.status_code == 200 else ""
    except Exception:
        crumb = ""

    if not crumb:
        return results

    def fetch_one(sym):
        yahoo_sym = to_yahoo_symbol(sym)
        try:
            url = (
                f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{yahoo_sym}"
                f"?modules=defaultKeyStatistics,summaryDetail,price&crumb={crumb}"
            )
            resp = session.get(url, timeout=12)
            if resp.status_code != 200:
                return sym, None
            body = resp.json()
            qs = body.get("quoteSummary", {}).get("result", [])
            if not qs:
                return sym, None
            modules = qs[0]
            sd = modules.get("summaryDetail", {})
            ks = modules.get("defaultKeyStatistics", {})
            pm = modules.get("price", {})

            def rv(obj, key):
                v = obj.get(key, {})
                return v.get("raw") if isinstance(v, dict) else v

            pe = rv(sd, "trailingPE") or rv(ks, "trailingPE")
            pb = rv(ks, "priceToBook")
            eps = rv(ks, "trailingEps") or rv(sd, "trailingEps")
            dy = rv(sd, "dividendYield")
            if dy is not None:
                dy = round(dy * 100, 2)
            mc = rv(pm, "marketCap") or rv(sd, "marketCap")
            return sym, {"pe": pe, "pb": pb, "eps": eps, "dividendYield": dy, "marketCap": mc}
        except Exception:
            return sym, None

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch_one, s): s for s in symbols}
        for f in as_completed(futures):
            sym, data = f.result()
            if data:
                results[sym] = data
    return results


def fetch_twse_institutional():
    """Fetch ALL stocks institutional trading from TWSE T86 (single bulk request)."""
    now = datetime.now(tz=TW_TZ)
    for days_back in range(0, 6):
        try:
            dt = now - timedelta(days=days_back)
            date_str = dt.strftime("%Y%m%d")
            url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
            resp = requests.get(url, headers={"User-Agent": YAHOO_UA}, timeout=10)
            if resp.status_code != 200:
                continue
            body = resp.json()
            if body.get("stat") != "OK":
                continue
            result = {}
            for row in body.get("data", []):
                code = str(row[0]).strip()
                try:
                    def pi(s):
                        return int(str(s).replace(",", "").replace(" ", ""))
                    foreign_net = pi(row[4])
                    trust_net = pi(row[7]) if len(row) > 7 else 0
                    dealer_net = pi(row[10]) if len(row) > 10 else 0
                    result[code] = {
                        "foreign": foreign_net,
                        "trust": trust_net,
                        "dealer": dealer_net,
                    }
                except (ValueError, IndexError):
                    pass
            return result
        except Exception:
            continue
    return {}


# ---------------------------------------------------------------------------
# Scoring functions
# ---------------------------------------------------------------------------

def score_us_stock(chart, fundamental):
    """Score a US stock out of 100 points."""
    if not chart or not chart.get("candles") or len(chart["candles"]) < 30:
        return 0, {}

    candles = chart["candles"]
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    price = chart["price"] or closes[-1]
    breakdown = {"technical": 0, "fundamental": 0, "risk": 0}

    # === Technical (40 pts) ===
    tech = 0

    # RSI (10 pts)
    rsi = calc_rsi(closes)
    if rsi is not None:
        if 30 <= rsi <= 50:
            tech += 10
        elif 50 < rsi <= 70:
            tech += 7
        elif rsi < 30:
            tech += 5
        else:
            tech += 3

    # MACD histogram (10 pts)
    _, _, hist = calc_macd(closes)
    if hist is not None:
        prev_closes = closes[:-1]
        _, _, prev_hist = calc_macd(prev_closes) if len(prev_closes) >= 35 else (None, None, None)
        if hist > 0 and (prev_hist is None or hist > prev_hist):
            tech += 10
        elif hist > 0:
            tech += 6
        elif hist < 0 and (prev_hist is None or hist > prev_hist):
            tech += 4
        else:
            tech += 2

    # Price vs MA200 (10 pts)
    ma200 = calc_sma(closes, 200) if len(closes) >= 200 else calc_sma(closes, len(closes))
    if ma200 and price > 0:
        if price > ma200:
            tech += 10
        elif price > ma200 * 0.95:
            tech += 6
        else:
            tech += 3

    # Deviation from MA50 (10 pts)
    ma50 = calc_sma(closes, 50) if len(closes) >= 50 else calc_sma(closes, min(len(closes), 20))
    if ma50 and price > 0:
        dev = (price - ma50) / ma50
        if abs(dev) <= 0.03:
            tech += 10
        elif 0.03 < dev <= 0.08:
            tech += 7
        elif dev > 0.08:
            tech += 3
        else:
            tech += 5

    breakdown["technical"] = tech

    # === Fundamental (40 pts) ===
    fund = 0
    f = fundamental or {}

    # PE (10 pts) - use absolute thresholds as proxy for sector comparison
    pe = f.get("pe")
    if pe and pe > 0:
        if pe < 15:
            fund += 10
        elif pe < 25:
            fund += 7
        elif pe < 40:
            fund += 4
        else:
            fund += 2

    # PB (8 pts)
    pb = f.get("pb")
    if pb and pb > 0:
        if pb < 3:
            fund += 8
        elif pb < 5:
            fund += 5
        else:
            fund += 2

    # Dividend yield (7 pts)
    dy = f.get("dividendYield") or 0
    if dy > 3:
        fund += 7
    elif dy > 2:
        fund += 5
    elif dy > 1:
        fund += 3
    else:
        fund += 1

    # 6-month momentum as growth proxy (8 pts)
    if len(closes) >= 120:
        mom = (closes[-1] - closes[-120]) / closes[-120]
    elif len(closes) >= 60:
        mom = (closes[-1] - closes[-60]) / closes[-60]
    else:
        mom = 0
    if mom > 0.15:
        fund += 8
    elif mom > 0.05:
        fund += 6
    elif mom > 0:
        fund += 4
    else:
        fund += 2

    # Market cap (7 pts)
    mc = f.get("marketCap") or 0
    if mc > 100e9:
        fund += 7
    elif mc > 10e9:
        fund += 5
    else:
        fund += 3

    breakdown["fundamental"] = fund

    # === Risk (20 pts) ===
    risk = 0

    # ATR volatility (10 pts)
    atr = calc_atr(highs, lows, closes)
    if atr and price > 0:
        atr_pct = atr / price
        if atr_pct < 0.02:
            risk += 10
        elif atr_pct < 0.04:
            risk += 7
        else:
            risk += 3

    # 52-week position (10 pts)
    h52 = chart.get("high52")
    l52 = chart.get("low52")
    if h52 and l52 and h52 > l52:
        pos = (price - l52) / (h52 - l52)
        if 0.3 <= pos <= 0.6:
            risk += 10
        elif 0.6 < pos <= 0.8:
            risk += 7
        elif pos > 0.8:
            risk += 4
        else:
            risk += 5

    breakdown["risk"] = risk

    total = tech + fund + risk
    return total, breakdown


def score_tw_stock(chart, fundamental, institutional):
    """Score a Taiwan stock out of 100 points."""
    if not chart or not chart.get("candles") or len(chart["candles"]) < 30:
        return 0, {}

    candles = chart["candles"]
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]
    price = chart["price"] or closes[-1]
    breakdown = {"technical": 0, "fundamental": 0, "institutional": 0, "risk": 0}

    # === Technical (35 pts) ===
    tech = 0

    # RSI (9 pts)
    rsi = calc_rsi(closes)
    if rsi is not None:
        if 30 <= rsi <= 50:
            tech += 9
        elif 50 < rsi <= 70:
            tech += 6
        elif rsi < 30:
            tech += 4
        else:
            tech += 2

    # MACD (9 pts)
    _, _, hist = calc_macd(closes)
    if hist is not None:
        prev_closes = closes[:-1]
        _, _, prev_hist = calc_macd(prev_closes) if len(prev_closes) >= 35 else (None, None, None)
        if hist > 0 and (prev_hist is None or hist > prev_hist):
            tech += 9
        elif hist > 0:
            tech += 5
        elif hist < 0 and (prev_hist is None or hist > prev_hist):
            tech += 3
        else:
            tech += 1

    # MA200 (9 pts)
    ma200 = calc_sma(closes, 200) if len(closes) >= 200 else calc_sma(closes, len(closes))
    if ma200 and price > 0:
        if price > ma200:
            tech += 9
        elif price > ma200 * 0.95:
            tech += 5
        else:
            tech += 2

    # MA50 deviation (8 pts)
    ma50 = calc_sma(closes, 50) if len(closes) >= 50 else calc_sma(closes, min(len(closes), 20))
    if ma50 and price > 0:
        dev = (price - ma50) / ma50
        if abs(dev) <= 0.03:
            tech += 8
        elif 0.03 < dev <= 0.08:
            tech += 6
        elif dev > 0.08:
            tech += 2
        else:
            tech += 4

    breakdown["technical"] = tech

    # === Fundamental (40 pts) ===
    fund = 0
    f = fundamental or {}

    # PE (10 pts)
    pe = f.get("pe")
    if pe and pe > 0:
        if pe < 12:
            fund += 10
        elif pe < 20:
            fund += 7
        elif pe < 30:
            fund += 4
        else:
            fund += 2

    # PB (8 pts)
    pb = f.get("pb")
    if pb and pb > 0:
        if pb < 2:
            fund += 8
        elif pb < 4:
            fund += 5
        else:
            fund += 2

    # Dividend yield (8 pts) - TW stocks tend higher
    dy = f.get("dividendYield") or 0
    if dy > 5:
        fund += 8
    elif dy > 3:
        fund += 6
    elif dy > 1:
        fund += 3
    else:
        fund += 1

    # Momentum / revenue growth proxy (7 pts)
    if len(closes) >= 120:
        mom = (closes[-1] - closes[-120]) / closes[-120]
    elif len(closes) >= 60:
        mom = (closes[-1] - closes[-60]) / closes[-60]
    else:
        mom = 0
    if mom > 0.15:
        fund += 7
    elif mom > 0.05:
        fund += 5
    elif mom > 0:
        fund += 3
    else:
        fund += 1

    # Market cap (7 pts) - use TWD thresholds
    mc = f.get("marketCap") or 0
    if mc > 1e12:  # > 1 兆
        fund += 7
    elif mc > 100e9:  # > 1000 億
        fund += 5
    else:
        fund += 3

    breakdown["fundamental"] = fund

    # === Institutional (15 pts) ===
    inst_score = 0
    inst = institutional or {}

    # Foreign net buying (10 pts)
    foreign = inst.get("foreign", 0)
    if foreign > 1000:
        inst_score += 10
    elif foreign > 0:
        inst_score += 6
    else:
        inst_score += 2

    # Trust + dealer combined (5 pts)
    trust = inst.get("trust", 0)
    dealer = inst.get("dealer", 0)
    combined = trust + dealer
    if combined > 0:
        inst_score += 5
    elif combined > -500:
        inst_score += 3
    else:
        inst_score += 1

    breakdown["institutional"] = inst_score

    # === Risk (10 pts) ===
    risk = 0

    # Volatility (5 pts)
    atr = calc_atr(highs, lows, closes)
    if atr and price > 0:
        atr_pct = atr / price
        if atr_pct < 0.02:
            risk += 5
        elif atr_pct < 0.04:
            risk += 3
        else:
            risk += 1

    # Volume vs 20-day avg (5 pts)
    if len(volumes) >= 20:
        avg_vol = sum(volumes[-20:]) / 20
        last_vol = volumes[-1]
        if avg_vol > 0 and last_vol / avg_vol > 1.2:
            risk += 5
        elif avg_vol > 0 and last_vol / avg_vol > 0.8:
            risk += 3
        else:
            risk += 1

    breakdown["risk"] = risk

    total = tech + fund + inst_score + risk
    return total, breakdown


# ---------------------------------------------------------------------------
# AI recommendation generation
# ---------------------------------------------------------------------------

def generate_recommendation(symbol, name, price, change, pct, score, breakdown, market, metrics_summary):
    """Generate AI recommendation using Anthropic API with web search."""
    if not ANTHROPIC_API_KEY:
        return f"{name}（{symbol}）綜合評分 {score} 分，技術面與基本面均呈正向訊號。"

    market_label = "台股" if market == "TW" else "美股"
    breakdown_str = "、".join(f"{k}: {v}分" for k, v in breakdown.items())

    prompt = f"""你是一位專業投資分析師。請為以下{market_label}撰寫 3-4 句簡潔的推薦原因。

股票：{symbol} {name}
股價：{price}（漲跌 {change}, {pct}%）
綜合評分：{score}/100（{breakdown_str}）
關鍵指標：{metrics_summary}

請用 web search 查詢 {symbol} 最新新聞，結合評分數據撰寫推薦原因。

規則：
- 3-4 句，精簡有力
- 提及具體數據（PE、RSI、營收成長等）
- 說明為何現在值得關注
- 中文撰寫
- 直接輸出推薦原因，不加標題或前言"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 500,
                "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 2}],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=25,
        )
        if resp.status_code == 200:
            data = resp.json()
            parts = [b["text"] for b in data.get("content", []) if b.get("type") == "text"]
            text = "\n".join(parts).strip()
            if text:
                return text
    except Exception:
        pass
    return f"{name}（{symbol}）綜合評分 {score} 分，技術面與基本面均呈正向訊號，建議關注。"


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pick_pipeline(force=False):
    """Run the full stock screening and picking pipeline."""
    now = datetime.now(TW_TZ)
    today = now.strftime("%Y-%m-%d")
    steps = []

    # Check trading days
    tw_trading = is_trading_day(today, "TW")
    us_trading = is_trading_day(today, "US")

    if not tw_trading and not us_trading and not force:
        return {"status": "skip", "date": today, "reason": "Not a trading day"}

    if force:
        tw_trading = True
        us_trading = True

    steps.append(f"Trading day check: TW={tw_trading}, US={us_trading}")

    # --- Phase A: Fetch technical data in parallel ---
    all_candidates = []
    if us_trading:
        all_candidates.extend(US_CANDIDATES)
    if tw_trading:
        all_candidates.extend(TW_CANDIDATES)

    chart_data = {}
    session = requests.Session()
    session.headers.update({"User-Agent": YAHOO_UA})

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch_chart_data, sym, session): sym for sym in all_candidates}
        for f in as_completed(futures):
            result = f.result()
            if result:
                chart_data[result["symbol"]] = result

    steps.append(f"Chart data fetched: {len(chart_data)}/{len(all_candidates)} stocks")

    # --- Phase B: Fetch fundamental data ---
    fund_session = requests.Session()
    fund_session.headers.update({"User-Agent": YAHOO_UA})
    fund_data = fetch_fundamental_batch(list(chart_data.keys()), fund_session)
    steps.append(f"Fundamental data fetched: {len(fund_data)} stocks")

    # --- Phase C: Fetch TW institutional data ---
    inst_data = {}
    if tw_trading:
        inst_data = fetch_twse_institutional()
        steps.append(f"Institutional data fetched: {len(inst_data)} stocks")

    # --- Phase D: Score all candidates ---
    us_scores = []
    tw_scores = []

    for sym in US_CANDIDATES:
        if sym not in chart_data:
            continue
        score, bd = score_us_stock(chart_data[sym], fund_data.get(sym))
        if score > 0:
            us_scores.append((sym, score, bd))

    for sym in TW_CANDIDATES:
        if sym not in chart_data:
            continue
        score, bd = score_tw_stock(chart_data[sym], fund_data.get(sym), inst_data.get(sym))
        if score > 0:
            tw_scores.append((sym, score, bd))

    us_scores.sort(key=lambda x: x[1], reverse=True)
    tw_scores.sort(key=lambda x: x[1], reverse=True)

    steps.append(f"Scored: US={len(us_scores)}, TW={len(tw_scores)}")

    # --- Phase E: Pick top stocks ---
    result = {"date": today, "generated": now.isoformat()}

    def build_pick(sym, score, bd, market):
        ch = chart_data.get(sym, {})
        fu = fund_data.get(sym, {})
        price = ch.get("price", 0)
        prev = ch.get("prev_close", 0)
        change = round(price - prev, 2) if prev else 0
        pct = round((change / prev) * 100, 2) if prev else 0

        # Build metrics summary for AI
        rsi = calc_rsi([c["close"] for c in ch.get("candles", [])])
        metrics = f"PE={fu.get('pe', 'N/A')}, RSI={rsi}, 殖利率={fu.get('dividendYield', 'N/A')}%"
        if market == "TW":
            inst = inst_data.get(sym, {})
            metrics += f", 外資淨買={inst.get('foreign', 0)}"

        reason = generate_recommendation(
            sym, ch.get("name", sym), price, change, pct, score, bd, market, metrics
        )

        return {
            "symbol": sym,
            "name": ch.get("name", sym),
            "price": price,
            "change": change,
            "pct": pct,
            "score": score,
            "breakdown": bd,
            "reason": reason,
            "metrics": {
                "pe": fu.get("pe"),
                "pb": fu.get("pb"),
                "eps": fu.get("eps"),
                "dividendYield": fu.get("dividendYield"),
                "marketCap": fu.get("marketCap"),
                "rsi": rsi,
            },
        }

    if us_scores and us_trading:
        top_us = us_scores[0]
        result["us"] = build_pick(top_us[0], top_us[1], top_us[2], "US")
        steps.append(f"US pick: {top_us[0]} (score={top_us[1]})")

    if tw_scores and tw_trading:
        top_tw = tw_scores[0]
        result["tw"] = build_pick(top_tw[0], top_tw[1], top_tw[2], "TW")
        steps.append(f"TW pick: {top_tw[0]} (score={top_tw[1]})")

    # --- Phase F: Store in KV ---
    payload = json.dumps(result, ensure_ascii=False)
    kv_command("SET", f"pick:{today}", payload, "EX", PICK_TTL)
    kv_command("SET", "pick:latest", today, "EX", LATEST_TTL)
    steps.append("Stored in KV")

    return {"status": "ok", "date": today, "steps": steps, "result": result}


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        # Auth check
        auth = self.headers.get("Authorization", "")
        if CRON_SECRET and not auth.endswith(CRON_SECRET):
            self._send_json(401, {"error": "Unauthorized"})
            return

        today = datetime.now(TW_TZ).strftime("%Y-%m-%d")
        force = params.get("force", [""])[0] == "true"

        # Check existing
        if not force:
            cached = kv_command("GET", f"pick:{today}")
            if cached:
                self._send_json(200, {
                    "status": "cached",
                    "date": today,
                    "message": f"Pick for {today} already exists",
                })
                return

        # Run pipeline
        try:
            result = run_pick_pipeline(force=force)
            self._send_json(200, result)
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def _send_json(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode("utf-8"))
