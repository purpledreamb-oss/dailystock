"""
Vercel Serverless Function - Daily ETF Pick Cron
GET /api/etf_cron  -> screen ETFs, score, AI recommend, store in KV

Flow:
1. Verify CRON_SECRET
2. Check trading day
3. Fetch technical + fundamental data for 20 US + 20 TW ETF candidates
4. Score all candidates (ETF-specific: momentum, risk, size, yield, trend)
5. Pick top US + top TW
6. Generate AI recommendation text
7. Store in KV (etf_pick:{date}, etf_pick:latest)
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
PICK_TTL = 7776000  # 90 days
LATEST_TTL = 604800  # 7 days

KV_HEADERS = {}
if KV_REST_API_TOKEN:
    KV_HEADERS = {
        "Authorization": f"Bearer {KV_REST_API_TOKEN}",
        "Content-Type": "application/json",
    }

YAHOO_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# ---------------------------------------------------------------------------
# ETF Candidate Pools
# ---------------------------------------------------------------------------
US_ETF_CANDIDATES = [
    # Broad market
    "SPY", "QQQ", "VOO", "VTI", "IWM", "DIA",
    # Sector / thematic
    "SOXX", "XLF", "XLE", "XLK", "XLV", "ARKK",
    # Bonds
    "TLT", "BND", "HYG",
    # Dividend
    "SCHD", "VYM", "DVY",
    # International
    "EEM", "VEA",
]

TW_ETF_CANDIDATES = [
    # Market-cap weighted
    "0050", "006208", "0051",
    # High dividend
    "0056", "00878", "00919", "00929", "00940", "00713", "00850",
    # Tech
    "00891", "00892",
    # Bond
    "00937B", "00679B", "00751B", "00772B",
    # ESG / Thematic
    "00888", "00900", "00896", "00733",
]

# ---------------------------------------------------------------------------
# Chinese name mapping
# ---------------------------------------------------------------------------
US_ETF_NAMES = {
    "SPY": "SPDR S&P 500", "QQQ": "Invesco QQQ", "VOO": "Vanguard S&P 500",
    "VTI": "Vanguard全市場", "IWM": "iShares羅素2000", "DIA": "SPDR道瓊",
    "SOXX": "iShares半導體", "XLF": "SPDR金融", "XLE": "SPDR能源",
    "XLK": "SPDR科技", "XLV": "SPDR醫療", "ARKK": "ARK創新",
    "TLT": "iShares長期美債", "BND": "Vanguard總債券", "HYG": "iShares高收益債",
    "SCHD": "Schwab高股息", "VYM": "Vanguard高股息", "DVY": "iShares精選股息",
    "EEM": "iShares新興市場", "VEA": "Vanguard已開發",
}

TW_ETF_NAMES = {
    "0050": "元大台灣50", "006208": "富邦台50", "0051": "元大中型100",
    "0056": "元大高股息", "00878": "國泰永續高股息", "00919": "群益台灣精選高息",
    "00929": "復華台灣科技優息", "00940": "元大台灣價值高息", "00713": "元大高息低波",
    "00850": "元大臺灣ESG永續",
    "00891": "中信關鍵半導體", "00892": "富邦台灣半導體",
    "00937B": "群益ESG投等債20+", "00679B": "元大美債20年",
    "00751B": "元大AAA至A公司債", "00772B": "中信高評級公司債",
    "00888": "永豐台灣ESG", "00900": "富邦特選高股息30",
    "00896": "中信綠能及電動車", "00733": "富邦臺灣中小",
}

# ---------------------------------------------------------------------------
# Holiday lists (same as pick_cron.py)
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


def is_taiwan_etf(symbol):
    return bool(re.match(r"^\d{4,6}[A-Z]?$", symbol))


def to_yahoo_symbol(symbol):
    if is_taiwan_etf(symbol):
        return f"{symbol}.TW"
    return symbol


# ---------------------------------------------------------------------------
# Technical indicator calculations
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


def calc_max_drawdown(closes):
    """Calculate max drawdown over the given price series."""
    if len(closes) < 2:
        return 0
    peak = closes[0]
    max_dd = 0
    for c in closes:
        if c > peak:
            peak = c
        dd = (peak - c) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    return max_dd


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_chart_data(symbol, session=None):
    """Fetch 6mo daily OHLCV from Yahoo Finance."""
    yahoo_sym = to_yahoo_symbol(symbol)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?range=6mo&interval=1d"
    s = session or requests
    try:
        resp = s.get(url, headers={"User-Agent": YAHOO_UA}, timeout=8)
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


def fetch_etf_fundamental_batch(symbols, session):
    """Fetch yield, expense ratio, AUM, volume for ETFs."""
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
                f"?modules=summaryDetail,defaultKeyStatistics,price,fundProfile&crumb={crumb}"
            )
            resp = session.get(url, timeout=8)
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
            fp = modules.get("fundProfile", {})

            def rv(obj, key):
                v = obj.get(key, {})
                return v.get("raw") if isinstance(v, dict) else v

            # Yield (dividendYield or yield)
            dy = rv(sd, "yield") or rv(sd, "dividendYield")
            if dy is not None and dy < 1:
                dy = round(dy * 100, 2)  # convert decimal to percentage

            # Expense ratio
            expense = None
            fees = fp.get("feesExpensesInvestment", {})
            if fees:
                expense = rv(fees, "annualReportExpenseRatio")
            if expense is None:
                expense = rv(ks, "annualReportExpenseRatio")
            if expense is not None and expense < 1:
                expense = round(expense * 100, 2)  # convert to percentage

            # AUM / market cap
            mc = rv(pm, "marketCap") or rv(sd, "marketCap")

            # Average volume
            avg_vol = rv(sd, "averageVolume") or rv(sd, "averageDailyVolume10Day")

            return sym, {
                "dividendYield": dy,
                "expenseRatio": expense,
                "marketCap": mc,
                "averageVolume": avg_vol,
            }
        except Exception:
            return sym, None

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fetch_one, s): s for s in symbols}
        for f in as_completed(futures):
            sym, data = f.result()
            if data:
                results[sym] = data
    return results


# ---------------------------------------------------------------------------
# ETF Scoring (100 points)
# ---------------------------------------------------------------------------

def score_etf(chart, fundamental):
    """Score an ETF out of 100 points.
    Dimensions: momentum(30) + risk(25) + size(20) + yield(15) + trend(10)
    """
    if not chart or not chart.get("candles") or len(chart["candles"]) < 30:
        return 0, {}

    candles = chart["candles"]
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]
    price = chart["price"] or closes[-1]
    f = fundamental or {}

    breakdown = {"momentum": 0, "risk": 0, "size": 0, "yield": 0, "trend": 0}

    # === Momentum (30 pts) ===
    mom = 0

    # 1-month return (10 pts)
    if len(closes) >= 22:
        ret_1m = (closes[-1] - closes[-22]) / closes[-22]
    elif len(closes) >= 10:
        ret_1m = (closes[-1] - closes[-10]) / closes[-10]
    else:
        ret_1m = 0
    if ret_1m > 0.03:
        mom += 10
    elif ret_1m > 0.01:
        mom += 7
    elif ret_1m > 0:
        mom += 4
    else:
        mom += 2

    # 3-month return (10 pts)
    if len(closes) >= 66:
        ret_3m = (closes[-1] - closes[-66]) / closes[-66]
    elif len(closes) >= 44:
        ret_3m = (closes[-1] - closes[-44]) / closes[-44]
    else:
        ret_3m = 0
    if ret_3m > 0.08:
        mom += 10
    elif ret_3m > 0.03:
        mom += 7
    elif ret_3m > 0:
        mom += 4
    else:
        mom += 2

    # RSI position (10 pts)
    rsi = calc_rsi(closes)
    if rsi is not None:
        if 40 <= rsi <= 60:
            mom += 10
        elif 60 < rsi <= 70:
            mom += 7
        elif 30 <= rsi < 40:
            mom += 6
        elif rsi > 70:
            mom += 3
        else:
            mom += 4

    breakdown["momentum"] = mom

    # === Risk (25 pts) ===
    risk = 0

    # ATR volatility (10 pts)
    atr = calc_atr(highs, lows, closes)
    if atr and price > 0:
        atr_pct = atr / price
        if atr_pct < 0.015:
            risk += 10
        elif atr_pct < 0.03:
            risk += 7
        else:
            risk += 3

    # Max drawdown over 6 months (10 pts)
    max_dd = calc_max_drawdown(closes)
    if max_dd < 0.05:
        risk += 10
    elif max_dd < 0.10:
        risk += 7
    elif max_dd < 0.20:
        risk += 4
    else:
        risk += 2

    # 52-week position (5 pts)
    h52 = chart.get("high52")
    l52 = chart.get("low52")
    if h52 and l52 and h52 > l52:
        pos = (price - l52) / (h52 - l52)
        if 0.4 <= pos <= 0.7:
            risk += 5
        elif 0.7 < pos <= 0.85:
            risk += 4
        elif pos > 0.85:
            risk += 2
        else:
            risk += 3

    breakdown["risk"] = risk

    # === Size & Liquidity (20 pts) ===
    size = 0

    # Average daily volume (10 pts)
    avg_vol = f.get("averageVolume") or 0
    if avg_vol == 0 and len(volumes) >= 20:
        avg_vol = sum(volumes[-20:]) / 20
    if avg_vol > 1_000_000:
        size += 10
    elif avg_vol > 500_000:
        size += 7
    elif avg_vol > 100_000:
        size += 4
    else:
        size += 1

    # AUM / market cap (10 pts)
    mc = f.get("marketCap") or 0
    is_tw = is_taiwan_etf(chart["symbol"])
    if is_tw:
        # TWD: >500億=10, 100-500億=7, 10-100億=4
        if mc > 50e9:
            size += 10
        elif mc > 10e9:
            size += 7
        elif mc > 1e9:
            size += 4
        else:
            size += 1
    else:
        # USD: >100億=10, 10-100億=7, 1-10億=4
        if mc > 10e9:
            size += 10
        elif mc > 1e9:
            size += 7
        elif mc > 100e6:
            size += 4
        else:
            size += 1

    breakdown["size"] = size

    # === Yield (15 pts) ===
    yld = 0

    # Dividend yield (10 pts)
    dy = f.get("dividendYield") or 0
    if dy > 4:
        yld += 10
    elif dy > 3:
        yld += 8
    elif dy > 2:
        yld += 5
    elif dy > 1:
        yld += 3
    else:
        yld += 1

    # Expense ratio (5 pts) — lower is better
    er = f.get("expenseRatio")
    if er is not None:
        if er < 0.1:
            yld += 5
        elif er < 0.3:
            yld += 4
        elif er < 0.6:
            yld += 3
        else:
            yld += 1
    else:
        yld += 2  # no data, neutral

    breakdown["yield"] = yld

    # === Trend (10 pts) ===
    trend = 0

    # Price vs MA50 (5 pts)
    ma50 = calc_sma(closes, 50) if len(closes) >= 50 else calc_sma(closes, min(len(closes), 20))
    if ma50 and price > 0:
        if price > ma50:
            trend += 5
        elif price > ma50 * 0.97:
            trend += 3
        else:
            trend += 1

    # Price vs MA200 (5 pts)
    ma200 = calc_sma(closes, 200) if len(closes) >= 200 else calc_sma(closes, len(closes))
    if ma200 and price > 0:
        if price > ma200:
            trend += 5
        elif price > ma200 * 0.95:
            trend += 3
        else:
            trend += 1

    breakdown["trend"] = trend

    total = mom + risk + size + yld + trend
    return total, breakdown


# ---------------------------------------------------------------------------
# AI Recommendation
# ---------------------------------------------------------------------------

def generate_etf_recommendation(symbol, name, price, change, pct, score, breakdown, market, metrics_summary):
    """Generate AI recommendation for ETF using Anthropic API with web search."""
    if not ANTHROPIC_API_KEY:
        return f"{name}（{symbol}）綜合評分 {score} 分，動能與風險表現均佳。"

    market_label = "台股ETF" if market == "TW" else "美股ETF"
    breakdown_str = "、".join(f"{k}: {v}分" for k, v in breakdown.items())

    prompt = f"""你是一位專業 ETF 分析師。請為以下{market_label}撰寫 3-4 句簡潔的推薦原因。

ETF：{symbol} {name}
價格：{price}（漲跌 {change}, {pct}%）
綜合評分：{score}/100（{breakdown_str}）
關鍵指標：{metrics_summary}

請用 web search 查詢 {symbol} 最新相關新聞與市場趨勢，結合評分數據撰寫推薦原因。

規則：
- 3-4 句，精簡有力
- 提及具體數據（殖利率、費用率、規模、近期報酬等）
- 說明為何現在值得關注此 ETF
- 不要提及目標價或預期價格
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
            text = " ".join(parts).strip()
            text = re.sub(r'\n+', '', text)
            text = re.sub(r'\s{2,}', ' ', text)
            if text:
                return text
    except Exception:
        pass
    return f"{name}（{symbol}）綜合評分 {score} 分，動能與風險表現均佳，建議關注。"


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_etf_pipeline(force=False):
    """Run the full ETF screening and picking pipeline."""
    now = datetime.now(TW_TZ)
    today = now.strftime("%Y-%m-%d")
    steps = []

    tw_trading = is_trading_day(today, "TW")
    us_trading = is_trading_day(today, "US")

    if not tw_trading and not us_trading and not force:
        return {"status": "skip", "date": today, "reason": "Not a trading day"}

    if force:
        tw_trading = True
        us_trading = True

    steps.append(f"Trading day check: TW={tw_trading}, US={us_trading}")

    # --- Phase A: Fetch chart data in parallel ---
    all_candidates = []
    if us_trading:
        all_candidates.extend(US_ETF_CANDIDATES)
    if tw_trading:
        all_candidates.extend(TW_ETF_CANDIDATES)
    all_candidates = list(dict.fromkeys(all_candidates))

    chart_data = {}
    session = requests.Session()
    session.headers.update({"User-Agent": YAHOO_UA})

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fetch_chart_data, sym, session): sym for sym in all_candidates}
        for f in as_completed(futures):
            result = f.result()
            if result:
                chart_data[result["symbol"]] = result

    steps.append(f"Chart data fetched: {len(chart_data)}/{len(all_candidates)} ETFs")

    # --- Phase B: Fetch ETF fundamental data ---
    fund_session = requests.Session()
    fund_session.headers.update({"User-Agent": YAHOO_UA})
    fund_data = fetch_etf_fundamental_batch(list(chart_data.keys()), fund_session)

    steps.append(f"Fundamental data fetched: {len(fund_data)} ETFs")

    # --- Phase C: Score all ETFs ---
    us_scores = []
    tw_scores = []

    for sym in US_ETF_CANDIDATES:
        if sym not in chart_data:
            continue
        score, bd = score_etf(chart_data[sym], fund_data.get(sym))
        if score > 0:
            us_scores.append((sym, score, bd))

    for sym in TW_ETF_CANDIDATES:
        if sym not in chart_data:
            continue
        score, bd = score_etf(chart_data[sym], fund_data.get(sym))
        if score > 0:
            tw_scores.append((sym, score, bd))

    us_scores.sort(key=lambda x: x[1], reverse=True)
    tw_scores.sort(key=lambda x: x[1], reverse=True)

    steps.append(f"Scored: US={len(us_scores)}, TW={len(tw_scores)}")

    # --- Phase D: Pick top ETFs and generate recommendations ---
    result = {"date": today, "generated": now.isoformat()}

    def build_etf_pick(sym, score, bd, market):
        ch = chart_data.get(sym, {})
        fu = fund_data.get(sym, {})
        price = ch.get("price", 0)
        candles = ch.get("candles", [])
        prev = candles[-2]["close"] if len(candles) >= 2 else ch.get("prev_close", 0)
        change = round(price - prev, 2) if prev else 0
        pct = round((change / prev) * 100, 2) if prev else 0

        # Metrics summary for AI
        rsi = calc_rsi([c["close"] for c in candles])
        dy = fu.get("dividendYield") or 0
        er = fu.get("expenseRatio")
        mc = fu.get("marketCap") or 0

        metrics = f"殖利率={dy}%, 費用率={er}%"
        if mc > 0:
            if is_taiwan_etf(sym):
                metrics += f", 規模={round(mc / 1e8)}億TWD"
            else:
                metrics += f", AUM={round(mc / 1e9, 1)}B USD"
        metrics += f", RSI={rsi}"

        # 1-month return
        closes = [c["close"] for c in candles]
        if len(closes) >= 22:
            ret_1m = round((closes[-1] - closes[-22]) / closes[-22] * 100, 2)
        else:
            ret_1m = 0
        metrics += f", 近1月報酬={ret_1m}%"

        reason = generate_etf_recommendation(
            sym, ch.get("name", sym), price, change, pct, score, bd, market, metrics
        )

        name_map = TW_ETF_NAMES if market == "TW" else US_ETF_NAMES
        return {
            "symbol": sym,
            "name": ch.get("name", sym),
            "name_zh": name_map.get(sym, ""),
            "price": price,
            "pick_price": price,
            "change": change,
            "pct": pct,
            "score": score,
            "breakdown": bd,
            "reason": reason,
            "metrics": {
                "dividendYield": dy,
                "expenseRatio": er,
                "marketCap": mc,
                "rsi": rsi,
                "return_1m": ret_1m,
            },
        }

    if us_scores and us_trading:
        top_us = us_scores[0]
        result["us"] = build_etf_pick(top_us[0], top_us[1], top_us[2], "US")
        steps.append(f"US ETF pick: {top_us[0]} (score={top_us[1]})")

    if tw_scores and tw_trading:
        top_tw = tw_scores[0]
        result["tw"] = build_etf_pick(top_tw[0], top_tw[1], top_tw[2], "TW")
        steps.append(f"TW ETF pick: {top_tw[0]} (score={top_tw[1]})")

    # --- Phase E: Store in KV ---
    payload = json.dumps(result, ensure_ascii=False)
    kv_command("SET", f"etf_pick:{today}", payload, "EX", PICK_TTL)
    kv_command("SET", "etf_pick:latest", today, "EX", LATEST_TTL)

    # Maintain etf_pick:dates list
    try:
        dates_raw = kv_command("GET", "etf_pick:dates")
        dates_list = json.loads(dates_raw) if dates_raw else []
    except Exception:
        dates_list = []
    if today not in dates_list:
        dates_list.append(today)
        dates_list = sorted(dates_list)[-90:]
        kv_command("SET", "etf_pick:dates", json.dumps(dates_list))
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
            cached = kv_command("GET", f"etf_pick:{today}")
            if cached:
                self._send_json(200, {
                    "status": "cached",
                    "date": today,
                    "message": f"ETF pick for {today} already exists",
                })
                return

        # Run pipeline
        try:
            result = run_etf_pipeline(force=force)
            self._send_json(200, result)
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def _send_json(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode("utf-8"))
