#!/usr/bin/env python3
# scripts/push.py
# 拉取实时行情 + VIX → 计算量化评分 → Scrapling深度新闻 → 期权分析(LongPort/Yahoo/TD) → AI个股推理+宏观推理 → 推送飞书
# 数据源: Twelve Data (主) + Finnhub (补缺) + Yahoo/FRED VIX + Scrapling + LongPort/Yahoo Options

import os
import sys
import json
import time
import math
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

# ─── 配置 ────────────────────────────────────────────────────────────────────

FEISHU_WEBHOOK  = os.environ.get("FEISHU_WEBHOOK_URL", "")
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
FINNHUB_KEY     = os.environ.get("FINNHUB_API_KEY", "")
PUSH_TYPE       = os.environ.get("PUSH_TYPE", "morning")  # morning/open/midday/close
SCRAPLING_MODE  = os.environ.get("SCRAPLING_MODE", "basic")  # basic / stealth

# ─── AI 配置：三API降级轮换 ──────────────────────────────────────────────────
# 优先级: gemini → gemini2 → deepseek
# 轮换: 每次调用自动切换到下一个可用API，失败则降级
AI_PROVIDERS = []
def _env(key, default=""):
    """获取环境变量，空字符串也回退到默认值"""
    val = os.environ.get(key, "")
    return val if val else default

_g1_key = os.environ.get("GEMINI_API_KEY", "")
if _g1_key:
    AI_PROVIDERS.append({
        "name": "gemini",
        "api_key": _g1_key,
        "model": _env("GEMINI_MODEL", "gemini-2.0-flash"),
    })
_g2_key = os.environ.get("GEMINI_API_KEY_2", "")
if _g2_key:
    AI_PROVIDERS.append({
        "name": "gemini2",
        "api_key": _g2_key,
        "model": _env("GEMINI_MODEL_2", "gemini-2.0-flash"),
    })
_ds_key = os.environ.get("DEEPSEEK_API_KEY", "")
if _ds_key:
    AI_PROVIDERS.append({
        "name": "deepseek",
        "api_key": _ds_key,
        "base_url": _env("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        "model": _env("DEEPSEEK_MODEL", "deepseek-chat"),
    })

# 兼容旧环境变量
if not AI_PROVIDERS:
    _fallback_key = os.environ.get("AI_API_KEY", "")
    if _fallback_key:
        AI_PROVIDERS.append({
            "name": os.environ.get("AI_PROVIDER", "deepseek"),
            "api_key": _fallback_key,
            "base_url": os.environ.get("AI_BASE_URL", "https://api.deepseek.com"),
            "model": os.environ.get("AI_MODEL", "deepseek-chat"),
        })

_ai_provider_idx = 0  # 轮换索引

BJT = timezone(timedelta(hours=8))

# ─── 美股休市日（NYSE 2025-2026 主要假日）────────────────────────────────────

NYSE_HOLIDAYS = {
    # 2025
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
    "2025-11-27", "2025-12-25",
    # 2026
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
}

def is_market_holiday():
    """检查今天（美东时间）是否为 NYSE 休市日"""
    et = timezone(timedelta(hours=-4))  # EDT
    today_et = datetime.now(et).strftime("%Y-%m-%d")
    return today_et in NYSE_HOLIDAYS

# ─── 股票池（52只，含2指数）────────────────────────────────────────────────────

UNIVERSE = [
    # NASDAQ 20
    {"ticker": "AAPL",  "name": "Apple",              "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "MSFT",  "name": "Microsoft",          "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "NVDA",  "name": "NVIDIA",             "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "AMZN",  "name": "Amazon",             "index": "NASDAQ", "sector": "Consumer"},
    {"ticker": "META",  "name": "Meta",               "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "GOOGL", "name": "Alphabet",           "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "TSLA",  "name": "Tesla",              "index": "NASDAQ", "sector": "EV/Auto"},
    {"ticker": "AVGO",  "name": "Broadcom",           "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "COST",  "name": "Costco",             "index": "NASDAQ", "sector": "Retail"},
    {"ticker": "NFLX",  "name": "Netflix",            "index": "NASDAQ", "sector": "Media"},
    {"ticker": "AMD",   "name": "AMD",                "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "QCOM",  "name": "Qualcomm",           "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "ADBE",  "name": "Adobe",              "index": "NASDAQ", "sector": "SaaS"},
    {"ticker": "ASML",  "name": "ASML",               "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "PANW",  "name": "Palo Alto",          "index": "NASDAQ", "sector": "Cyber"},
    {"ticker": "INTU",  "name": "Intuit",             "index": "NASDAQ", "sector": "SaaS"},
    {"ticker": "NOW",   "name": "ServiceNow",         "index": "NASDAQ", "sector": "SaaS"},
    {"ticker": "ISRG",  "name": "Intuitive Surgical", "index": "NASDAQ", "sector": "Health"},
    {"ticker": "BKNG",  "name": "Booking",            "index": "NASDAQ", "sector": "Consumer"},
    {"ticker": "TXN",   "name": "Texas Instruments",  "index": "NASDAQ", "sector": "Semis"},
    # S&P500 30
    {"ticker": "BRK-B", "name": "Berkshire B",        "index": "S&P500", "sector": "Finance"},
    {"ticker": "JPM",   "name": "JPMorgan",           "index": "S&P500", "sector": "Finance"},
    {"ticker": "V",     "name": "Visa",               "index": "S&P500", "sector": "Finance"},
    {"ticker": "XOM",   "name": "ExxonMobil",         "index": "S&P500", "sector": "Energy"},
    {"ticker": "UNH",   "name": "UnitedHealth",       "index": "S&P500", "sector": "Health"},
    {"ticker": "LLY",   "name": "Eli Lilly",          "index": "S&P500", "sector": "Pharma"},
    {"ticker": "JNJ",   "name": "J&J",                "index": "S&P500", "sector": "Health"},
    {"ticker": "MA",    "name": "Mastercard",         "index": "S&P500", "sector": "Finance"},
    {"ticker": "PG",    "name": "P&G",                "index": "S&P500", "sector": "Consumer"},
    {"ticker": "HD",    "name": "Home Depot",         "index": "S&P500", "sector": "Retail"},
    {"ticker": "MRK",   "name": "Merck",              "index": "S&P500", "sector": "Pharma"},
    {"ticker": "ABBV",  "name": "AbbVie",             "index": "S&P500", "sector": "Pharma"},
    {"ticker": "CVX",   "name": "Chevron",            "index": "S&P500", "sector": "Energy"},
    {"ticker": "KO",    "name": "Coca-Cola",          "index": "S&P500", "sector": "Consumer"},
    {"ticker": "WMT",   "name": "Walmart",            "index": "S&P500", "sector": "Retail"},
    {"ticker": "BAC",   "name": "Bank of America",    "index": "S&P500", "sector": "Finance"},
    {"ticker": "TMO",   "name": "Thermo Fisher",      "index": "S&P500", "sector": "Health"},
    {"ticker": "AMGN",  "name": "Amgen",              "index": "S&P500", "sector": "Pharma"},
    {"ticker": "GS",    "name": "Goldman Sachs",      "index": "S&P500", "sector": "Finance"},
    {"ticker": "CAT",   "name": "Caterpillar",        "index": "S&P500", "sector": "Industrial"},
    {"ticker": "MS",    "name": "Morgan Stanley",     "index": "S&P500", "sector": "Finance"},
    {"ticker": "RTX",   "name": "RTX Corp",           "index": "S&P500", "sector": "Defense"},
    {"ticker": "UBER",  "name": "Uber",               "index": "S&P500", "sector": "Consumer"},
    {"ticker": "GE",    "name": "GE Aerospace",       "index": "S&P500", "sector": "Industrial"},
    {"ticker": "CRM",   "name": "Salesforce",         "index": "S&P500", "sector": "SaaS"},
    {"ticker": "ACN",   "name": "Accenture",          "index": "S&P500", "sector": "Tech"},
    {"ticker": "IBM",   "name": "IBM",                "index": "S&P500", "sector": "Tech"},
    {"ticker": "DHR",   "name": "Danaher",            "index": "S&P500", "sector": "Health"},
    {"ticker": "ORCL",  "name": "Oracle",             "index": "S&P500", "sector": "SaaS"},
    {"ticker": "PEP",   "name": "PepsiCo",            "index": "S&P500", "sector": "Consumer"},
    # 指数ETF
    {"ticker": "QQQ",   "name": "Invesco QQQ (Nasdaq 100)", "index": "INDEX",  "sector": "Index"},
    {"ticker": "SPY",   "name": "SPDR S&P 500",             "index": "INDEX",  "sector": "Index"},
]

# ─── 数据获取 ─────────────────────────────────────────────────────────────────

def fetch_vix():
    """多源获取 VIX：Twelve Data → Yahoo v8 → FRED (免费兜底)
    注: Finnhub 不支持 VIX 指数，已移除
    """
    if TWELVE_DATA_KEY:
        try:
            url = f"https://api.twelvedata.com/quote?symbol=VIX:INDEXCBOE&apikey={TWELVE_DATA_KEY}"
            r = requests.get(url, timeout=10)
            data = r.json()
            if data.get("status") != "error" and data.get("close"):
                price = float(data["close"])
                prev = float(data.get("previous_close") or price)
                chg = round((price - prev) / prev * 100, 2) if prev else 0
                print(f"VIX from Twelve Data: {price}", flush=True)
                return {"price": price, "change": chg}
            print(f"VIX Twelve Data error: {data.get('message', 'unknown')[:100]}")
        except Exception as e:
            print(f"VIX Twelve Data failed: {e}")

    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?range=5d&interval=1d"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        meta = data["chart"]["result"][0]["meta"]
        price = float(meta["regularMarketPrice"])
        prev = float(meta.get("chartPreviousClose") or meta.get("previousClose") or price)
        chg = round((price - prev) / prev * 100, 2) if prev else 0
        print(f"VIX from Yahoo: {price}")
        return {"price": price, "change": chg}
    except Exception as e:
        print(f"VIX Yahoo failed: {e}")

    try:
        r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS&cosd="
                         + (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d"),
                         timeout=10)
        if r.status_code == 200:
            lines = r.text.strip().split("\n")
            if len(lines) >= 3:
                last = lines[-1].split(",")
                prev = lines[-2].split(",")
                if len(last) == 2 and last[1].strip() and len(prev) == 2 and prev[1].strip():
                    price = float(last[1].strip())
                    prev_price = float(prev[1].strip())
                    chg = round((price - prev_price) / prev_price * 100, 2)
                    print(f"VIX from FRED: {price} (date: {last[0].strip()})")
                    return {"price": price, "change": chg}
        print(f"VIX FRED: status={r.status_code}")
    except Exception as e:
        print(f"VIX FRED failed: {e}")

    print("All VIX sources failed")
    return None

def log(msg):
    print(msg, flush=True)

def fetch_quotes_twelvedata():
    """Twelve Data 分批行情（每批最多8只，遵守免费版8次/分钟限制）"""
    BATCH_SIZE = 8
    result = {}
    ticker_list = [TD_SYMBOL_MAP.get(s["ticker"], s["ticker"]) for s in UNIVERSE]
    ticker_to_stock = {TD_SYMBOL_MAP.get(s["ticker"], s["ticker"]): s for s in UNIVERSE}
    total_batches = (len(ticker_list) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(ticker_list), BATCH_SIZE):
        batch = ticker_list[i:i + BATCH_SIZE]
        sym = ",".join(batch)
        batch_num = i // BATCH_SIZE + 1
        log(f"[{batch_num}/{total_batches}] fetching: {sym}")
        url = f"https://api.twelvedata.com/quote?symbol={sym}&apikey={TWELVE_DATA_KEY}"

        # 最多重试1次
        for attempt in range(2):
            try:
                r = requests.get(url, timeout=15)
                data = r.json()
                if data.get("status") == "error":
                    log(f"  API error({attempt}): {data.get('message', '')[:100]}")
                    if attempt < 1:
                        time.sleep(8)
                        continue
                    break
                for td_key in batch:
                    stock = ticker_to_stock.get(td_key)
                    if not stock:
                        continue
                    q = data.get(td_key) or data.get(td_key.replace("/", ":"))
                    if not q or q.get("status") == "error":
                        continue
                    try:
                        result[stock["ticker"]] = {
                            "price":      float(q.get("close") or 0),
                            "change_pct": float(q.get("percent_change") or 0),
                            "high52w":    float(q.get("fifty_two_week", {}).get("high") or 0),
                            "low52w":     float(q.get("fifty_two_week", {}).get("low") or 0),
                            "pe":         float(q.get("pe") or 0) or None,
                            "volume":     int(q.get("volume") or 0),
                        }
                    except Exception:
                        continue
                got = sum(1 for t in [ticker_to_stock[t] for t in batch if t in ticker_to_stock] if t["ticker"] in result)
                log(f"  OK: {got}/{len(batch)} stocks")
                break
            except Exception as e:
                log(f"  Error({attempt}): {e}")
                if attempt < 1:
                    time.sleep(8)

        if i + BATCH_SIZE < len(ticker_list):
            time.sleep(8)  # 免费版8次/分钟，每批1次请求
    return result


# Twelve Data 特殊符号映射（指数需带交易所后缀）
TD_SYMBOL_MAP = {"BRK-B": "BRK/B"}
# Finnhub 特殊符号映射
FINNHUB_SYMBOL_MAP = {"BRK-B": "BRK.B"}

def fetch_quotes_finnhub(missing_tickers):
    """Finnhub 逐个行情（补缺），带速率限制"""
    result = {}
    for s in UNIVERSE:
        if s["ticker"] not in missing_tickers:
            continue
        symbol = FINNHUB_SYMBOL_MAP.get(s["ticker"], s["ticker"])
        try:
            url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_KEY}"
            r = requests.get(url, timeout=8)
            q = r.json()
            if q.get("c") and q["c"] > 0:
                pc = q.get("pc", q["c"])
                dp = q.get("dp")
                change_pct = round(dp, 2) if dp is not None else (round((q["c"] - pc) / pc * 100, 2) if pc else 0)
                result[s["ticker"]] = {
                    "price":      float(q["c"]),
                    "change_pct": change_pct,
                    "high52w":    0,   # Finnhub h/l 是当日数据，非52周
                    "low52w":     0,
                    "pe":         None,
                    "volume":     int(q.get("v", 0) or 0),
                }
        except Exception:
            continue
        time.sleep(1.2)  # 避免触发 60次/分钟 限制
    return result

def fetch_quotes():
    """主入口：Twelve Data → Finnhub 补缺（不覆盖已有数据）"""
    quotes = {}
    if TWELVE_DATA_KEY:
        try:
            quotes = fetch_quotes_twelvedata()
            print(f"Twelve Data: {len(quotes)} stocks")
        except Exception as e:
            print(f"Twelve Data failed: {e}")
    if FINNHUB_KEY:
        missing = [s["ticker"] for s in UNIVERSE if s["ticker"] not in quotes]
        if missing:
            print(f"Finnhub supplementing {len(missing)} missing tickers...")
            try:
                finnhub_quotes = fetch_quotes_finnhub(missing)
                for ticker, q in finnhub_quotes.items():
                    if ticker not in quotes:
                        quotes[ticker] = q
                print(f"After Finnhub: {len(quotes)} stocks total")
            except Exception as e:
                print(f"Finnhub failed: {e}")
    return quotes

# ─── 量化评分引擎 ─────────────────────────────────────────────────────────────

def get_vix_regime(vix):
    if vix < 15:  return {"label": "极度乐观", "emoji": "🟢", "mode": "进攻"}
    if vix < 20:  return {"label": "正常偏低", "emoji": "🟢", "mode": "进攻偏稳"}
    if vix < 25:  return {"label": "正常波动", "emoji": "🟡", "mode": "均衡"}
    if vix < 30:  return {"label": "警戒区间", "emoji": "🟠", "mode": "偏防守"}
    if vix < 35:  return {"label": "恐慌升温", "emoji": "🔴", "mode": "防守"}
    if vix < 45:  return {"label": "重度恐慌", "emoji": "🔴", "mode": "危机"}
    return              {"label": "极度恐慌", "emoji": "🚨", "mode": "崩溃预警"}

def get_weights(vix):
    """因子权重随 VIX 动态调整
    低 VIX: 偏重趋势位+动量（追涨）
    高 VIX: 偏重估值+稳定（防守）
    """
    if vix < 15:  return {"momentum": 0.25, "quality": 0.15, "valuation": 0.10, "stability": 0.10, "position": 0.40}
    if vix < 20:  return {"momentum": 0.25, "quality": 0.20, "valuation": 0.12, "stability": 0.13, "position": 0.30}
    if vix < 25:  return {"momentum": 0.20, "quality": 0.22, "valuation": 0.18, "stability": 0.15, "position": 0.25}
    if vix < 30:  return {"momentum": 0.12, "quality": 0.25, "valuation": 0.25, "stability": 0.20, "position": 0.18}
    if vix < 35:  return {"momentum": 0.08, "quality": 0.25, "valuation": 0.30, "stability": 0.25, "position": 0.12}
    return              {"momentum": 0.05, "quality": 0.22, "valuation": 0.35, "stability": 0.30, "position": 0.08}

def compute_score(quote, vix):
    """五因子评分：动量 / 质量 / 估值 / 稳定性 / 趋势位"""
    if not quote or not quote.get("price"):
        return None
    price    = quote["price"]
    chg      = quote["change_pct"]
    high52w  = quote["high52w"] or price * 1.2
    low52w   = quote["low52w"]  or price * 0.8
    pe       = quote["pe"]

    rng = high52w - low52w
    pos52w = ((price - low52w) / rng * 100) if rng > 0 else 50

    # 1. 动量：短期涨跌幅驱动
    momentum = min(max(50 + chg * 5, 0), 100)

    # 2. 质量：PE 合理区间（12-22）得分最优，偏离越远越差
    if pe and pe > 0:
        quality = min(max(100 - abs(pe - 17) * 3.5, 10), 95)
    else:
        quality = 50

    # 3. 估值：越接近52周低点越便宜（价值机会）
    valuation = min(max(100 - pos52w * 0.9, 5), 95)

    # 4. 稳定性：日波动越小越稳
    stability = min(max(100 - abs(chg) * 8, 10), 95)

    # 5. 趋势位：52周区间位置越高说明趋势越强（低VIX时追涨有效）
    position = min(max(pos52w, 5), 95)

    w = get_weights(vix)
    score = (momentum   * w["momentum"]   +
             quality    * w["quality"]    +
             valuation  * w["valuation"]  +
             stability  * w["stability"]  +
             position   * w["position"])
    return round(score)

def get_signal(score):
    if score > 72: return "强买 🔥"
    if score > 58: return "买入 ✅"
    if score > 42: return "中性 ➖"
    return "回避 ⚠️"

def get_option_strategy(score, vix):
    direction = "bull" if score > 65 else "neutral" if score > 45 else "bear"
    if vix < 20:
        m = {"bull": "买入 Call", "neutral": "Bull Call Spread", "bear": "买入 Put"}
    elif vix < 25:
        m = {"bull": "Bull Call Spread", "neutral": "Cash-Secured Put", "bear": "Bear Put Spread"}
    elif vix < 30:
        m = {"bull": "Cash-Secured Put", "neutral": "Iron Condor", "bear": "Bear Put Spread"}
    elif vix < 35:
        m = {"bull": "轮子策略(CSP+CC)", "neutral": "Wide Iron Condor", "bear": "买入 Put"}
    else:
        m = {"bull": "⚠️暂缓/Strangle卖出", "neutral": "⚠️空仓观望", "bear": "⚠️空仓观望"}
    return m[direction]

def get_position(score, vix):
    if vix < 20:
        if score > 70: return "重仓 8-10%"
        if score > 55: return "标配 5-7%"
        return "轻仓 2-3%"
    if vix < 25:
        if score > 70: return "标配 5-7%"
        if score > 55: return "轻仓 3-5%"
        return "观察 1-2%"
    if vix < 30:
        if score > 70: return "轻仓 3-5%"
        if score > 55: return "小仓 2-3%"
        return "规避"
    if vix < 35:
        if score > 72: return "小仓 1-3%"
        return "规避/空仓"
    return "空仓/对冲"

# ─── 新闻获取 ─────────────────────────────────────────────────────────────────

def translate_to_cn(text):
    """英文标题翻译为中文（MyMemory免费API，无需Key）"""
    try:
        url = f"https://api.mymemory.translated.net/get?q={requests.utils.quote(text)}&langpair=en|zh-CN"
        r = requests.get(url, timeout=5)
        data = r.json()
        translated = data.get("responseData", {}).get("translatedText", "")
        if translated and translated != text:
            return translated
    except Exception:
        pass
    return text  # 翻译失败返回原文

def fetch_news(vix=None):
    """多源新闻：Finnhub市场 + Google News最新头条(Business/World/Politics/Tech)
    不需要关键词，直接抓各板块最新Top新闻
    """
    all_news = []

    # 1. Finnhub 市场新闻
    if FINNHUB_KEY:
        try:
            url = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
            r = requests.get(url, timeout=8)
            items = r.json()
            for n in items[:5]:
                headline = n.get("headline", "")
                src = n.get("source", "")
                if headline:
                    cn = translate_to_cn(headline)
                    all_news.append({"headline": cn, "source": src, "category": "market"})
            print(f"Finnhub market news: {len(all_news)} items")
        except Exception as e:
            print(f"Finnhub news failed: {e}")

    # 2. Google News RSS - 各板块最新头条（英文源，翻译为中文）
    try:
        sections = {
            "business":  "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB",
            "world":     "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx1YlY4U0FtVnVHZ0pWVXlnQVAB",
            "politics":  "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGR6TVdZU0FtVnVHZ0pWVXlnQVAB",
            "technology": "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGRqTVhZU0FtVnVHZ0pWVXlnQVAB",
        }
        seen = set()
        for section, topic_id in sections.items():
            rss_url = f"https://news.google.com/rss/topics/{topic_id}?hl=en-US&gl=US&ceid=US:en"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"}
            try:
                r = requests.get(rss_url, headers=headers, timeout=8)
                if r.status_code != 200:
                    continue
                root = ET.fromstring(r.text)
                for item in root.findall(".//item")[:3]:
                    title = item.findtext("title", "").strip()
                    title_key = title[:60].lower()
                    if title_key in seen or not title:
                        continue
                    seen.add(title_key)
                    cn = translate_to_cn(title)
                    all_news.append({"headline": cn, "source": "Google News", "category": section})
            except Exception:
                continue
            time.sleep(0.3)
        print(f"Google News: {len([n for n in all_news if n['source']=='Google News'])} items from {list(sections.keys())}")
    except Exception as e:
        print(f"Google News failed: {e}")

    # 按板块分组
    market = [n for n in all_news if n["category"] == "market"][:3]
    business = [n for n in all_news if n["category"] == "business"][:3]
    world = [n for n in all_news if n["category"] in ("world", "politics")][:3]
    tech = [n for n in all_news if n["category"] == "technology"][:2]
    return market, business, world, tech

# ─── Scrapling 深度新闻抓取 ──────────────────────────────────────────────────

SCRAPLING_NEWS_SOURCES = [
    # (name, url_template, css_selector)
    # CSS 选择器参考 Scrapling 官方文档: page.css() 支持 Scrapy/Parsel 语法
    ("Yahoo Finance",  "https://finance.yahoo.com/quote/{ticker}/news/",  "h3 a::text, h3::text"),
    ("MarketWatch",    "https://www.marketwatch.com/investing/stock/{ticker}", "h3.article__headline::text"),
    ("SeekingAlpha",   "https://seekingalpha.com/symbol/{ticker}/news",   "a[data-test-id='post-list-item-title']::text"),
    ("Google News",    "https://news.google.com/search?q={ticker}+stock&hl=en-US", "h3::text, h4::text"),
    ("Reuters",        "https://www.reuters.com/search/news?query={ticker}", "h3.search-result-title::text"),
    ("Benzinga",       "https://www.benzinga.com/quote/{ticker}",          "h2::text, h3.title::text"),
    ("Investing.com",  "https://www.investing.com/equities/{ticker}-news", "article a[title]::attr(title)"),
    ("TipRanks",       "https://www.tipranks.com/stocks/{ticker}/news",    "h3::text, a.title::text"),
    ("Barrons",        "https://www.barrons.com/market-data/stocks/{ticker}", "h3::text"),
    ("CNBC",          "https://www.cnbc.com/quotes/{ticker}?tab=news",     "a.title::text"),
]

def scrapling_news(tickers, min_total=20):
    """Scrapling 深度抓取 TOP10 个股全网新闻、公告、机构评论
    去重过滤无效内容，每个 ticker 目标 ≥2 条，总计 ≥20 条
    fallback: Finnhub company-news
    
    官方文档: https://scrapling.readthedocs.io/
    - Fetcher: 快速 HTTP 请求，模拟浏览器 TLS 指纹
    - StealthyFetcher: 基于 Playwright，可绕过 Cloudflare 等反爬
    - CSS 选择器支持 ::text / ::attr() 伪元素（同 Scrapy/Parsel）
    """
    all_news = []
    seen_titles = set()

    try:
        if SCRAPLING_MODE == "stealth":
            from scrapling.fetchers import StealthyFetcher
            fetch = StealthyFetcher.fetch
            print("Scrapling mode: stealth (Playwright-based)")
        else:
            from scrapling.fetchers import Fetcher
            fetch = Fetcher.fetch
            print("Scrapling mode: basic (HTTP)")

        for ticker in tickers:
            ticker_news = []
            ticker_lower = ticker.lower().replace("-", ".")

            for src_name, url_tpl, css_sel in SCRAPLING_NEWS_SOURCES:
                if len(ticker_news) >= 4 or len(all_news) >= min_total + 10:
                    break
                try:
                    url = url_tpl.format(ticker=ticker_lower, TICKER=ticker)
                    if SCRAPLING_MODE == "stealth":
                        page = fetch(url, headless=True, network_idle=True, timeout=15)
                    else:
                        page = fetch(url, timeout=15)
                    titles = page.css(css_sel)

                    for el in titles[:3]:
                        text = el.get() if hasattr(el, 'get') else (el.text.strip() if hasattr(el, 'text') else str(el).strip())
                        if not text or len(text) < 10:
                            continue
                        key = text[:50].lower()
                        if key in seen_titles:
                            continue
                        seen_titles.add(key)
                        cn = translate_to_cn(text)
                        ticker_news.append({"headline": cn, "source": src_name, "ticker": ticker})
                    time.sleep(0.3)
                except Exception as e:
                    log(f"  Scrapling {src_name} for {ticker}: {e}")
                    continue

            all_news.extend(ticker_news)
            print(f"  Scrapling {ticker}: {len(ticker_news)} articles")

        if len(all_news) >= min_total:
            print(f"Scrapling total: {len(all_news)} articles (target met)")
            return all_news

    except ImportError:
        print("scrapling not installed, falling back to Finnhub")
    except Exception as e:
        print(f"Scrapling failed: {e}, falling back to Finnhub")

    # Fallback: Finnhub company-news
    return _finhub_stock_news(tickers)

def _finhub_stock_news(tickers, days=3):
    """Finnhub 兜底个股新闻"""
    results = []
    if not FINNHUB_KEY:
        return results
    from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for ticker in tickers:
        try:
            url = (f"https://finnhub.io/api/v1/company-news?symbol={ticker}"
                   f"&from={from_date}&to={to_date}&token={FINNHUB_KEY}")
            r = requests.get(url, timeout=8)
            items = r.json()
            for n in items[:5]:
                headline = n.get("headline", "")
                if headline:
                    cn = translate_to_cn(headline)
                    results.append({"headline": cn, "source": n.get("source", ""), "ticker": ticker})
        except Exception:
            continue
        time.sleep(1.2)
    print(f"Finnhub stock news: {len(results)} articles")
    return results

# ─── 期权深度分析（LongPort SDK）─────────────────────────────────────────────

# 长桥 LongPort OpenAPI
# pip install longport
# 文档: https://open.longportapp.com/docs/quote/pull/optionchain-date-strike
# 期权报价: https://open.longportapp.com/docs/quote/pull/option-quote
# Greeks: ctx.calc_indexes(symbols, [CalcIndex.Delta, Gamma, Theta, Vega, Rho, ImpliedVolatility])
# API 免费使用，需开通长桥账户 + 美股 LV1 行情权限（App内购买）
# 需配置环境变量: LONGPORT_APP_KEY, LONGPORT_APP_SECRET, LONGPORT_ACCESS_TOKEN
LONGPORT_APP_KEY      = os.environ.get("LONGPORT_APP_KEY", "")
LONGPORT_APP_SECRET   = os.environ.get("LONGPORT_APP_SECRET", "")
LONGPORT_ACCESS_TOKEN = os.environ.get("LONGPORT_ACCESS_TOKEN", "")

def round_strike(price, direction="otm"):
    """将行权价规整为合法档位：
    < $25: $0.5倍数 | < $200: $2.5倍数 | < $1000: $5倍数 | >= $1000: $10倍数
    """
    if price < 25:     step = 0.5
    elif price < 200:  step = 2.5
    elif price < 1000: step = 5
    else:              step = 10
    if direction == "otm":   return math.ceil(price / step) * step
    elif direction == "itm": return math.floor(price / step) * step
    else:                    return round(price / step) * step

def _longport_ctx():
    """创建 LongPort 上下文（照搬官方文档）
    Config.from_env() 自动读取 LONGPORT_APP_KEY / APP_SECRET / ACCESS_TOKEN 环境变量
    """
    if not all([LONGPORT_APP_KEY, LONGPORT_APP_SECRET, LONGPORT_ACCESS_TOKEN]):
        return None
    try:
        from longport.openapi import QuoteContext, Config
        config = Config.from_env()
        ctx = QuoteContext(config)
        return ctx
    except ImportError:
        print("longport SDK not installed, skip real option data")
        return None
    except Exception as e:
        print(f"LongPort init failed: {e}")
        return None

def _get_greeks(ctx, option_symbols):
    """通过 calc_indexes 批量获取 Greeks（官方文档方式）
    返回: dict {symbol: {delta, gamma, theta, vega, rho, iv}}
    """
    result = {}
    if not option_symbols:
        return result
    try:
        from longport.openapi import CalcIndex
        indexes = [CalcIndex.Delta, CalcIndex.Gamma, CalcIndex.Theta,
                   CalcIndex.Vega, CalcIndex.Rho, CalcIndex.ImpliedVolatility]
        # calc_indexes 限制每次请求的 symbol 数量，分批查询
        BATCH = 50
        for i in range(0, len(option_symbols), BATCH):
            batch = option_symbols[i:i + BATCH]
            resp = ctx.calc_indexes(batch, indexes)
            for item in resp:
                greeks = {}
                if hasattr(item, 'delta') and item.delta is not None:
                    greeks["delta"] = float(item.delta)
                if hasattr(item, 'gamma') and item.gamma is not None:
                    greeks["gamma"] = float(item.gamma)
                if hasattr(item, 'theta') and item.theta is not None:
                    greeks["theta"] = float(item.theta)
                if hasattr(item, 'vega') and item.vega is not None:
                    greeks["vega"] = float(item.vega)
                if hasattr(item, 'implied_volatility') and item.implied_volatility is not None:
                    greeks["iv"] = float(item.implied_volatility)
                # 用 symbol 作为 key
                sym = item.symbol if hasattr(item, 'symbol') else batch[0] if batch else ""
                if sym:
                    result[sym] = greeks
            time.sleep(0.1)
    except ImportError:
        print("  CalcIndex not available in this SDK version, skip Greeks")
    except Exception as e:
        print(f"  calc_indexes failed: {e}")
    return result

def fetch_option_chain_deep(ticker, ctx=None):
    """长桥 API 拉取完整期权链 + 实时报价 + Greeks（照搬官方文档）
    
    调用流程:
    1. option_chain_expiry_date_list(symbol) → 到期日列表
    2. option_chain_info_by_date(symbol, expiry) → StrikeInfo列表
       每个 StrikeInfo 含: strike_price, call_symbol, put_symbol
    3. option_quote([symbols]) → 期权报价 (bid/ask/volume/last_done)
       quote.option_extend 含: open_interest, direction(C/P), strike_price, expiry_date
    4. calc_indexes([symbols], [CalcIndex.Delta/Gamma/Theta/Vega/Rho/ImpliedVolatility])
       → 希腊值 (Delta, Gamma, Theta, Vega, Rho, IV)
    
    返回: list of {strike, expiry, type, bid, ask, last_done, iv, delta, gamma, theta, vega, volume, oi}
    """
    if ctx is None:
        ctx = _longport_ctx()
    if ctx is None:
        return []

    try:
        symbol = f"{ticker}.US"
        # 步骤1: 获取到期日列表
        expiry_dates = ctx.option_chain_expiry_date_list(symbol=symbol)
        if not expiry_dates:
            return []

        all_contracts = []
        # 取最近3个到期日
        for exp in expiry_dates[:3]:
            exp_str = exp.strftime("%Y-%m-%d") if hasattr(exp, 'strftime') else str(exp)
            # 步骤2: 获取行权价列表（含 call_symbol / put_symbol）
            try:
                strike_infos = ctx.option_chain_info_by_date(symbol=symbol, expiry_date=exp)
            except AttributeError:
                # 旧版 SDK 可能方法名不同
                strike_infos = ctx.option_chain_strike_list(symbol=symbol, expiry=exp)

            # 收集所有期权代码，批量查询
            option_symbols = []
            symbol_meta = {}  # symbol → {strike, type}
            for si in strike_infos[:20]:
                strike = float(si.strike_price)
                if si.call_symbol:
                    option_symbols.append(si.call_symbol)
                    symbol_meta[si.call_symbol] = {"strike": strike, "type": "call"}
                if si.put_symbol:
                    option_symbols.append(si.put_symbol)
                    symbol_meta[si.put_symbol] = {"strike": strike, "type": "put"}

            if not option_symbols:
                continue

            # 步骤3: 批量获取期权报价
            quotes_map = {}
            try:
                quotes = ctx.option_quote(option_symbols)
                for q in quotes:
                    quotes_map[q.symbol] = q
            except Exception as e:
                log(f"  LongPort option_quote failed for {ticker} expiry {exp_str}: {e}")

            # 步骤4: 批量获取 Greeks
            greeks_map = _get_greeks(ctx, option_symbols)

            # 合并报价 + Greeks
            for sym, meta in symbol_meta.items():
                q = quotes_map.get(sym)
                ext = q.option_extend if q and hasattr(q, 'option_extend') else None
                greeks = greeks_map.get(sym, {})
                all_contracts.append({
                    "strike": meta["strike"],
                    "expiry": ext.expiry_date if ext and hasattr(ext, 'expiry_date') else exp_str,
                    "type": meta["type"],
                    "bid": float(q.bid_price or 0) if q and hasattr(q, 'bid_price') and q.bid_price else 0,
                    "ask": float(q.ask_price or 0) if q and hasattr(q, 'ask_price') and q.ask_price else 0,
                    "last_done": float(q.last_done or 0) if q and hasattr(q, 'last_done') and q.last_done else 0,
                    "iv": greeks.get("iv", float(ext.implied_volatility or 0) if ext and hasattr(ext, 'implied_volatility') else 0),
                    "delta": greeks.get("delta", 0),
                    "gamma": greeks.get("gamma", 0),
                    "theta": greeks.get("theta", 0),
                    "vega": greeks.get("vega", 0),
                    "volume": int(q.volume or 0) if q and hasattr(q, 'volume') and q.volume else 0,
                    "oi": int(ext.open_interest or 0) if ext and hasattr(ext, 'open_interest') else 0,
                })

            time.sleep(0.2)

        print(f"LongPort option chain for {ticker}: {len(all_contracts)} contracts from {len(expiry_dates[:3])} expiries")
        return all_contracts

    except Exception as e:
        print(f"LongPort option chain failed for {ticker}: {e}")
        return []

def fetch_twelvedata_options(ticker):
    """Twelve Data 兜底期权数据（免费版有限）"""
    if not TWELVE_DATA_KEY:
        return []
    try:
        url = f"https://api.twelvedata.com/options/chain?symbol={ticker}&apikey={TWELVE_DATA_KEY}"
        r = requests.get(url, timeout=15)
        data = r.json()
        contracts = []
        for item in data.get("data", [])[:30]:
            try:
                contracts.append({
                    "strike": float(item.get("strike", 0)),
                    "expiry": item.get("expiration_date", ""),
                    "type": item.get("contract_type", "call").lower(),
                    "bid": float(item.get("bid", 0)),
                    "ask": float(item.get("ask", 0)),
                    "iv": float(item.get("implied_volatility", 0)),
                    "delta": float(item.get("delta", 0)),
                    "gamma": float(item.get("gamma", 0)),
                    "theta": float(item.get("theta", 0)),
                    "vega": float(item.get("vega", 0)),
                    "volume": int(item.get("volume", 0)),
                    "oi": int(item.get("open_interest", 0)),
                })
            except Exception:
                continue
        print(f"Twelve Data options for {ticker}: {len(contracts)} contracts")
        return contracts
    except Exception as e:
        print(f"Twelve Data options failed for {ticker}: {e}")
        return []


def fetch_yahoo_options(ticker):
    """Yahoo Finance 免费期权链数据（无需 API key）
    使用 Yahoo v8 API 获取完整期权链，含 bid/ask/volume/oi/iv
    数据优先级: LongPort(会员) → Yahoo(免费真实) → Twelve Data → 估算
    """
    try:
        # 1) 获取到期日列表
        url = f"https://query1.finance.yahoo.com/v7/finance/options/{ticker}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = requests.get(url, headers=headers, timeout=15)
        data = r.json()
        result = data.get("optionChain", {}).get("result", [])
        if not result:
            print(f"Yahoo options for {ticker}: no data returned")
            return []

        chain = result[0]
        expiries = chain.get("expirationDates", [])[:3]  # 取最近3个到期日
        if not expiries:
            return []

        all_contracts = []
        for exp_ts in expiries:
            # 2) 按到期日获取期权链
            url_exp = f"https://query1.finance.yahoo.com/v7/finance/options/{ticker}?date={exp_ts}"
            r2 = requests.get(url_exp, headers=headers, timeout=15)
            data2 = r2.json()
            opts = data2.get("optionChain", {}).get("result", [])
            if not opts:
                continue

            calls = opts[0].get("options", [{}])[0].get("calls", [])
            puts = opts[0].get("options", [{}])[0].get("puts", [])
            exp_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime("%Y-%m-%d")

            for otype, opt_list in [("call", calls), ("put", puts)]:
                for opt in opt_list[:20]:  # 每个方向最多20个
                    try:
                        # 过滤非标准期权（weeklies等奇怪的后缀）
                        if opt.get("contractSymbol", "").count(".") > 1:
                            continue
                        all_contracts.append({
                            "strike": float(opt.get("strike", 0)),
                            "expiry": exp_str,
                            "type": otype,
                            "bid": float(opt.get("bid", 0)),
                            "ask": float(opt.get("ask", 0)),
                            "last_done": float(opt.get("lastPrice", 0)),
                            "iv": float(opt.get("impliedVolatility", 0)) * 100 if opt.get("impliedVolatility") else 0,
                            "delta": float(opt.get("delta", 0)) if opt.get("delta") else 0,
                            "gamma": 0, "theta": 0, "vega": 0,
                            "volume": int(opt.get("volume", 0)),
                            "oi": int(opt.get("openInterest", 0)),
                        })
                    except Exception:
                        continue
            time.sleep(0.15)  # 避免限流

        print(f"Yahoo options for {ticker}: {len(all_contracts)} contracts from {len(expiries)} expiries")
        return all_contracts

    except Exception as e:
        print(f"Yahoo options failed for {ticker}: {e}")
        return []

def _detect_unusual_activity(contracts, price):
    """检测期权成交量异动（Unusual Options Activity）
    核心逻辑:
    1. volume/oi 比值 > 3 → 大量新仓（机构建仓信号）
    2. volume 远超同到期日平均 → 异常活跃
    3. 结合 strike vs price 判断方向意图
    
    返回: list of {strike, expiry, type, volume, oi, vol_oi_ratio, signal}
    """
    if not contracts:
        return []
    
    unusual = []
    # 按到期日分组计算平均成交量
    by_expiry = {}
    for c in contracts:
        exp = c.get("expiry", "")
        by_expiry.setdefault(exp, []).append(c)
    
    expiry_avg_vol = {}
    for exp, clist in by_expiry.items():
        vols = [c.get("volume", 0) for c in clist if c.get("volume", 0) > 0]
        expiry_avg_vol[exp] = sum(vols) / len(vols) if vols else 0

    for c in contracts:
        vol = c.get("volume", 0)
        oi = c.get("oi", 0)
        if vol <= 0:
            continue
        
        # 指标1: volume/OI 比值（OI=0时用1避免除零）
        vol_oi_ratio = vol / max(oi, 1)
        
        # 指标2: volume 相对该到期日平均的倍数
        exp = c.get("expiry", "")
        avg_vol = expiry_avg_vol.get(exp, 1)
        vol_vs_avg = vol / max(avg_vol, 1)
        
        # 异动判定: vol/oi > 3 OR volume > 平均5倍
        is_unusual = vol_oi_ratio > 3 or vol_vs_avg > 5
        if not is_unusual:
            continue
        
        # 判断方向意图
        strike = c.get("strike", 0)
        otype = c.get("type", "")
        if otype == "call" and strike >= price:
            intent = "看涨"    # OTM Call 大量买入
        elif otype == "call" and strike < price:
            intent = "看涨(ITM)"  # ITM Call
        elif otype == "put" and strike <= price:
            intent = "看跌"    # OTM Put 大量买入
        elif otype == "put" and strike > price:
            intent = "看跌(ITM)"
        else:
            intent = "中性"
        
        unusual.append({
            "strike": strike,
            "expiry": exp,
            "type": otype,
            "volume": vol,
            "oi": oi,
            "vol_oi_ratio": round(vol_oi_ratio, 1),
            "vol_vs_avg": round(vol_vs_avg, 1),
            "intent": intent,
        })
    
    # 按 volume 降序
    unusual.sort(key=lambda x: x["volume"], reverse=True)
    return unusual[:5]  # 最多5条


def option_analysis(ticker, price, score, vix):
    """深度期权分析：拉取期权链 → 检测异动 → 筛选流动性 → 推理策略 → 合约推荐
    综合考虑: 量化评分方向 + 期权成交量异动 + IV水平 + VIX环境
    
    返回: dict with strategy, direction, contracts, breakeven, max_loss, 
          take_profit, risk_reward, unusual_activity, signal_shift
    """
    # 1. 拉取期权链数据（优先级: LongPort → Yahoo → Twelve Data）
    data_source = "估算值"
    contracts = fetch_option_chain_deep(ticker)
    if contracts:
        data_source = "LongPort真实数据"
    else:
        contracts = fetch_yahoo_options(ticker)
        if contracts:
            data_source = "Yahoo真实数据"
        else:
            contracts = fetch_twelvedata_options(ticker)
            if contracts:
                data_source = "TwelveData"

    # 2. 检测期权成交量异动
    unusual = _detect_unusual_activity(contracts, price)

    # 3. 基础方向（量化评分）
    base_direction = "bull" if score > 65 else "bear" if score < 42 else "neutral"

    # 4. 异动修正方向：如果异动信号与评分方向一致则加强，否则标注冲突
    direction = base_direction
    signal_shift = ""
    if unusual:
        # 统计异动方向
        bull_signals = sum(1 for u in unusual if "看涨" in u["intent"])
        bear_signals = sum(1 for u in unusual if "看跌" in u["intent"])
        if bull_signals > bear_signals + 1:
            unusual_direction = "bull"
        elif bear_signals > bull_signals + 1:
            unusual_direction = "bear"
        else:
            unusual_direction = "neutral"
        
        # 评分方向 vs 异动方向冲突时，标注但不强制覆盖
        if unusual_direction != "neutral" and unusual_direction != base_direction:
            signal_shift = f"⚠️评分={base_direction}但期权异动={unusual_direction}"

    # 5. 筛选高流动性合约 (volume + oi 排序)
    if contracts:
        liquid = sorted(contracts, key=lambda c: c.get("volume", 0) + c.get("oi", 0) * 0.5, reverse=True)
        liquid = [c for c in liquid if c.get("bid", 0) > 0 and c.get("ask", 0) > 0][:10]
    else:
        liquid = []

    # 6. 综合推荐策略
    result = {
        "ticker": ticker, "direction": direction, "price": price,
        "real_data": bool(liquid), "data_source": data_source,
        "unusual_activity": unusual,
        "signal_shift": signal_shift,
    }

    if direction == "bull":
        if vix < 25:
            result["strategy"] = "买入Call / Bull Call Spread"
            call_candidates = [c for c in liquid if c["type"] == "call" and c["strike"] >= price * 0.98]
            best = call_candidates[0] if call_candidates else None
            if best:
                premium = (best["bid"] + best["ask"]) / 2 or best.get("last_done", 0)
                iv_str = f" IV={best['iv']:.0f}%" if best.get("iv") else ""
                delta_str = f" Δ={best['delta']:.2f}" if best.get("delta") else ""
                result["contracts"] = [f"买 Call ${best['strike']} exp {best['expiry']} @${premium:.2f}{iv_str}{delta_str}"]
                result["breakeven"] = round(best["strike"] + premium, 2)
                result["max_loss"] = round(premium * 100, 0)
                result["take_profit"] = f"≥${round(best['strike'] + premium * 2, 2)}"
                result["risk_reward"] = "1:2+"
            else:
                strike = round_strike(price * 1.02, "otm")
                result["contracts"] = [f"买 Call ${strike}/30-45天 (估算)"]
                result["breakeven"] = round(strike * 1.03, 2)
                result["max_loss"] = "权利金"
                result["take_profit"] = f"≥${round(strike * 1.06, 2)}"
                result["risk_reward"] = "1:2"
        else:
            result["strategy"] = "Bull Call Spread / Cash-Secured Put"
            call_liquid = [c for c in liquid if c["type"] == "call"]
            if len(call_liquid) >= 2:
                buy_c = call_liquid[0]
                sell_c = [c for c in call_liquid if c["strike"] > buy_c["strike"]][:1]
                if sell_c:
                    net_debit = ((buy_c["bid"] + buy_c["ask"]) / 2) - ((sell_c[0]["bid"] + sell_c[0]["ask"]) / 2)
                    result["contracts"] = [f"买${buy_c['strike']}C / 卖${sell_c[0]['strike']}C exp {buy_c['expiry']} 净付${net_debit:.2f}"]
                    result["breakeven"] = round(buy_c["strike"] + net_debit, 2)
                    result["max_loss"] = round(net_debit * 100, 0)
                    result["take_profit"] = f"≥${round(sell_c[0]['strike'], 2)}"
                    result["risk_reward"] = "1:1.5"
            else:
                s_buy = round_strike(price * 0.98, "itm")
                s_sell = round_strike(price * 1.05, "otm")
                result["strategy"] = "Bull Call Spread"
                result["contracts"] = [f"买${s_buy}C / 卖${s_sell}C 30-60天 (估算)"]
                result["breakeven"] = s_buy
                result["max_loss"] = "净权利金"
                result["take_profit"] = f"≥${s_sell}"
                result["risk_reward"] = "1:1.5"

    elif direction == "bear":
        if vix < 25:
            result["strategy"] = "买入Put"
            put_candidates = [c for c in liquid if c["type"] == "put" and c["strike"] <= price * 1.02]
            best = put_candidates[0] if put_candidates else None
            if best:
                premium = (best["bid"] + best["ask"]) / 2 or best.get("last_done", 0)
                iv_str = f" IV={best['iv']:.0f}%" if best.get("iv") else ""
                delta_str = f" Δ={best['delta']:.2f}" if best.get("delta") else ""
                result["contracts"] = [f"买 Put ${best['strike']} exp {best['expiry']} @${premium:.2f}{iv_str}{delta_str}"]
                result["breakeven"] = round(best["strike"] - premium, 2)
                result["max_loss"] = round(premium * 100, 0)
                result["take_profit"] = f"≤${round(best['strike'] - premium * 2, 2)}"
                result["risk_reward"] = "1:2+"
            else:
                strike = round_strike(price * 0.98, "itm")
                result["contracts"] = [f"买 Put ${strike}/30-45天 (估算)"]
                result["breakeven"] = round(strike * 0.97, 2)
                result["max_loss"] = "权利金"
                result["take_profit"] = f"≤${round(strike * 0.94, 2)}"
                result["risk_reward"] = "1:2"
        else:
            result["strategy"] = "Bear Put Spread / 卖Call"
            s_buy = round_strike(price * 0.98, "itm")
            s_sell = round_strike(price * 0.92, "otm")
            result["contracts"] = [f"买${s_buy}P / 卖${s_sell}P 30-60天 (估算)"]
            result["breakeven"] = s_buy
            result["max_loss"] = "净权利金"
            result["take_profit"] = f"≤${s_sell}"
            result["risk_reward"] = "1:1.5"
    else:
        if vix >= 25:
            result["strategy"] = "Iron Condor"
            s1 = round_strike(price * 0.94, "otm")
            s2 = round_strike(price * 0.97, "otm")
            s3 = round_strike(price * 1.03, "otm")
            s4 = round_strike(price * 1.06, "otm")
            result["contracts"] = [f"Iron Condor: 卖${s2}P/${s3}C 买${s1}P/${s4}C (估算)"]
            result["breakeven"] = f"${s2}-${s3}"
            result["max_loss"] = "行权价差-净权利金"
            result["take_profit"] = "权利金归零"
            result["risk_reward"] = "1:1"
        else:
            result["strategy"] = "Bull Call Spread"
            s_buy = round_strike(price * 0.97, "itm")
            s_sell = round_strike(price * 1.03, "otm")
            result["contracts"] = [f"买${s_buy}C / 卖${s_sell}C 30-60天 (估算)"]
            result["breakeven"] = s_buy
            result["max_loss"] = "净权利金"
            result["take_profit"] = f"≥${s_sell}"
            result["risk_reward"] = "1:1.5"

    # 附加 IV 信息
    if liquid:
        avg_iv = sum(c.get("iv", 0) for c in liquid[:5]) / max(len(liquid[:5]), 1)
        result["avg_iv"] = round(avg_iv, 1)

    return result

# ─── 兼容旧接口 ─────────────────────────────────────────────────────────────

def build_option_picks(scored_stocks, vix):
    """根据评分+VIX给出具体期权合约建议（前3只强买/买入股）
    支持短/中/长三档期限，行权价规整为合法档位
    """
    buys = [s for s in scored_stocks if s["score"] > 58][:3]
    if not buys:
        return []
    picks = []
    for s in buys:
        price = s["price"]
        ticker = s["ticker"]
        score = s["score"]
        direction = "bull" if score > 65 else "neutral"

        if direction == "bull":
            if vix < 25:
                strike_short = round_strike(price * 1.02, "otm")
                strike_mid   = round_strike(price * 1.03, "otm")
                strike_long  = round_strike(price * 1.05, "otm")
                picks.append(f"{ticker} 买入Call ${strike_short}/30天 | ${strike_mid}/60天 | ${strike_long}/90天")
            else:
                s_buy  = round_strike(price * 0.98, "itm")
                s_sell = round_strike(price * 1.05, "otm")
                picks.append(f"{ticker} Bull Call Spread 买${s_buy}/卖${s_sell} | 30-60天")
        else:
            if vix < 25:
                s_buy  = round_strike(price * 0.97, "itm")
                s_sell = round_strike(price * 1.03, "otm")
                picks.append(f"{ticker} Bull Call Spread 买${s_buy}/卖${s_sell} | 30-60天")
            else:
                s1 = round_strike(price * 0.95, "itm")
                s2 = round_strike(price * 0.97, "itm")
                picks.append(f"{ticker} Cash-Secured Put ${s1}/30天 | ${s2}/60天")

    return picks

# ─── AI 策略推理（三API降级轮换）───────────────────────────────────────────────

def ai_call(prompt, json_mode=False, max_retries=3):
    """统一 AI 调用入口：三API降级轮换
    优先级: gemini → gemini2 → deepseek
    每次调用自动轮换到下一个 provider，失败则降级
    json_mode=True 时强制要求 JSON 输出
    """
    global _ai_provider_idx
    if not AI_PROVIDERS:
        print("[AI] No provider configured, skip")
        return None

    print(f"[AI] Providers available: {[p['name'] for p in AI_PROVIDERS]}, current_idx={_ai_provider_idx}, json_mode={json_mode}")
    print(f"[AI] Prompt length: {len(prompt)} chars")

    providers_tried = []
    for attempt in range(max_retries):
        # 轮换选择 provider
        idx = (_ai_provider_idx + attempt) % len(AI_PROVIDERS)
        p = AI_PROVIDERS[idx]
        if p["name"] in providers_tried:
            continue
        providers_tried.append(p["name"])

        t0 = time.time()
        try:
            print(f"[AI] Attempt {attempt+1}: calling {p['name']} (model={p['model']}, json_mode={json_mode})...")
            result = _do_ai_call(p, prompt, json_mode)
            elapsed = time.time() - t0
            # 成功则轮换到下一个
            _ai_provider_idx = (idx + 1) % len(AI_PROVIDERS)
            print(f"[AI] ✅ {p['name']} OK ({elapsed:.1f}s, {len(result)} chars), next_idx={_ai_provider_idx}")
            return result
        except Exception as e:
            elapsed = time.time() - t0
            print(f"[AI] ❌ {p['name']} failed ({elapsed:.1f}s): {type(e).__name__}: {e}, trying next...")
            continue

    print(f"[AI] All providers failed (tried: {providers_tried})")
    return None

def _do_ai_call(provider, prompt, json_mode=False):
    """执行单个 API 调用"""
    name = provider["name"]
    key = provider["api_key"]

    if name == "deepseek":
        return _call_openai_compat(provider, prompt, json_mode)
    elif name in ("gemini", "gemini2"):
        return _call_gemini(provider, prompt, json_mode)
    else:
        raise ValueError(f"Unknown provider: {name}")

def _call_openai_compat(provider, prompt, json_mode=False):
    """OpenAI 兼容格式 API（DeepSeek 等）"""
    url = f"{provider['base_url']}/chat/completions"
    payload = {
        "model": provider["model"],
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4096,
        "temperature": 0.7,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    print(f"[AI] POST {url} model={provider['model']} json_mode={json_mode}")
    r = requests.post(url, headers={"Authorization": f"Bearer {provider['api_key']}", "Content-Type": "application/json"},
                      json=payload, timeout=90)
    print(f"[AI] Response: status={r.status_code}, len={len(r.text)}")
    r.raise_for_status()
    data = r.json()
    content = data["choices"][0]["message"]["content"].strip()
    print(f"[AI] OpenAI-compat result: {len(content)} chars, preview={content[:80]}...")
    return content

def _call_gemini(provider, prompt, json_mode=False):
    """Google Gemini API（参照官方 google-generativeai SDK 逻辑，用 REST 调用）"""
    model = provider["model"]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={provider['api_key']}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": 4096, "temperature": 0.7},
    }
    if json_mode:
        payload["generationConfig"]["responseMimeType"] = "application/json"
    print(f"[AI] POST generativelanguage.googleapis.com model={model} json_mode={json_mode}")
    r = requests.post(url, json=payload, timeout=90)
    print(f"[AI] Response: status={r.status_code}, len={len(r.text)}")
    r.raise_for_status()
    data = r.json()
    content = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    print(f"[AI] Gemini result: {len(content)} chars, preview={content[:80]}...")
    return content


def _parse_ai_json(ticker, result):
    """多级 JSON 解析：直接→代码块→正则提取 rating+action+reason"""
    import re
    # 1) 直接解析
    try:
        parsed = json.loads(result)
        print(f"[AI] {ticker} reasoning parsed OK: rating={parsed.get('rating')}")
        return parsed
    except json.JSONDecodeError:
        pass
    # 2) 提取 ```json ... ``` 代码块
    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', result, re.DOTALL)
    if m:
        try:
            parsed = json.loads(m.group(1))
            print(f"[AI] {ticker} reasoning code-block extracted OK: rating={parsed.get('rating')}")
            return parsed
        except json.JSONDecodeError:
            pass
    # 3) 正则逐字段提取（允许截断）
    parsed = {}
    m_rating = re.search(r'"rating"\s*:\s*(\d)', result)
    if m_rating:
        parsed["rating"] = int(m_rating.group(1))
    m_action = re.search(r'"action"\s*:\s*"([^"]+)"', result)
    if m_action:
        parsed["action"] = m_action.group(1)
    m_reason = re.search(r'"reason"\s*:\s*"(.+?)"', result, re.DOTALL)
    if m_reason:
        parsed["reason"] = m_reason.group(1)[:100]
    if parsed.get("rating") is not None:
        parsed.setdefault("action", "确认量化信号")
        parsed.setdefault("reason", "推理提取成功")
        print(f"[AI] {ticker} reasoning regex extracted: rating={parsed['rating']}, action={parsed['action']}")
        return parsed
    print(f"[AI] {ticker} output not valid JSON: {result[:200]}")
    return {"rating": 0, "action": "确认量化信号", "reason": "AI输出格式错误"}

VALID_ACTIONS = {"确认量化信号", "降级为观望", "反转为反向操作"}

def stock_reasoning(ticker, stock_data, news_list):
    """个股 AI 审判官：结合量化得分与实时舆情进行决策修正
    输入: ticker, 盘面数据str, 新闻列表
    输出: {"rating": 0-5, "action": str, "reason": str}
    action: '确认量化信号' / '降级为观望' / '反转为反向操作'
    """
    prompt = f"""你是一位拥有顶级投行经验的资深量化分析师和基本面研究员。当前量化系统给出了机器评分，你需要结合新闻基本面进行人工复核。

【输入数据】
- 目标股票：{ticker}
- 量化初步判断：{stock_data}
- 近期相关新闻：{news_list}

【任务与推理链】
1. 降噪提取：从新闻中剥离无效信息，提取对基本面/估值有实质影响的核心事件。
2. 预期差推演：评估新闻利好/利空是否已反映在当前股价或量化评分中。
3. 信号审判：若新闻存在致命风险（如集体诉讼、财报暴雷、重大会计质疑），你必须果断否决量化系统的高分信号。

【输出要求】
必须且只能输出一个有效的 JSON 对象，不得包含 Markdown 标记。格式如下：
{{"rating": <0到5之间的整数>, "action": "<必须是以下三个词之一：'确认量化信号' 或 '降级为观望' 或 '反转为反向操作'>", "reason": "<50-100字。若选择'降级'或'反转'，须精准说明量化系统忽略的致命风险。>"}}"""

    result = ai_call(prompt, json_mode=True)
    if not result:
        print(f"[AI] {ticker} reasoning: ai_call returned None")
        return {"rating": 0, "action": "确认量化信号", "reason": "AI调用失败"}

    parsed = _parse_ai_json(ticker, result)
    # 校验 action 合法性
    if parsed.get("action") not in VALID_ACTIONS:
        parsed["action"] = "确认量化信号"
    return parsed


def batch_stock_reasoning(scored_stocks, stock_news):
    """批量个股推理：TOP10 逐只调用 AI 新闻汇总分析
    返回: dict {ticker: {"rating": int, "reason": str}}
    """
    if not AI_PROVIDERS:
        print("[AI] No provider, skip stock reasoning")
        return {}

    # 按ticker分组新闻
    news_by_ticker = {}
    for n in stock_news:
        t = n.get("ticker", "")
        news_by_ticker.setdefault(t, []).append(n["headline"])

    results = {}
    top10 = scored_stocks[:10]
    print(f"[AI] Starting batch stock reasoning for {len(top10)} stocks...")
    for i, s in enumerate(top10):
        ticker = s["ticker"]
        # 组装盘面数据
        pe_str = f"PE={s.get('pe')}" if s.get("pe") else "PE=N/A"
        stock_data = (
            f"量化评分:{s['score']}({s['signal']}) "
            f"涨跌:{s['change_pct']:+.1f}% {pe_str} "
            f"期权策略:{s['option_strategy']} 建议仓位:{s['position']}"
        )
        # 组装新闻
        headlines = news_by_ticker.get(ticker, [])
        news_str = "\n".join([f"- {h}" for h in headlines[:5]]) if headlines else "暂无相关新闻"

        print(f"[AI] Reasoning [{i+1}/10] {ticker} (news={len(headlines)})...")
        result = stock_reasoning(ticker, stock_data, news_str)
        results[ticker] = result
        print(f"[AI] {ticker} => rating={result.get('rating','-')}, action={result.get('action','-')}, reason={result.get('reason','')[:60]}")
        time.sleep(0.5)  # 避免触发限流

    print(f"[AI] Stock reasoning done: {len(results)} stocks")
    return results


def ai_analyze(vix_data, scored_stocks, stock_news, macro_news,
               option_analyses=None, stock_reasonings=None):
    """调用 AI 进行宏观策略推理（三API降级轮换）
    整合: 量化评分 + 个股推理结果 + Scrapling新闻 + 期权链分析
    输出: 简讯、情绪判断、核心事件、风险提示、期权交易建议
    """
    if not AI_PROVIDERS:
        print("[AI] No provider, skip AI analysis")
        return None

    vix = vix_data["price"]
    regime = get_vix_regime(vix)
    top10 = scored_stocks[:10]

    # ── 构建上下文 ──
    ctx = f"## 市场环境\nVIX={vix:.1f}({regime['label']},模式:{regime['mode']}) 涨跌{vix_data['change']:+.1f}%\n\n"

    # TOP10 评分 + AI评级
    ctx += "## TOP10评分\n"
    for i, s in enumerate(top10):
        pe_str = f"PE={s.get('pe')}" if s.get("pe") else "PE=N/A"
        ai_r = stock_reasonings.get(s["ticker"], {}) if stock_reasonings else {}
        rating_str = f" AI评级:{ai_r.get('rating','-')}星" if ai_r.get("rating") else ""
        ctx += (f"{i+1}. {s['ticker']} 评分{s['score']} {s['signal']} "
                f"涨跌{s['change_pct']:+.1f}% {pe_str} 期权:{s['option_strategy']} "
                f"仓位:{s['position']}{rating_str}\n")
        if ai_r.get("reason"):
            ctx += f"   AI推理: {ai_r['reason']}\n"

    # Scrapling 深度新闻（按 ticker 分组）
    if stock_news:
        ctx += "\n## 个股深度新闻（Scrapling抓取）\n"
        by_ticker = {}
        for n in stock_news:
            t = n.get("ticker", "")
            by_ticker.setdefault(t, []).append(n["headline"])
        for ticker in [s["ticker"] for s in top10]:
            if ticker in by_ticker:
                ctx += f"【{ticker}】" + " | ".join(by_ticker[ticker][:3]) + "\n"

    # 宏观新闻
    if macro_news:
        all_h = []
        for cat in macro_news:
            for n in cat[:2]:
                all_h.append(n["headline"])
        if all_h:
            ctx += "\n## 宏观新闻\n" + "\n".join([f"- {h}" for h in all_h[:8]])

    # 期权深度分析（含异动检测）
    if option_analyses:
        ctx += "\n## 期权链深度分析\n"
        for oa in option_analyses:
            ctx += f"【{oa['ticker']}】方向:{oa['direction']} 策略:{oa.get('strategy','')}"
            if oa.get("avg_iv"):
                ctx += f" IV={oa['avg_iv']}%"
            if oa.get("contracts"):
                ctx += f" 合约:{oa['contracts'][0]}"
            if oa.get("breakeven"):
                ctx += f" 盈亏平衡:${oa['breakeven']}"
            if oa.get("max_loss"):
                ctx += f" 最大亏损:{oa['max_loss']}"
            if oa.get("risk_reward"):
                ctx += f" 风险收益比:{oa['risk_reward']}"
            if oa.get("signal_shift"):
                ctx += f" ⚠️{oa['signal_shift']}"
            unusual = oa.get("unusual_activity", [])
            if unusual:
                ctx += "\n  期权异动:"
                for u in unusual:
                    ctx += f"\n  · {u['type'].upper()} ${u['strike']} Vol={u['volume']} OI={u['oi']} V/O={u['vol_oi_ratio']}x 意图:{u['intent']}"
            ctx += "\n"

    # ── VIX 涨幅判断 ──
    vix_chg = vix_data.get("change", 0)
    vix_spike = vix_chg > 5

    # ── Prompt ──
    prompt = f"""{ctx}

你是资深美股量化+期权策略师。基于以上所有数据（行情+个股AI推理+Scrapling深度新闻+期权链），进行深度推理。

{"⚠️【🔴 宏观红线触发】VIX单日涨幅超过5%（当前+" + f"{vix_chg:.1f}" + "%），系统进入避险模式！" if vix_spike else ""}

【🔴 宏观红线铁律 (最高优先级)】
1. 情绪熔断：若 VIX 单日涨幅超过 5%，或宏观新闻涉及战争、局部冲突、重大自然灾害，【情绪判断】严禁给出"利好"，最高只能是"避险"。
2. 策略约束：在避险/防守环境下，【期权交易建议】禁止推荐纯多头买方策略（如裸买 Call）。

请输出以下结构化内容：

**1. 简讯**：用3-5句话概括今日TOP10个股的核心动态，不要罗列，要提炼因果逻辑

**2. 情绪判断**：整体市场情绪为 [利好/中性/利空/避险]，逐票给出情绪方向和关键依据（新闻事件+数据信号）。严格执行红线铁律。

**3. 核心事件**：列出3-5个最重要的事件驱动，标注影响的ticker和方向

**4. 风险提示**：当前最需警惕的2-3个风险。地缘政治风险必须置顶。

**5. 期权交易建议**：结合期权链与 VIX 环境，给出1-2个最值得执行的期权策略，说明理由、合约选择、风控要点。严禁空话，必须给出具体的风控要点。避险环境下禁止推荐裸买Call。

中文回答，简洁有力，直接给结论和依据，不要空话套话。"""

    result = ai_call(prompt)
    if result:
        print(f"[AI] Macro analysis OK ({len(result)} chars), preview: {result[:100]}...")
    else:
        print("[AI] Macro analysis FAILED - all providers returned nothing")
    return result

# ─── 飞书消息构建 ─────────────────────────────────────────────────────────────

PUSH_TITLES = {
    "morning": "🌙 美股选股·开盘前预热",
    "open":    "🔔 美股选股·已开盘",
    "midday":  "🌙 美股选股·半场复盘",
    "close":   "🏁 美股选股·收盘总结",
}

PUSH_SUBTITLES = {
    "morning": "开盘前45分钟 · 策略准备",
    "open":    "开盘17分钟 · 方向确认",
    "midday":  "半场 · 异动监控",
    "close":   "收盘 · 今日复盘 + 次日预判",
}

WEIGHT_LABELS = {
    "momentum":   "动量",
    "quality":    "质量",
    "valuation":  "估值",
    "stability":  "稳定",
    "position":   "趋势位",
}

def build_feishu_text(vix_data, scored_stocks, push_type, stock_news=None, macro_news=None, ai_summary=None, option_analyses=None, stock_reasonings=None):
    now_bjt = datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    vix     = vix_data["price"]
    vix_chg = vix_data["change"]
    regime  = get_vix_regime(vix)
    w       = get_weights(vix)

    top10   = scored_stocks[:10]
    movers  = [s for s in scored_stocks if abs(s["change_pct"]) > 3]

    lines = []
    lines.append(PUSH_TITLES.get(push_type, "📊 量化选股播报"))
    lines.append(PUSH_SUBTITLES.get(push_type, ""))
    lines.append("")

    # VIX
    vix_chg_str = f"+{vix_chg:.1f}%" if vix_chg >= 0 else f"{vix_chg:.1f}%"
    lines.append(f"{regime['emoji']} VIX 恐慌指数：{vix:.1f}（{vix_chg_str}）")
    lines.append(f"市场情绪：{regime['label']} · 策略模式：{regime['mode']}")
    w_str = " · ".join([f"{WEIGHT_LABELS[k]} {round(v*100)}%" for k, v in w.items()])
    lines.append(f"当前权重 · {w_str}")
    lines.append("")

    # TOP10
    lines.append("📊 综合评分 TOP10")
    for i, s in enumerate(top10):
        chg = s["change_pct"]
        chg_str = f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%"
        chg_icon = "📈" if chg >= 0 else "📉"
        # AI 评级与审判
        ai_r = stock_reasonings.get(s["ticker"], {}) if stock_reasonings else {}
        ai_action = ai_r.get("action", "确认量化信号")
        ai_stars = f" ⭐{ai_r.get('rating','-')}" if ai_r.get("rating") else ""
        # 根据 action 调整信号图标
        if ai_action == "反转为反向操作":
            signal_icon = "⛔"
            signal_text = "AI反转"
        elif ai_action == "降级为观望":
            signal_icon = "⚠️"
            signal_text = "AI降级"
        else:
            signal_icon = "✅" if s["signal"] == "买入" else ("➖" if s["signal"] == "中性" else "❌")
            signal_text = s["signal"]
        lines.append(
            f"  {i+1}. {s['ticker']} {signal_text} {signal_icon}  "
            f"评分{s['score']}  {chg_icon}{chg_str}{ai_stars}  "
            f"💡{s['option_strategy']}  📦{s['position']}"
        )
        if ai_r.get("reason"):
            lines.append(f"     AI: {ai_r['reason']}")
        if ai_action != "确认量化信号":
            lines.append(f"     ⚖️ AI审判: {ai_action}")
    lines.append("")

    # 异动股
    if movers:
        lines.append("🚀 今日异动（涨跌>3%）")
        for s in movers[:8]:
            chg = s["change_pct"]
            icon = "🚀" if chg > 0 else "💥"
            lines.append(
                f"  {icon} {s['ticker']} {'+' if chg>0 else ''}{chg:.1f}%  "
                f"评分{s['score']}  {s['option_strategy']}"
            )
        lines.append("")

    # 期权提示
    vix_chg = vix_data.get("change", 0)
    if vix >= 35:
        lines.append("🔴 VIX>35，极度恐慌！暂停期权买方，以卖方策略或空仓为主")
    elif vix >= 25:
        lines.append("🎯 VIX>25，IV偏高，卖方策略（CSP/Iron Condor）占优")
    else:
        lines.append("✅ VIX<25，IV偏低，买方策略（Long Call/Spread）成本合理")
    if vix_chg > 5:
        lines.append(f"🔴 宏观红线触发！VIX单日+{vix_chg:.1f}%，系统进入避险模式，禁止裸买Call")
    lines.append("")

    # 强买信号
    strong_buys = [s for s in scored_stocks if s["score"] > 72]
    if strong_buys:
        sb_list = "、".join([s["ticker"] for s in strong_buys[:8]])
        lines.append(f"🔥 强买信号（评分>72）：{sb_list}")
        lines.append("")

    # 宏观新闻
    if macro_news:
        market_news, business_news, world_news, tech_news = macro_news
    else:
        market_news, business_news, world_news, tech_news = [], [], [], []
    if market_news:
        lines.append("📰 市场要闻")
        for n in market_news:
            lines.append(f"  · [{n['source']}] {n['headline']}")
        lines.append("")
    if business_news:
        lines.append("💼 商业财经")
        for n in business_news:
            lines.append(f"  · {n['headline']}")
        lines.append("")
    if world_news:
        lines.append("🌍 国际/政经")
        for n in world_news:
            lines.append(f"  · {n['headline']}")
        lines.append("")
    if tech_news:
        lines.append("💻 科技动态")
        for n in tech_news:
            lines.append(f"  · {n['headline']}")
        lines.append("")

    # 个股深度新闻（Scrapling）
    if stock_news:
        lines.append("📋 个股深度新闻")
        by_ticker = {}
        for n in stock_news:
            t = n.get("ticker", "")
            by_ticker.setdefault(t, []).append(n)
        for ticker in [s["ticker"] for s in scored_stocks[:10]]:
            if ticker in by_ticker:
                for n in by_ticker[ticker][:3]:
                    lines.append(f"  · [{ticker}] [{n.get('source','')}] {n['headline']}")
        lines.append("")

    # 期权合约建议（基础版）
    option_picks = build_option_picks(scored_stocks, vix)
    if option_picks:
        lines.append("🎯 期权合约建议")
        for p in option_picks:
            lines.append(f"  ▸ {p}")
        lines.append("")

    # 期权深度分析（LongPort + AI）
    if option_analyses:
        lines.append("📊 期权链深度分析")
        for oa in option_analyses:
            iv_str = f" IV={oa['avg_iv']}%" if oa.get("avg_iv") else ""
            lines.append(f"  【{oa['ticker']}】{oa.get('strategy','')} | 方向:{oa['direction']}{iv_str}")
            # 异动检测
            unusual = oa.get("unusual_activity", [])
            if unusual:
                for u in unusual:
                    lines.append(f"    🔥 异动 {u['type'].upper()} ${u['strike']} exp {u['expiry']} "
                                 f"Vol={u['volume']} OI={u['oi']} V/O={u['vol_oi_ratio']}x "
                                 f"意图:{u['intent']}")
            if oa.get("signal_shift"):
                lines.append(f"    {oa['signal_shift']}")
            for c in oa.get("contracts", []):
                lines.append(f"    ▸ {c}")
            if oa.get("breakeven"):
                lines.append(f"    盈亏平衡: ${oa['breakeven']}")
            if oa.get("max_loss"):
                lines.append(f"    最大亏损: {oa['max_loss']}")
            if oa.get("take_profit"):
                lines.append(f"    止盈区间: {oa['take_profit']}")
            if oa.get("risk_reward"):
                lines.append(f"    风险收益比: {oa['risk_reward']}")
            data_src = oa.get("data_source", "LongPort真实数据" if oa.get("real_data") else "估算值")
            lines.append(f"    数据源: {data_src}")
        lines.append("")

    # AI 策略研判
    if ai_summary:
        lines.append("🤖 AI 策略研判")
        for line in ai_summary.split("\n"):
            if line.strip():
                lines.append(f"  {line.strip()}")
        lines.append("")

    lines.append(f"⏰ {now_bjt} 北京时间 · 选股播报推送消息 · 数据仅供参考")
    return "\n".join(lines)

# ─── 推送飞书 ─────────────────────────────────────────────────────────────────

def push_to_feishu(text):
    if not FEISHU_WEBHOOK:
        print("❌ FEISHU_WEBHOOK_URL not set")
        return False
    payload = {
        "msg_type": "text",
        "content": {"text": text},
    }
    r = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
    result = r.json()
    if result.get("StatusCode") == 0 or result.get("code") == 0:
        print("✅ Feishu push success")
        return True
    else:
        print(f"❌ Feishu push failed: {result}")
        return False

# ─── 主流程 ──────────────────────────────────────────────────────────────────

def main():
    push_type = PUSH_TYPE
    print(f"🚀 Starting push: {push_type}")

    # 0. 休市日检查
    if is_market_holiday():
        print("📅 Today is a NYSE holiday, skipping push")
        sys.exit(0)

    # 1. 拉 VIX
    vix_data = fetch_vix()
    if not vix_data:
        print("❌ Failed to fetch VIX, aborting")
        sys.exit(1)
    vix = vix_data["price"]
    print(f"VIX: {vix:.1f}")

    # 2. 拉行情
    quotes = fetch_quotes()
    if not quotes:
        print("❌ Failed to fetch quotes, aborting")
        sys.exit(1)
    print(f"Fetched {len(quotes)} quotes")

    # 3. 计算评分
    scored = []
    for s in UNIVERSE:
        q = quotes.get(s["ticker"])
        score = compute_score(q, vix)
        if score is None:
            continue
        scored.append({
            **s,
            "score":           score,
            "signal":          get_signal(score),
            "option_strategy": get_option_strategy(score, vix),
            "position":        get_position(score, vix),
            "price":           q["price"],
            "change_pct":      q["change_pct"],
            "pe":              q.get("pe"),
        })
    scored.sort(key=lambda x: x["score"], reverse=True)

    if not scored:
        print("❌ No stocks scored, aborting")
        sys.exit(1)
    print(f"Scored {len(scored)} stocks, top: {scored[0]['ticker']} ({scored[0]['score']})")

    # 4. Scrapling 深度抓取个股新闻（TOP10，≥20条）
    top10_tickers = [s["ticker"] for s in scored[:10]]
    stock_news = scrapling_news(top10_tickers, min_total=20)

    # 5. 拉宏观新闻
    macro_news = fetch_news(vix)

    # 6. 期权深度分析（TOP3 买入股）
    top_buys = [s for s in scored if s["score"] > 58][:3]
    print(f"[STEP6] Option analysis: {len(top_buys)} buy candidates ({[s['ticker'] for s in top_buys]})")
    option_analyses = []
    for s in top_buys:
        oa = option_analysis(s["ticker"], s["price"], s["score"], vix)
        option_analyses.append(oa)
    print(f"[STEP6] Option analyses done: {len(option_analyses)} results")

    # 7. AI 个股新闻推理（TOP10逐只）
    print(f"[STEP7] Starting AI stock reasoning...")
    stock_reasonings = batch_stock_reasoning(scored, stock_news)

    # 8. AI 宏观策略推理（整合个股推理+期权分析）
    print(f"[STEP8] Starting AI macro analysis...")
    ai_summary = ai_analyze(vix_data, scored, stock_news, macro_news, option_analyses, stock_reasonings)

    # 9. 构建消息
    print(f"[STEP9] Building Feishu message...")
    text = build_feishu_text(vix_data, scored, push_type, stock_news, macro_news, ai_summary, option_analyses, stock_reasonings)

    # 10. 推送
    print(f"[STEP10] Pushing to Feishu ({len(text)} chars)...")
    success = push_to_feishu(text)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
