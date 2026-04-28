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
import concurrent.futures
import threading
from datetime import datetime, timezone, timedelta

# ─── 配置 ────────────────────────────────────────────────────────────────────

FEISHU_WEBHOOK  = os.environ.get("FEISHU_WEBHOOK_URL", "")
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
FINNHUB_KEY     = os.environ.get("FINNHUB_API_KEY", "")
PUSH_TYPE       = os.environ.get("PUSH_TYPE", "morning")  # morning/close
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

# ─── 股票池（30只，含2指数ETF）─────────────────────────────────────────────────
# 七姐妹(Magnificent 7): AAPL, MSFT, NVDA, AMZN, META, GOOGL, TSLA 必选

UNIVERSE = [
    # ★ 七姐妹 Magnificent 7（必选）
    {"ticker": "AAPL",  "name": "Apple",              "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "MSFT",  "name": "Microsoft",          "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "NVDA",  "name": "NVIDIA",             "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "AMZN",  "name": "Amazon",             "index": "NASDAQ", "sector": "Consumer"},
    {"ticker": "META",  "name": "Meta",               "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "GOOGL", "name": "Alphabet",           "index": "NASDAQ", "sector": "Tech"},
    {"ticker": "TSLA",  "name": "Tesla",              "index": "NASDAQ", "sector": "EV/Auto"},
    # 半导体
    {"ticker": "AVGO",  "name": "Broadcom",           "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "AMD",   "name": "AMD",                "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "ASML",  "name": "ASML",               "index": "NASDAQ", "sector": "Semis"},
    {"ticker": "TXN",   "name": "Texas Instruments",  "index": "NASDAQ", "sector": "Semis"},
    # 科技/SaaS
    {"ticker": "NFLX",  "name": "Netflix",            "index": "NASDAQ", "sector": "Media"},
    {"ticker": "ADBE",  "name": "Adobe",              "index": "NASDAQ", "sector": "SaaS"},
    {"ticker": "CRM",   "name": "Salesforce",         "index": "S&P500", "sector": "SaaS"},
    {"ticker": "ORCL",  "name": "Oracle",             "index": "S&P500", "sector": "SaaS"},
    # 金融
    {"ticker": "JPM",   "name": "JPMorgan",           "index": "S&P500", "sector": "Finance"},
    {"ticker": "BRK-B", "name": "Berkshire B",        "index": "S&P500", "sector": "Finance"},
    {"ticker": "V",     "name": "Visa",               "index": "S&P500", "sector": "Finance"},
    # 医药
    {"ticker": "LLY",   "name": "Eli Lilly",          "index": "S&P500", "sector": "Pharma"},
    {"ticker": "UNH",   "name": "UnitedHealth",       "index": "S&P500", "sector": "Health"},
    # 消费/零售
    {"ticker": "COST",  "name": "Costco",             "index": "NASDAQ", "sector": "Retail"},
    {"ticker": "WMT",   "name": "Walmart",            "index": "S&P500", "sector": "Retail"},
    {"ticker": "PEP",   "name": "PepsiCo",            "index": "S&P500", "sector": "Consumer"},
    # 能源
    {"ticker": "XOM",   "name": "ExxonMobil",         "index": "S&P500", "sector": "Energy"},
    {"ticker": "CVX",   "name": "Chevron",            "index": "S&P500", "sector": "Energy"},
    # 工业/国防
    {"ticker": "CAT",   "name": "Caterpillar",        "index": "S&P500", "sector": "Industrial"},
    {"ticker": "RTX",   "name": "RTX Corp",           "index": "S&P500", "sector": "Defense"},
    # 其他
    {"ticker": "BKNG",  "name": "Booking",            "index": "NASDAQ", "sector": "Consumer"},
    {"ticker": "UBER",  "name": "Uber",               "index": "S&P500", "sector": "Consumer"},
    {"ticker": "HD",    "name": "Home Depot",          "index": "S&P500", "sector": "Retail"},
    # 指数ETF
    {"ticker": "QQQ",   "name": "Invesco QQQ (Nasdaq 100)", "index": "INDEX",  "sector": "Index"},
    {"ticker": "SPY",   "name": "SPDR S&P 500",             "index": "INDEX",  "sector": "Index"},
]

# ─── 数据获取 ─────────────────────────────────────────────────────────────────


# ─── Twitter 抓取 (RapidAPI) ──────────────────────────────────────────────────

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")

def fetch_twitter_timeline(username="joely7758521"):
    """
    从 RapidAPI 获取用户的最新推文。
    你需要在 RapidAPI 订阅一个 Twitter/X API 服务（如 Twitter API45 或类似的 Scraper），
    并在环境变量中设置 RAPIDAPI_KEY。
    """
    if not RAPIDAPI_KEY:
        print("⚠️ RAPIDAPI_KEY 未设置，跳过 Twitter 抓取。")
        return []

    url = "https://twitter-api45.p.rapidapi.com/timeline.php"
    querystring = {"screenname": username}
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "twitter-api45.p.rapidapi.com"
    }

    print(f"🐦 Fetching Twitter timeline for @{username}...")
    try:
        import requests
        r = requests.get(url, headers=headers, params=querystring, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Failed to fetch Twitter: {e}")
        return []

    # 假设 API 返回 { "timeline": [ { "id": "...", "text": "...", "media": [...] } ] }
    # 不同的 RapidAPI 提供商数据结构可能不同，请根据实际情况微调
    tweets = data.get("timeline", [])
    if not tweets and isinstance(data, list):
        tweets = data
        
    last_id_file = f".{username}_last_tweet_id"
    last_id = ""
    import os
    if os.path.exists(last_id_file):
        with open(last_id_file, "r") as f:
            last_id = f.read().strip()

    new_tweets = []
    for t in tweets:
        tid = str(t.get("id", t.get("tweet_id", "")))
        if not tid:
            continue
        if tid == last_id:
            break  # 假设推文是按时间倒序排列的
        text = t.get("text", "")
        # 尝试提取图片 URL
        media = t.get("media", [])
        image_urls = []
        if isinstance(media, list):
            for m in media:
                if isinstance(m, dict) and m.get("type") == "photo":
                    image_urls.append(m.get("media_url_https", m.get("url", "")))
                elif isinstance(m, str) and (m.endswith(".jpg") or m.endswith(".png")):
                     image_urls.append(m)

        new_tweets.append({
            "id": tid,
            "text": text,
            "images": [url for url in image_urls if url]
        })

    if new_tweets:
        # 更新 last_id
        with open(last_id_file, "w") as f:
            f.write(new_tweets[0]["id"])
        print(f"✅ Found {len(new_tweets)} new tweets from @{username}")
    else:
        print(f"ℹ️ No new tweets from @{username} since {last_id}")

    return new_tweets


def fetch_vix():
    """多源获取 VIX：Twelve Data → Yahoo v8 → FRED (免费兜底)
    注: Finnhub 不支持 VIX 指数，已移除
    返回: {"price", "change", "source", "as_of"}
    """
    et = timezone(timedelta(hours=-4))
    now_et = datetime.now(et)

    if TWELVE_DATA_KEY:
        try:
            url = f"https://api.twelvedata.com/quote?symbol=VIX:INDEXCBOE&apikey={TWELVE_DATA_KEY}"
            r = requests.get(url, timeout=10)
            data = r.json()
            if data.get("status") != "error" and data.get("close"):
                price = float(data["close"])
                # 优先用官方涨跌幅，避免自算时 previous_close 跨日不准
                if data.get("change_percent"):
                    chg = round(float(str(data["change_percent"]).replace("%", "")), 2)
                else:
                    prev = float(data.get("previous_close") or price)
                    chg = round((price - prev) / prev * 100, 2) if prev else 0
                print(f"VIX from Twelve Data: {price} (chg={chg}%)", flush=True)
                return {"price": price, "change": chg, "source": "Twelve Data", "as_of": now_et.strftime("%m-%d %H:%M ET")}
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
        # 优先用官方涨跌幅，其次用 previousClose 自算，避免 chartPreviousClose 是5天前数据
        if meta.get("regularMarketChangePercent"):
            chg = round(float(meta["regularMarketChangePercent"]), 2)
        else:
            prev = float(meta.get("previousClose") or meta.get("chartPreviousClose") or price)
            chg = round((price - prev) / prev * 100, 2) if prev else 0
        print(f"VIX from Yahoo: {price} (chg={chg}%)")
        return {"price": price, "change": chg, "source": "Yahoo", "as_of": now_et.strftime("%m-%d %H:%M ET")}
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
                    return {"price": price, "change": chg, "source": "FRED", "as_of": last[0].strip()}
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
                            "price":         float(q.get("close") or 0),
                            "change_pct":    float(q.get("percent_change") or 0),
                            "high52w":       float(q.get("fifty_two_week", {}).get("high") or 0),
                            "low52w":        float(q.get("fifty_two_week", {}).get("low") or 0),
                            "pe":            float(q.get("pe") or 0) or None,
                            "volume":        int(q.get("volume") or 0),
                            "avg_volume":    int(q.get("average_volume") or 0),
                            "day_high":      float(q.get("high") or 0),
                            "day_low":       float(q.get("low") or 0),
                            "day_open":      float(q.get("open") or 0),
                            "fifty_day_avg": float(q.get("fifty_day_average") or 0),
                            "two_hundred_avg": float(q.get("two_hundred_day_average") or 0),
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

def fetch_quotes_yahoo(missing_tickers):
    """Yahoo Finance 逐个行情（补缺兜底），无需 API Key"""
    result = {}
    for ticker in missing_tickers:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1d"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            r = requests.get(url, headers=headers, timeout=8)
            data = r.json()
            if data.get("chart") and data["chart"].get("result") and len(data["chart"]["result"]) > 0:
                meta = data["chart"]["result"][0]["meta"]
                price = float(meta["regularMarketPrice"])
                if meta.get("regularMarketChangePercent"):
                    change_pct = round(float(meta["regularMarketChangePercent"]), 2)
                else:
                    prev = float(meta.get("previousClose") or meta.get("chartPreviousClose") or price)
                    change_pct = round((price - prev) / prev * 100, 2) if prev else 0

                result[ticker] = {
                    "price": price,
                    "change_pct": change_pct,
                    "high52w": meta.get("fiftyTwoWeekHigh", price * 1.2),
                    "low52w": meta.get("fiftyTwoWeekLow", price * 0.8),
                    "pe": None,
                    "volume": meta.get("regularMarketVolume", 0),
                    "avg_volume": 0,
                    "day_high": meta.get("regularMarketDayHigh", price),
                    "day_low": meta.get("regularMarketDayLow", price),
                    "day_open": meta.get("regularMarketOpen", price),
                    "fifty_day_avg": 0,
                    "two_hundred_avg": 0,
                }
        except Exception:
            continue
        time.sleep(0.5)  # 防止请求过快被 Yahoo 限流
    return result

def fetch_quotes():
    """主入口：Twelve Data → Finnhub 补缺 → Yahoo 兜底（不覆盖已有数据）"""
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

    missing = [s["ticker"] for s in UNIVERSE if s["ticker"] not in quotes]
    if missing:
        print(f"Yahoo supplementing {len(missing)} missing tickers...")
        try:
            yahoo_quotes = fetch_quotes_yahoo(missing)
            for ticker, q in yahoo_quotes.items():
                if ticker not in quotes:
                    quotes[ticker] = q
            print(f"After Yahoo: {len(quotes)} stocks total")
        except Exception as e:
            print(f"Yahoo failed: {e}")

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
    """因子权重随 VIX 动态调整（混合型：趋势确认+过热惩罚+回归加分）
    低 VIX: 偏重趋势+回踩机会
    高 VIX: 偏重估值+安全边际
    """
    if vix < 15:  return {"momentum": 0.18, "quality": 0.12, "valuation": 0.20, "stability": 0.10, "position": 0.22, "pullback": 0.18}
    if vix < 20:  return {"momentum": 0.18, "quality": 0.15, "valuation": 0.20, "stability": 0.12, "position": 0.20, "pullback": 0.15}
    if vix < 25:  return {"momentum": 0.15, "quality": 0.18, "valuation": 0.22, "stability": 0.15, "position": 0.15, "pullback": 0.15}
    if vix < 30:  return {"momentum": 0.08, "quality": 0.20, "valuation": 0.28, "stability": 0.20, "position": 0.10, "pullback": 0.14}
    if vix < 35:  return {"momentum": 0.05, "quality": 0.18, "valuation": 0.32, "stability": 0.25, "position": 0.08, "pullback": 0.12}
    return              {"momentum": 0.03, "quality": 0.15, "valuation": 0.37, "stability": 0.28, "position": 0.05, "pullback": 0.12}

def _momentum_hybrid(chg):
    """混合型动量：温和上涨(3-5%)最优，暴涨(>8%)惩罚，下跌轻罚"""
    if chg >= 0:
        # 0~3%: 线性加分, 3~5%: 最优区间, 5~8%: 缓降, >8%: 惩罚
        if chg <= 3:
            return 50 + chg * 8
        elif chg <= 5:
            return 74 + (chg - 3) * 3   # 74~80
        elif chg <= 8:
            return 80 - (chg - 5) * 6   # 80~62
        else:
            return max(62 - (chg - 8) * 8, 15)  # 暴涨惩罚
    else:
        # 小跌(-1~-3%)轻微加分(回踩机会)，大跌(>-5%)扣分
        if chg >= -1:
            return 50 + chg * 2
        elif chg >= -3:
            return 48 + (chg + 1) * 4   # 48~40
        elif chg >= -5:
            return 40 + (chg + 3) * 5   # 40~30
        else:
            return max(30 + (chg + 5) * 4, 5)

def _position_hybrid(pos52w):
    """混合型趋势位：40-70%区间最优(有趋势但不过热)，>80%惩罚"""
    if pos52w <= 20:
        return 30 + pos52w * 0.5  # 30~40 低位回升
    elif pos52w <= 40:
        return 40 + (pos52w - 20) * 1.5  # 40~70 上升趋势确认
    elif pos52w <= 70:
        return 70 + (pos52w - 40) * 0.33  # 70~80 最优区间
    elif pos52w <= 85:
        return 80 - (pos52w - 70) * 1.5  # 80~57.5 过热预警
    else:
        return max(57.5 - (pos52w - 85) * 3, 15)  # 高位惩罚

def _pullback_score(chg, pos52w=50, day_high=0, price=0, fifty_day_avg=0, volume=0, avg_volume=0):
    """回踩因子（三维锁定）：趋势向上 + 短期回调 + 量价配合
    ① 趋势过滤：52周位置>30% 且 价格>MA50 → 确认上升趋势
    ② 回调检测：日跌1-5% 且 当日从高点回落 → 健康回调非暴跌
    ③ 量价配合：成交量>均量70% → 有市场关注度
    只有三维同时满足才给高分，否则大幅打折
    """
    # 基础分：日跌幅区间
    if -5 <= chg <= -1:
        base = 60 + (-chg - 1) * 8  # 60~92
    elif -1 < chg < 0:
        base = 55
    elif 0 <= chg <= 2:
        base = 50
    else:
        base = max(50 - (abs(chg) - 2) * 5, 10)

    # 三维锁定检查
    trend_ok = pos52w > 30 and (fifty_day_avg <= 0 or price >= fifty_day_avg * 0.97)
    pullback_ok = -5 <= chg <= -1 and day_high > 0 and price < day_high * 0.99
    volume_ok = avg_volume <= 0 or volume >= avg_volume * 0.7

    locks = sum([trend_ok, pullback_ok, volume_ok])
    if locks == 3:
        return min(base, 95)       # 三维全满：满分
    elif locks == 2:
        return min(int(base * 0.7), 75)  # 两维：打7折
    elif locks == 1:
        return min(int(base * 0.4), 50)  # 一维：打4折
    else:
        return min(int(base * 0.2), 30)  # 无维度：严重打折

def compute_score(quote, vix):
    """六因子混合评分：动量(过热惩罚) / 质量 / 估值(加权重) / 稳定性 / 趋势位(过热惩罚) / 回踩加分"""
    if not quote or not quote.get("price"):
        return None
    price    = quote["price"]
    chg      = quote["change_pct"]
    high52w  = quote["high52w"] or price * 1.2
    low52w   = quote["low52w"]  or price * 0.8
    pe       = quote["pe"]

    rng = high52w - low52w
    pos52w = ((price - low52w) / rng * 100) if rng > 0 else 50

    # 1. 动量（混合型：温和最优，暴涨惩罚）
    momentum = min(max(_momentum_hybrid(chg), 0), 100)

    # 2. 质量：PE 合理区间（12-22）得分最优，偏离越远越差
    if pe and pe > 0:
        quality = min(max(100 - abs(pe - 17) * 3.5, 10), 95)
    else:
        quality = 50

    # 3. 估值：越接近52周低点越便宜（价值机会）
    valuation = min(max(100 - pos52w * 0.9, 5), 95)

    # 4. 稳定性：日波动越小越稳
    stability = min(max(100 - abs(chg) * 8, 10), 95)

    # 5. 趋势位（混合型：中位最优，高位惩罚）
    position = min(max(_position_hybrid(pos52w), 5), 95)

    # 6. 回踩因子（三维锁定：趋势+回调+量价）
    pullback = min(max(_pullback_score(
        chg, pos52w,
        quote.get("day_high", 0), price,
        quote.get("fifty_day_avg", 0),
        quote.get("volume", 0), quote.get("avg_volume", 0)
    ), 5), 95)

    w = get_weights(vix)
    score = (momentum   * w["momentum"]   +
             quality    * w["quality"]    +
             valuation  * w["valuation"]  +
             stability  * w["stability"]  +
             position   * w["position"]   +
             pullback   * w["pullback"])
    return round(score)

def compute_score_reversal(quote, vix):
    """均值回归评分：专门选超卖/低位票，等反弹
    核心逻辑：越跌越多分越高，52周低位加分，估值便宜加分
    """
    if not quote or not quote.get("price"):
        return None
    price    = quote["price"]
    chg      = quote["change_pct"]
    high52w  = quote["high52w"] or price * 1.2
    low52w   = quote["low52w"]  or price * 0.8
    pe       = quote["pe"]

    rng = high52w - low52w
    pos52w = ((price - low52w) / rng * 100) if rng > 0 else 50

    # 1. 动量（反转）：跌越多分越高
    if chg < -3:
        momentum = min(95, 70 + abs(chg) * 5)
    elif chg < -1:
        momentum = 60 + abs(chg) * 5
    elif chg < 0:
        momentum = 55
    else:
        momentum = max(50 - chg * 4, 20)  # 涨了扣分

    # 2. 质量：PE偏低反而加分（价值陷阱检测）
    if pe and pe > 0:
        if pe < 15:
            quality = 85
        elif pe < 25:
            quality = 70
        elif pe < 40:
            quality = 50
        else:
            quality = 30
    else:
        quality = 50

    # 3. 估值（重权重）：越低越便宜=越好
    valuation = min(max(100 - pos52w * 1.1, 10), 95)

    # 4. 稳定性（反转）：波动大反而是机会
    stability = 50  # 均值回归不关心稳定性

    # 5. 趋势位：低位=安全边际高
    position = min(max(100 - pos52w, 10), 95)

    # 6. 回踩因子：跌得多加分多（均值回归也用三维锁定，但趋势条件放宽）
    pullback = min(max(_pullback_score(
        chg, pos52w,
        quote.get("day_high", 0), price,
        quote.get("fifty_day_avg", 0),
        quote.get("volume", 0), quote.get("avg_volume", 0)
    ), 5), 95)

    # 均值回归权重：估值+回踩+低位主导
    w = {"momentum": 0.12, "quality": 0.10, "valuation": 0.30, "stability": 0.05, "position": 0.25, "pullback": 0.18}
    score = (momentum   * w["momentum"]   +
             quality    * w["quality"]    +
             valuation  * w["valuation"]  +
             stability  * w["stability"]  +
             position   * w["position"]   +
             pullback   * w["pullback"])
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
        seen_lock = threading.Lock()

        def fetch_google_news_section(item):
            section, topic_id = item
            rss_url = f"https://news.google.com/rss/topics/{topic_id}?hl=en-US&gl=US&ceid=US:en"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"}
            local_news = []
            try:
                r = requests.get(rss_url, headers=headers, timeout=8)
                if r.status_code == 200:
                    root = ET.fromstring(r.text)
                    for item_node in root.findall(".//item")[:3]:
                        title = item_node.findtext("title", "").strip()
                        title_key = title[:60].lower()
                        if not title:
                            continue

                        is_new = False
                        with seen_lock:
                            if title_key not in seen:
                                seen.add(title_key)
                                is_new = True

                        if is_new:
                            cn = translate_to_cn(title)
                            local_news.append({"headline": cn, "source": "Google News", "category": section})
            except Exception:
                pass
            return local_news

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(sections)) as executor:
            for result in executor.map(fetch_google_news_section, sections.items()):
                all_news.extend(result)
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
    ("Finviz",         "https://finviz.com/quote.ashx?t={ticker}",       "table.fullview-news-outer tr td a.tab-link-news::text"),
    ("Google News",    "https://news.google.com/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en", "a.JtKRv::text"),
    ("MarketWatch",    "https://www.marketwatch.com/investing/stock/{ticker}", "h3.article__headline a::text"),
    ("SeekingAlpha",   "https://seekingalpha.com/symbol/{ticker}/news",   "a[data-test-id='post-list-item-title']::text"),
    ("Benzinga",       "https://www.benzinga.com/quote/{ticker}",          "h3.title::text"),
    ("CNBC",           "https://www.cnbc.com/quotes/{ticker}?tab=news",     "a.title::text"),
]

def scrapling_news(tickers, min_total=20):
    """Scrapling 深度抓取 TOP3 个股全网新闻、公告、机构评论
    去重过滤无效内容，每个 ticker 目标 ≥3 条，总计 ≥10 条
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
            print("Scrapling mode: stealth (Playwright-based)")
        else:
            from scrapling.fetchers import Fetcher
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
                        page = StealthyFetcher.fetch(url, headless=True, network_idle=True, timeout=15)
                    else:
                        page = Fetcher.get(url)
                    titles = page.css(css_sel)

                    for el in titles[:3]:
                        # Handle Scrapling elements securely. Sometimes they contain whitespace.
                        text = el.get() if hasattr(el, 'get') else (el.text if hasattr(el, 'text') else str(el))
                        if text:
                            text = text.strip()
                        if not text or len(text) < 10 or text.isspace():
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
        else:
            print(f"Scrapling total: {len(all_news)} articles (target not met, falling back to Finnhub for more)")
            all_news.extend(_finhub_stock_news(tickers))
            return all_news

    except ImportError:
        print("scrapling not installed, falling back to Finnhub")
    except Exception as e:
        print(f"Scrapling failed: {e}, falling back to Finnhub")

    # Fallback: Finnhub company-news
    try:
        all_news
    except NameError:
        all_news = []

    all_news.extend(_finhub_stock_news(tickers))
    return all_news

def _finhub_stock_news(tickers, days=3):
    """Finnhub 兜底个股新闻"""
    results = []
    if not FINNHUB_KEY:
        return results
    from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Use a lock and a shared last_request_time to implement rate limiting
    rate_limit_lock = threading.Lock()
    last_request_time = [0.0]
    min_interval = 1.01  # Slightly more than 1 second to stay under 60 req/min

    def fetch_ticker_news(ticker):
        ticker_news = []
        try:
            now = time.time()
            with rate_limit_lock:
                elapsed = now - last_request_time[0]
                if elapsed < min_interval:
                    sleep_time = min_interval - elapsed
                else:
                    sleep_time = 0
                last_request_time[0] = now + sleep_time
            if sleep_time > 0:
                time.sleep(sleep_time)

            url = (f"https://finnhub.io/api/v1/company-news?symbol={ticker}"
                   f"&from={from_date}&to={to_date}&token={FINNHUB_KEY}")
            r = requests.get(url, timeout=8)
            items = r.json()
            for n in items[:5]:
                headline = n.get("headline", "")
                if headline:
                    cn = translate_to_cn(headline)
                    ticker_news.append({"headline": cn, "source": n.get("source", ""), "ticker": ticker})
        except Exception:
            pass
        return ticker_news

    # Even with rate limiting, we use ThreadPoolExecutor to allow other tickers
    # to start their wait or process their results while one is fetching.
    # Given the 1s rate limit, max_workers doesn't need to be high, but helps with overlap.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # map naturally preserves the order of the input iterable
        for ticker_news in executor.map(fetch_ticker_news, tickers):
            results.extend(ticker_news)

    print(f"Finnhub stock news: {len(results)} articles")
    return results

# ─── Gist 记忆体引擎（7天滚动观察列表）──────────────────────────────────────

GIST_PAT = os.environ.get("GIST_PAT", "")
GIST_ID  = os.environ.get("GIST_ID", "")

def update_and_get_watchlist(today_top_tickers):
    """读取 Gist 观察列表 → 清理7天过期 → 录入今日强势股（按天去重）→ 写回
    返回: dict {ticker: {"count": int, "last_seen": "YYYY-MM-DDTHH:MM:SS+00:00"}}
    """
    if not GIST_PAT or not GIST_ID:
        print("⚠️ 缺少 Gist 环境变量，记忆体失效")
        return {}

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GIST_PAT}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # 1. 读取
    watchlist = {}
    try:
        resp = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers, timeout=10)
        if resp.status_code == 200:
            content = resp.json()["files"]["watchlist.json"]["content"]
            watchlist = json.loads(content)
            print(f"Gist read OK: {len(watchlist)} tracked stocks")
        else:
            print(f"Gist read failed: HTTP {resp.status_code}")
    except Exception as e:
        print(f"Gist read failed: {e}")

    # 2. 清理7天过期
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)
    cleaned = {}
    for k, v in watchlist.items():
        try:
            if datetime.fromisoformat(v["last_seen"]) > cutoff:
                cleaned[k] = v
        except Exception:
            continue
    pruned = len(watchlist) - len(cleaned)
    if pruned:
        print(f"Gist pruned {pruned} expired entries")

    # 3. 录入今日强势股（同一天不重复计数）
    now_str = now.isoformat()
    now_date = now_str[:10]
    for ticker in today_top_tickers:
        if ticker in cleaned:
            last_date = cleaned[ticker]["last_seen"][:10]
            if last_date != now_date:
                cleaned[ticker]["count"] += 1
                cleaned[ticker]["last_seen"] = now_str
        else:
            cleaned[ticker] = {"count": 1, "last_seen": now_str}

    # 4. 写回
    try:
        payload = {"files": {"watchlist.json": {"content": json.dumps(cleaned, indent=2)}}}
        r = requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            print(f"Gist write OK: {len(cleaned)} stocks")
        else:
            print(f"Gist write failed: HTTP {r.status_code}")
    except Exception as e:
        print(f"Gist write failed: {e}")

    return cleaned

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
                strike = float(si.price if hasattr(si, 'price') else getattr(si, 'strike_price', 0))
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
                greeks = greeks_map.get(sym, {})

                # option_extend and bid_price/ask_price are not available directly in longport python SDK OptionQuote
                expiry_val = getattr(q, 'expiry_date', exp_str)
                if expiry_val and not isinstance(expiry_val, str):
                    expiry_val = expiry_val.strftime("%Y-%m-%d") if hasattr(expiry_val, 'strftime') else str(expiry_val)

                all_contracts.append({
                    "strike": meta["strike"],
                    "expiry": expiry_val,
                    "type": meta["type"],
                    "bid": getattr(q, 'bid_price', 0) if q else 0, # usually missing in OptionQuote
                    "ask": getattr(q, 'ask_price', 0) if q else 0,
                    "last_done": float(getattr(q, 'last_done', 0) or 0) if q else 0,
                    "iv": greeks.get("iv", float(getattr(q, 'implied_volatility', 0) or 0)),
                    "delta": greeks.get("delta", 0),
                    "gamma": greeks.get("gamma", 0),
                    "theta": greeks.get("theta", 0),
                    "vega": greeks.get("vega", 0),
                    "volume": int(getattr(q, 'volume', 0) or 0) if q else 0,
                    "oi": int(getattr(q, 'open_interest', 0) or 0) if q else 0,
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


def option_analysis(ticker, price, score, vix, ai_action="确认量化信号"):
    """深度期权分析：拉取期权链 → 检测异动 → AI一票否决 → 推荐合约
    综合考虑: 量化评分方向 + AI审判 + 期权成交量异动 + IV水平 + VIX环境
    
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

    # 🚨 4. AI 一票否决权介入
    direction = base_direction
    signal_shift = ""

    if "降级" in ai_action or "观望" in ai_action:
        direction = "neutral"
        signal_shift = "⚠️ AI强制降级：存在基本面风险，取消多头期权推荐"
    elif "反转" in ai_action or "反向" in ai_action:
        direction = "bear"
        signal_shift = "🚨 AI强制反转：存在致命利空，转为看跌策略"

    if unusual:
        bull_signals = sum(1 for u in unusual if "看涨" in u["intent"])
        bear_signals = sum(1 for u in unusual if "看跌" in u["intent"])
        if bull_signals > bear_signals + 1:
            unusual_direction = "bull"
        elif bear_signals > bull_signals + 1:
            unusual_direction = "bear"
        else:
            unusual_direction = "neutral"
        
        if unusual_direction != "neutral" and unusual_direction != direction:
            shift_msg = f" | 另注: 系统看{direction}但期权异动看{unusual_direction}"
            signal_shift = signal_shift + shift_msg if signal_shift else f"⚠️系统看{direction}但期权异动={unusual_direction}"

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

def build_option_picks(scored_stocks, vix, stock_reasonings=None):
    """根据评分+VIX+AI审判给出具体期权合约建议"""
    buys = [s for s in scored_stocks if s["score"] > 58][:3]
    if not buys:
        return []
    picks = []
    for s in buys:
        price = s["price"]
        ticker = s["ticker"]
        score = s["score"]

        # 🚨 检查 AI 审判结果，执行强制拦截
        if stock_reasonings:
            ai_verdict = stock_reasonings.get(ticker, {})
            ai_action = ai_verdict.get("action", "")
            if any(word in ai_action for word in ("降级", "观望")):
                # 降级为中性：只推荐保守策略
                s_buy = round_strike(price * 0.97, "itm")
                s_sell = round_strike(price * 1.03, "otm")
                picks.append(f"{ticker} AI降级→Bull Call Spread 买${s_buy}/卖${s_sell} | 30-60天")
                continue
            elif any(word in ai_action for word in ("反转", "反向")):
                # 反转为看跌
                strike_put = round_strike(price * 0.98, "itm")
                picks.append(f"{ticker} AI反转→买入Put ${strike_put}/30-45天")
                continue

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
    """批量个股推理：TOP3 逐只调用 AI 新闻汇总分析
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
    top3 = scored_stocks[:3]
    print(f"[AI] Starting batch stock reasoning for {len(top3)} stocks...")
    for i, s in enumerate(top3):
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

        print(f"[AI] Reasoning [{i+1}/{len(top3)}] {ticker} (news={len(headlines)})...")
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
    top3 = scored_stocks[:3]

    # ── 构建上下文 ──
    ctx = f"## 市场环境\nVIX={vix:.1f}({regime['label']},模式:{regime['mode']})\n\n"

    # TOP3 评分 + AI评级
    ctx += "## TOP3评分\n"
    for i, s in enumerate(top3):
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
        for ticker in [s["ticker"] for s in top3]:
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
    vix_spike = vix > 35  # 用绝对值判断，不参考涨跌幅

    # ── Prompt ──
    prompt = f"""{ctx}

你是资深美股量化+期权策略师。基于以上所有数据（行情+个股AI推理+Scrapling深度新闻+期权链），进行深度推理。

{"⚠️【🔴 宏观红线触发】VIX超过35（当前" + f"{vix:.1f}" + "），系统进入避险模式！" if vix_spike else ""}

【🔴 宏观红线铁律 (最高优先级)】
1. 情绪熔断：若 VIX 超过35，或宏观新闻涉及战争、局部冲突、重大自然灾害，【情绪判断】严禁给出"利好"，最高只能是"避险"。
2. 策略约束：在避险/防守环境下，【期权交易建议】禁止推荐纯多头买方策略（如裸买 Call）。

请输出以下结构化内容：

**1. 简讯**：用3-5句话概括今日TOP3个股的核心动态，不要罗列，要提炼因果逻辑

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
    "close":   "🏁 美股选股·收盘总结",
}

PUSH_SUBTITLES = {
    "morning": "开盘前45分钟 · 策略准备",
    "close":   "收盘 · 今日复盘 + 次日预判",
}

WEIGHT_LABELS = {
    "momentum":   "动量",
    "quality":    "质量",
    "valuation":  "估值",
    "stability":  "稳定",
    "position":   "趋势位",
    "pullback":   "回踩",
}

def build_feishu_text(vix_data, scored_stocks, push_type, stock_news=None, macro_news=None, ai_summary=None, option_analyses=None, stock_reasonings=None, reversal_stocks=None, joely_tweets=None):
    now_bjt = datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    vix     = vix_data["price"]
    regime  = get_vix_regime(vix)
    w       = get_weights(vix)

    top3    = scored_stocks[:3]
    movers  = [s for s in scored_stocks if abs(s["change_pct"]) > 3]

    lines = []
    lines.append(PUSH_TITLES.get(push_type, "📊 量化选股播报"))
    lines.append(PUSH_SUBTITLES.get(push_type, ""))
    lines.append("")

    # VIX
    vix_src = vix_data.get("source", "?")
    vix_as_of = vix_data.get("as_of", "")
    vix_time_tag = f" [{vix_src} {vix_as_of}]" if vix_as_of else f" [{vix_src}]"
    lines.append(f"{regime['emoji']} VIX 恐慌指数：{vix:.1f}{vix_time_tag}")
    lines.append(f"市场情绪：{regime['label']} · 策略模式：{regime['mode']}")
    w_str = " · ".join([f"{WEIGHT_LABELS[k]} {round(v*100)}%" for k, v in w.items()])
    lines.append(f"当前权重 · {w_str}")
    lines.append("")

    # TOP3
    lines.append("📊 综合评分 TOP3")
    for i, s in enumerate(top3):
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

    # 均值回归参考（2只超卖票）
    if reversal_stocks:
        lines.append("🔄 均值回归参考（超跌反弹机会）")
        for s in reversal_stocks[:2]:
            chg = s["change_pct"]
            chg_str = f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%"
            ai_r = stock_reasonings.get(s["ticker"], {}) if stock_reasonings else {}
            ai_stars = f" ⭐{ai_r.get('rating','-')}" if ai_r.get("rating") else ""
            ai_action = ai_r.get("action", "")
            action_tag = f" ⚖️{ai_action}" if ai_action and ai_action != "确认量化信号" else ""
            lines.append(
                f"  🔄 {s['ticker']} 评分{s['reversal_score']}  {chg_str}{ai_stars}  "
                f"💡{s['option_strategy']}  📦{s['position']}{action_tag}"
            )
            if ai_r.get("reason"):
                lines.append(f"     AI: {ai_r['reason']}")
        lines.append("⚠️ 均值回归策略风险较高，建议配合CSP或Put Spread入场")
        lines.append("")

    # 期权提示
    if vix >= 35:
        lines.append("🔴 VIX>35，极度恐慌！暂停期权买方，以卖方策略或空仓为主")
    elif vix >= 25:
        lines.append("🎯 VIX>25，IV偏高，卖方策略（CSP/Iron Condor）占优")
    else:
        lines.append("✅ VIX<25，IV偏低，买方策略（Long Call/Spread）成本合理")
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
        for ticker in [s["ticker"] for s in scored_stocks[:3]]:
            if ticker in by_ticker:
                for n in by_ticker[ticker][:3]:
                    lines.append(f"  · [{ticker}] [{n.get('source','')}] {n['headline']}")
        lines.append("")

    # 期权合约建议（基础版）
    option_picks = build_option_picks(scored_stocks, vix, stock_reasonings)
    if option_picks:
        lines.append("🎯 期权合约建议")
        for p in option_picks:
            lines.append(f"  ▸ {p}")
        lines.append("")

    # 期权深度分析（LongPort + AI）
    if option_analyses:
        lines.append("📊 期权链深度分析")
        for oa in option_analyses:
            sniper_tag = " 🎯狙击点" if oa.get("sniper") else ""
            iv_str = f" IV={oa['avg_iv']}%" if oa.get("avg_iv") else ""
            lines.append(f"  【{oa['ticker']}】{oa.get('strategy','')} | 方向:{oa['direction']}{iv_str}{sniper_tag}")
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

# Joely 抓取
    if joely_tweets:
        lines.append("🐦 Joely 最新动态")
        for t in joely_tweets:
            text = t.get("text", "")
            images = t.get("images", [])
            lines.append(f"  ▸ {text}")
            if images:
                lines.append(f"    (包含 {len(images)} 张图片)")
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

# 0.5 抓取推特
    joely_tweets = []
    try:
        new_tweets = fetch_twitter_timeline("joely7758521")
        joely_tweets = new_tweets
    except Exception as e:
        print(f"❌ Error in Twitter fetch: {e}")

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
    reversal_candidates = []  # 均值回归候选
    for s in UNIVERSE:
        q = quotes.get(s["ticker"])
        score = compute_score(q, vix)
        if score is None:
            continue
        rev_score = compute_score_reversal(q, vix)
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
        if rev_score and q.get("change_pct", 0) < 0:  # 只看下跌票
            reversal_candidates.append({
                **s,
                "reversal_score":  rev_score,
                "signal":          get_signal(rev_score),
                "option_strategy": get_option_strategy(rev_score, vix),
                "position":        get_position(rev_score, vix),
                "price":           q["price"],
                "change_pct":      q["change_pct"],
                "pe":              q.get("pe"),
            })
    scored.sort(key=lambda x: x["score"], reverse=True)
    reversal_candidates.sort(key=lambda x: x["reversal_score"], reverse=True)
    reversal_top2 = reversal_candidates[:2]

    if not scored:
        print("❌ No stocks scored, aborting")
        sys.exit(1)
    print(f"Scored {len(scored)} stocks, top: {scored[0]['ticker']} ({scored[0]['score']})")
    if reversal_top2:
        print(f"Reversal candidates: {[(r['ticker'], r['reversal_score']) for r in reversal_top2]}")

    # 4. Scrapling 深度抓取个股新闻（TOP3 + 均值回归TOP2，≥15条）
    top3_tickers = [s["ticker"] for s in scored[:3]]
    reversal_tickers = [s["ticker"] for s in reversal_top2]
    news_tickers = list(dict.fromkeys(top3_tickers + reversal_tickers))  # 去重保序
    stock_news = scrapling_news(news_tickers, min_total=15)

    # 5. 拉宏观新闻
    macro_news = fetch_news(vix)

    # 6. AI 个股新闻推理（TOP3 + 均值回归TOP2）
    print(f"[STEP6] Starting AI stock reasoning for {len(news_tickers)} stocks...")
    # 合并两个列表供AI推理，去重
    reasoning_stocks = scored[:3] + [s for s in reversal_top2 if s["ticker"] not in {x["ticker"] for x in scored[:3]}]
    stock_reasonings = batch_stock_reasoning(reasoning_stocks, stock_news)

    # 7. Gist 记忆体更新（7天滚动观察列表）
    today_strong_tickers = [s["ticker"] for s in scored if s["score"] > 60]
    history_cache = update_and_get_watchlist(today_strong_tickers)

    # 8. 基础期权分析（score>58 的 TOP3，带 AI 否决）
    top_buys = [s for s in scored if s["score"] > 58][:3]
    print(f"[STEP8] Option analysis with AI Veto: {len(top_buys)} buy candidates ({[s['ticker'] for s in top_buys]})")
    option_analyses = []
    sniper_hits = []  # 狙击手命中的 ticker 列表
    for s in top_buys:
        ai_verdict = stock_reasonings.get(s["ticker"], {})
        ai_action = ai_verdict.get("action", "确认量化信号")
        oa = option_analysis(s["ticker"], s["price"], s["score"], vix, ai_action)
        option_analyses.append(oa)

    # 9. 回踩狙击手（叠加模式：额外的狙击点分析）
    print(f"[STEP9] Sniper Pullback Analysis...")
    pullback_low = -5.0 if vix > 25 else -3.5
    pullback_candidates = [s for s in scored if s["score"] > 58 and s.get("change_pct", 0) < 0]
    existing_tickers = {oa["ticker"] for oa in option_analyses}
    for s in pullback_candidates:
        ticker = s["ticker"]
        change_pct = s.get("change_pct", 0)
        if ticker in existing_tickers:
            continue  # 已在基础推荐中，跳过
        if ticker in history_cache and history_cache[ticker]["count"] >= 2:
            if pullback_low <= change_pct <= -1.0:
                ai_verdict = stock_reasonings.get(ticker, {})
                ai_action = ai_verdict.get("action", "确认量化信号")
                if "降级" not in ai_action and "反转" not in ai_action:
                    print(f"🎯 狙击点命中: {ticker} change={change_pct:+.1f}% count={history_cache[ticker]['count']}")
                    oa = option_analysis(ticker, s["price"], s["score"], vix, ai_action)
                    oa["sniper"] = True  # 标记为狙击手推荐
                    option_analyses.append(oa)
                    sniper_hits.append(ticker)

    # 汇总期权数据源分布
    ds_counts = {}
    for oa in option_analyses:
        ds = oa.get("data_source", "估算值")
        ds_counts[ds] = ds_counts.get(ds, 0) + 1
    real_count = sum(oa.get("real_data", False) for oa in option_analyses)
    print(f"[STEP8-9] Option analyses done: {len(option_analyses)} results | 狙击点:{sniper_hits or '无'} | 真实数据:{real_count}/{len(option_analyses)} | 数据源分布:{ds_counts}")

    # 10. AI 宏观策略推理（整合个股推理+期权分析）
    print(f"[STEP10] Starting AI macro analysis...")
    ai_summary = ai_analyze(vix_data, scored, stock_news, macro_news, option_analyses, stock_reasonings)

    # 11. 构建消息
    print(f"[STEP11] Building Feishu message...")
    text = build_feishu_text(vix_data, scored, push_type, stock_news, macro_news, ai_summary, option_analyses, stock_reasonings, reversal_top2, joely_tweets)

    # 12. 推送
    print(f"[STEP12] Pushing to Feishu ({len(text)} chars)...")
    success = push_to_feishu(text)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
