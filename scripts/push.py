#!/usr/bin/env python3
# scripts/push.py
# 拉取实时行情 + VIX → 计算量化评分 → 推送飞书
# 数据源: Twelve Data (主) + Finnhub (补缺) + Yahoo/FRED VIX

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone, timedelta

# ─── 配置 ────────────────────────────────────────────────────────────────────

FEISHU_WEBHOOK  = os.environ.get("FEISHU_WEBHOOK_URL", "")
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
FINNHUB_KEY     = os.environ.get("FINNHUB_API_KEY", "")
PUSH_TYPE       = os.environ.get("PUSH_TYPE", "morning")  # morning/open/midday/close

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

# ─── 股票池（50只）────────────────────────────────────────────────────────────

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
    ticker_list = [s["ticker"].replace("BRK-B", "BRK/B") for s in UNIVERSE]
    ticker_to_stock = {s["ticker"].replace("BRK-B", "BRK/B"): s for s in UNIVERSE}
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
            time.sleep(8)
    return result

        # 免费版8次/分钟，每批1次请求，间隔12秒
        if i + BATCH_SIZE < len(ticker_list):
            time.sleep(12)
    return result

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

# ─── 飞书消息构建 ─────────────────────────────────────────────────────────────

PUSH_TITLES = {
    "morning": "🌙 美股开盘前预热",
    "open":    "🔔 美股已开盘",
    "midday":  "🌙 美股半场复盘",
    "close":   "🏁 美股收盘总结",
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

def build_feishu_card(vix_data, scored_stocks, push_type):
    now_bjt = datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    vix     = vix_data["price"]
    vix_chg = vix_data["change"]
    regime  = get_vix_regime(vix)
    w       = get_weights(vix)

    top10   = scored_stocks[:10]
    movers  = [s for s in scored_stocks if abs(s["change_pct"]) > 3]

    # ── Header ──────────────────────────────────────────────────────────────
    header_color = (
        "green"  if vix < 20 else
        "yellow" if vix < 25 else
        "orange" if vix < 35 else
        "red"
    )

    elements = []

    # ── VIX 模块 ────────────────────────────────────────────────────────────
    vix_chg_str = f"+{vix_chg:.1f}%" if vix_chg >= 0 else f"{vix_chg:.1f}%"
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": (
                f"**{regime['emoji']} VIX 恐慌指数：{vix:.1f}** （{vix_chg_str}）\n"
                f"市场情绪：**{regime['label']}** · 策略模式：**{regime['mode']}**"
            )
        }
    })

    # 因子权重
    w_str = " · ".join([f"{WEIGHT_LABELS[k]} {round(v*100)}%" for k, v in w.items()])
    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md", "content": f"当前权重 · {w_str}"}
    })
    elements.append({"tag": "hr"})

    # ── TOP10 排名 ───────────────────────────────────────────────────────────
    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md", "content": "**📊 综合评分 TOP10**"}
    })

    rows = []
    for i, s in enumerate(top10):
        chg = s["change_pct"]
        chg_str = f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%"
        chg_icon = "📈" if chg >= 0 else "📉"
        rows.append(
            f"**{i+1}. {s['ticker']}** {s['signal']}  "
            f"评分 `{s['score']}`  {chg_icon}{chg_str}  "
            f"💡 {s['option_strategy']}  📦 {s['position']}"
        )

    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md", "content": "\n".join(rows)}
    })
    elements.append({"tag": "hr"})

    # ── 异动股 ──────────────────────────────────────────────────────────────
    if movers:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "**🚀 今日异动（涨跌>3%）**"}
        })
        mover_rows = []
        for s in movers[:8]:
            chg = s["change_pct"]
            icon = "🚀" if chg > 0 else "💥"
            mover_rows.append(
                f"{icon} **{s['ticker']}** {'+' if chg>0 else ''}{chg:.1f}%  "
                f"评分{s['score']}  {s['option_strategy']}"
            )
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "\n".join(mover_rows)}
        })
        elements.append({"tag": "hr"})

    # ── 期权条件提示 ─────────────────────────────────────────────────────────
    if vix >= 35:
        opt_tip = "⚠️ **VIX>35，建议暂停期权买方，以卖方策略或空仓为主**"
    elif vix >= 25:
        opt_tip = "🎯 **VIX>25，IV偏高，卖方策略（CSP/Iron Condor）占优**"
    else:
        opt_tip = "✅ **VIX<25，IV偏低，买方策略（Long Call/Spread）成本合理**"
    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md", "content": opt_tip}
    })
    elements.append({"tag": "hr"})

    # ── 强买信号全列 ─────────────────────────────────────────────────────────
    strong_buys = [s for s in scored_stocks if s["score"] > 72]
    if strong_buys:
        sb_list = "、".join([s["ticker"] for s in strong_buys[:8]])
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"🔥 **强买信号（评分>72）：{sb_list}**"}
        })

    # ── 时间戳 ──────────────────────────────────────────────────────────────
    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md", "content": f"⏰ {now_bjt} 北京时间 · 数据仅供参考"}
    })

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": f"{PUSH_TITLES.get(push_type, '📊 Quant Alpha 推送')}"
            },
            "subtitle": {
                "tag": "plain_text",
                "content": PUSH_SUBTITLES.get(push_type, "")
            },
            "template": header_color,
        },
        "elements": elements,
    }
    return card

# ─── 推送飞书 ─────────────────────────────────────────────────────────────────

def push_to_feishu(card):
    if not FEISHU_WEBHOOK:
        print("❌ FEISHU_WEBHOOK_URL not set")
        return False
    payload = {
        "msg_type": "interactive",
        "card": card,
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
        })
    scored.sort(key=lambda x: x["score"], reverse=True)

    if not scored:
        print("❌ No stocks scored, aborting")
        sys.exit(1)
    print(f"Scored {len(scored)} stocks, top: {scored[0]['ticker']} ({scored[0]['score']})")

    # 4. 构建消息
    card = build_feishu_card(vix_data, scored, push_type)

    # 5. 推送
    success = push_to_feishu(card)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
