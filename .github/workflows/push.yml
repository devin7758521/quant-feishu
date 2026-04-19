#!/usr/bin/env python3
# scripts/push.py
# 拉取实时行情 + VIX → 计算量化评分 → 推送飞书
# 数据源: Twelve Data (主) + Finnhub (备) + Yahoo Finance VIX

import os
import sys
import json
import math
import requests
from datetime import datetime, timezone, timedelta

# ─── 配置 ────────────────────────────────────────────────────────────────────

FEISHU_WEBHOOK  = os.environ.get("FEISHU_WEBHOOK_URL", "")
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
FINNHUB_KEY     = os.environ.get("FINNHUB_API_KEY", "")
PUSH_TYPE       = os.environ.get("PUSH_TYPE", "morning")  # morning/open/midday/close

# 北京时间
BJT = timezone(timedelta(hours=8))

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
    """从 Yahoo Finance 拉 VIX"""
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=%5EVIX"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        data = r.json()
        q = data["quoteResponse"]["result"][0]
        return {"price": q["regularMarketPrice"], "change": q["regularMarketChangePercent"]}
    except Exception as e:
        print(f"VIX fetch failed: {e}")
        return None

def fetch_quotes_twelvedata():
    """Twelve Data 批量行情"""
    tickers = [s["ticker"].replace("BRK-B", "BRK/B") for s in UNIVERSE]
    url = f"https://api.twelvedata.com/quote?symbol={','.join(tickers)}&apikey={TWELVE_DATA_KEY}"
    r = requests.get(url, timeout=30)
    data = r.json()
    result = {}
    for s in UNIVERSE:
        td_key = s["ticker"].replace("BRK-B", "BRK/B")
        q = data.get(td_key) or data.get(td_key.replace("/", ":"))
        if not q or q.get("status") == "error":
            continue
        try:
            result[s["ticker"]] = {
                "price":         float(q.get("close") or 0),
                "change_pct":    float(q.get("percent_change") or 0),
                "high52w":       float(q.get("fifty_two_week", {}).get("high") or 0),
                "low52w":        float(q.get("fifty_two_week", {}).get("low") or 0),
                "pe":            float(q.get("pe") or 0) or None,
                "volume":        int(q.get("volume") or 0),
            }
        except Exception:
            continue
    return result

def fetch_quotes_finnhub():
    """Finnhub 逐个拉行情（备用）"""
    result = {}
    for s in UNIVERSE:
        try:
            url = f"https://finnhub.io/api/v1/quote?symbol={s['ticker']}&token={FINNHUB_KEY}"
            r = requests.get(url, timeout=8)
            q = r.json()
            if q.get("c") and q["c"] > 0:
                pc = q.get("pc", q["c"])
                result[s["ticker"]] = {
                    "price":      float(q["c"]),
                    "change_pct": round((q["c"] - pc) / pc * 100, 2) if pc else 0,
                    "high52w":    float(q.get("h", 0)),
                    "low52w":     float(q.get("l", 0)),
                    "pe":         None,
                    "volume":     0,
                }
        except Exception:
            continue
    return result

def fetch_quotes():
    """主入口：Twelve Data → Finnhub → 失败"""
    quotes = {}
    if TWELVE_DATA_KEY:
        try:
            quotes = fetch_quotes_twelvedata()
            print(f"Twelve Data: {len(quotes)} stocks")
        except Exception as e:
            print(f"Twelve Data failed: {e}")
    if len(quotes) < 10 and FINNHUB_KEY:
        try:
            quotes = fetch_quotes_finnhub()
            print(f"Finnhub: {len(quotes)} stocks")
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
    if vix < 15:  return {"momentum": 0.35, "quality": 0.20, "valuation": 0.10, "low_vol": 0.10, "earnings": 0.25}
    if vix < 20:  return {"momentum": 0.30, "quality": 0.25, "valuation": 0.15, "low_vol": 0.12, "earnings": 0.18}
    if vix < 25:  return {"momentum": 0.22, "quality": 0.28, "valuation": 0.20, "low_vol": 0.15, "earnings": 0.15}
    if vix < 30:  return {"momentum": 0.15, "quality": 0.30, "valuation": 0.25, "low_vol": 0.20, "earnings": 0.10}
    if vix < 35:  return {"momentum": 0.10, "quality": 0.32, "valuation": 0.25, "low_vol": 0.25, "earnings": 0.08}
    return              {"momentum": 0.05, "quality": 0.28, "valuation": 0.20, "low_vol": 0.40, "earnings": 0.07}

def compute_score(quote, vix):
    if not quote or not quote.get("price"):
        return None
    price    = quote["price"]
    chg      = quote["change_pct"]
    high52w  = quote["high52w"] or price * 1.2
    low52w   = quote["low52w"]  or price * 0.8
    pe       = quote["pe"]

    rng = high52w - low52w
    pos52w   = ((price - low52w) / rng * 100) if rng > 0 else 50
    momentum = min(max((chg + 5) * 10 * 0.4 + pos52w * 0.6, 0), 100)
    quality  = min(max((65 - pe) / 60 * 100, 5), 95) if pe else 50
    valuation= min(max((55 - pe) / 50 * 100, 5), 95) if pe else 50
    low_vol  = 100 - abs(pos52w - 50)
    earnings = min(max(50 + chg * 5, 0), 100)

    w = get_weights(vix)
    score = (momentum  * w["momentum"]  +
             quality   * w["quality"]   +
             valuation * w["valuation"] +
             low_vol   * w["low_vol"]   +
             earnings  * w["earnings"])
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
    w_str = (f"动量 {round(w['momentum']*100)}% · "
             f"质量 {round(w['quality']*100)}% · "
             f"估值 {round(w['valuation']*100)}% · "
             f"低波 {round(w['low_vol']*100)}% · "
             f"盈利 {round(w['earnings']*100)}%")
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
        "card": json.dumps(card),
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
    print(f"Scored {len(scored)} stocks, top: {scored[0]['ticker']} ({scored[0]['score']})")

    # 4. 构建消息
    card = build_feishu_card(vix_data, scored, push_type)

    # 5. 推送
    success = push_to_feishu(card)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
