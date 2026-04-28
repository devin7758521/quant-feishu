import re
import os

# 1. Update push.py
with open("scripts/push.py", "r", encoding="utf-8") as f:
    code = f.read()

twitter_code = """
# ─── Twitter 抓取 (RapidAPI) ──────────────────────────────────────────────────

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")

def fetch_twitter_timeline(username="joely7758521"):
    \"\"\"
    从 RapidAPI 获取用户的最新推文。
    你需要在 RapidAPI 订阅一个 Twitter/X API 服务（如 Twitter API45 或类似的 Scraper），
    并在环境变量中设置 RAPIDAPI_KEY。
    \"\"\"
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

"""

if "fetch_twitter_timeline" not in code:
    code = code.replace("def fetch_vix():", twitter_code + "\ndef fetch_vix():")


code = code.replace("def build_feishu_text(vix_data, scored_stocks, push_type, stock_news=None, macro_news=None, ai_summary=None, option_analyses=None, stock_reasonings=None, reversal_stocks=None):",
                    "def build_feishu_text(vix_data, scored_stocks, push_type, stock_news=None, macro_news=None, ai_summary=None, option_analyses=None, stock_reasonings=None, reversal_stocks=None, joely_tweets=None):")

new_feishu_joely = """
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
"""
if "# Joely 抓取" not in code:
    code = re.sub(r'    lines.append\(f"⏰ {now_bjt} 北京时间 .*?数据仅供参考"\)', new_feishu_joely.strip(), code, flags=re.DOTALL)


main_addition = """
    # 0.5 抓取推特
    joely_tweets = []
    try:
        new_tweets = fetch_twitter_timeline("joely7758521")
        joely_tweets = new_tweets
    except Exception as e:
        print(f"❌ Error in Twitter fetch: {e}")

    # 1. 拉 VIX
"""
if "0.5 抓取推特" not in code:
    code = code.replace("    # 1. 拉 VIX", main_addition.strip())

code = code.replace("text = build_feishu_text(vix_data, scored, push_type, stock_news, macro_news, ai_summary, option_analyses, stock_reasonings, reversal_top2)",
                    "text = build_feishu_text(vix_data, scored, push_type, stock_news, macro_news, ai_summary, option_analyses, stock_reasonings, reversal_top2, joely_tweets)")


with open("scripts/push.py", "w", encoding="utf-8") as f:
    f.write(code)

# 2. Update README.md
with open("README.md", "r", encoding="utf-8") as f:
    text = f.read()

if "RAPIDAPI_KEY" not in text:
    text = text.replace("| `SCRAPLING_MODE`", "| `RAPIDAPI_KEY` | RapidAPI Key（用于抓取 X/Twitter 数据，建议订阅 Twitter API45 等免费/廉价接口）|\n| `SCRAPLING_MODE`")
if "X/Twitter 动态监控" not in text:
    text = text.replace("- **期权合约建议**", "- **X/Twitter 动态监控**：实时跟进 @joely7758521 的最新推文及配图状态\n- **期权合约建议**")

with open("README.md", "w", encoding="utf-8") as f:
    f.write(text)

print("done")
