# ⚡ Quant Alpha · 飞书推送版

不需要网页、不需要服务器，每天4次自动推送美股量化分析到飞书群。
用 GitHub Actions 免费运行，零成本。

---

## 推送时间（北京时间，工作日）

| 推送 | 时间 | 内容 |
|------|------|------|
| 🌙 盘前预热 | **21:47** | VIX + TOP3混合评分 + 期权策略 + 均值回归参考 |
| 🏁 收盘总结 | **05:47** | 今日复盘 + AI策略研判 + 狙击点 |

---

## 部署步骤（10分钟）

### 第一步：新建 GitHub 仓库

1. github.com → New repository → 命名 `quant-feishu` → Create
2. 把这个文件包里的所有文件上传进去

### 第二步：配置飞书机器人

1. 打开飞书 → 进入你的群
2. 群设置 → 群机器人 → 添加机器人 → 自定义机器人
3. 命名 `Quant Alpha` → 复制 Webhook URL

### 第三步：在 GitHub 填入 Secrets

1. GitHub 仓库 → Settings → Secrets and variables → Actions
2. 点 **New repository secret**，依次添加：

| Secret 名称 | 值 |
|------------|-----|
| `FEISHU_WEBHOOK_URL` | 飞书机器人 Webhook URL |
| `TWELVE_DATA_API_KEY` | Twelve Data API Key（twelvedata.com 免费注册）|
| `FINNHUB_API_KEY` | Finnhub API Key（finnhub.io 免费注册，备用）|
| `LONGPORT_APP_KEY` | 长桥 OpenAPI App Key（open.longportapp.com，需开户）|
| `LONGPORT_APP_SECRET` | 长桥 OpenAPI App Secret |
| `LONGPORT_ACCESS_TOKEN` | 长桥 OpenAPI Access Token |
| `DEEPSEEK_API_KEY` | DeepSeek API Key（deepseek.com 注册）|
| `DEEPSEEK_BASE_URL` | DeepSeek 自定义端点（可选，默认官方）|
| `DEEPSEEK_MODEL` | DeepSeek 模型名（可选，默认 deepseek-v4-flash）|
| `GEMINI_API_KEY` | Gemini API Key #1（aistudio.google.com 免费）|
| `GEMINI_MODEL` | Gemini #1 模型名（可选，默认 gemini-2.0-flash）|
| `GEMINI_API_KEY_2` | Gemini API Key #2（第二个账号/项目，可选）|
| `GEMINI_MODEL_2` | Gemini #2 模型名（可选，默认 gemini-2.0-flash）|
| `TWITTER_USERNAME` | Twitter 用户名（twscrape 抓取推文用）|
| `TWITTER_PASSWORD` | Twitter 密码 |
| `TWITTER_EMAIL` | Twitter 绑定邮箱 |
| `TWSCRAPE_GIST_ID` | twscrape 状态持久化 Gist ID（可选）|
| `SCRAPLING_MODE` | Scrapling 抓取模式：`basic`（默认，HTTP）或 `stealth`（Playwright反爬）|

> **AI降级轮换**: deepseek → gemini → gemini2，每次调用自动轮换，失败自动降级到下一个。至少配置1个API Key即可运行。

### 第四步：启用 Actions

1. GitHub 仓库 → Actions 标签
2. 如果提示需要启用，点击 **Enable**
3. 左侧找到 `Quant Alpha 飞书推送`
4. 点 **Run workflow** 手动测试一次，看飞书群有没有收到消息

---

## 手动触发

任何时候想看最新数据：
GitHub → Actions → Quant Alpha 飞书推送 → Run workflow → 选类型 → Run

---

## 飞书消息内容

每次推送包含：
- **VIX 恐慌指数** + 市场情绪判断 + 因子权重
- **混合型评分 TOP3**：评分、AI评级(0-5星)、AI审判、期权策略、仓位
- **均值回归参考 TOP2**：超跌反弹机会（仅当日有下跌票时显示）
- **异动股提醒**（涨跌>3%）
- **宏观新闻**：市场要闻、商业财经、国际政经、科技动态
- **个股深度新闻**：Scrapling 抓取全网新闻/公告/机构评论（≥15条）
- **X/Twitter 动态监控**：实时跟进 @joely7758521 的最新推文
- **期权合约建议**（AI否决+降级逻辑）
- **期权链深度分析**：LongPort 真实期权链 + 希腊值 + 最优合约推荐
- **回踩狙击点**：Gist 7天记忆体 + 三维回踩锁定
- **AI 宏观策略研判**：结构化推理（简讯/情绪/核心事件/风险/期权建议）

### 卡片布局（设计中）

```
┌────────────────────────────────────────────┐
│ 🟢 VIX 19.3 · 正常偏低 · 进攻偏稳           │
│ 权重: 动量18% 估值20% 回踩15%               │
├────────────────────────────────────────────┤
│ 📊 综合评分 TOP3                            │
│ # │ 代码 │ 评分 │ 信号    │ 涨跌   │ AI  │
│───│──────│──────│─────────│────────│─────│
│ 1 │ BKNG │  62  │ ⚠️降级 │ -1.5%  │ ⭐2 │
│ 2 │ AMD  │  60  │ ⚠️降级 │ +0.6%  │ ⭐3 │
│ 3 │ BRK-B│  60  │ ✅买入  │ +1.1%  │ ⭐4 │
├────────────────────────────────────────────┤
│ 🔄 均值回归                                 │
│ UBER 67 -1.2% · BKNG 64 -1.5%             │
├────────────────────────────────────────────┤
│ 🚀 异动扫描                                 │
│ TSLA-3.6% ORCL-6.0% TXN+19.4%            │
├────────────────────────────────────────────┤
│ 📰 要闻 · 💻 科技 · 🌍 国际                 │
│ (点击展开)                                  │
├────────────────────────────────────────────┤
│ 🎯 期权建议                                 │
│ BRK-B Bull Call Spread $455C/$485C        │
├────────────────────────────────────────────┤
│ 🤖 AI策略研判                               │
│ (点击展开)                                  │
├────────────────────────────────────────────┤
│ 🐦 Joely最新动态                            │
│ (点击展开)                                  │
├────────────────────────────────────────────┤
│ ⏰ 2026-05-04 18:28 · 数据仅供参考          │
└────────────────────────────────────────────┘
```

---

## 不需要的功能

不需要 Vercel，不需要网页，不需要服务器，不需要开VPN。
GitHub Actions 在美国服务器运行，直接访问 Twelve Data 和 Finnhub，没有任何封锁问题。
