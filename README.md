# ⚡ Quant Alpha · 飞书推送版

不需要网页、不需要服务器，每天4次自动推送美股量化分析到飞书群。
用 GitHub Actions 免费运行，零成本。

---

## 推送时间（北京时间，工作日）

| 推送 | 时间 | 内容 |
|------|------|------|
| 🌙 开盘预热 | **21:47** | VIX + TOP10评分 + 期权策略 |
| 🔔 开盘确认 | **22:47** | 开盘方向 + 异动提醒 |
| 🌙 半场复盘 | **02:17** | 半场总结 + 异动股 |
| 🏁 收盘总结 | **05:47** | 今日复盘 + 强买信号 |

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
- **VIX 恐慌指数** + 涨跌幅 + 市场情绪判断
- **当前因子权重**（随VIX自动调整）
- **TOP10 评分排名**：评分、信号、期权策略、仓位建议
- **异动股提醒**（涨跌>3%）
- **宏观新闻**：市场要闻、商业财经、国际政经、科技动态
- **个股深度新闻**：Scrapling 抓取全网新闻/公告/机构评论（≥20条），去重过滤
- **期权合约建议**（基础版：评分+VIX 查表）
- **期权链深度分析**：LongPort 真实期权链 + 希腊值 + 流动性筛选 + 最优合约推荐 + 盈亏分析
- **强买信号列表**（评分>72的标的）
- **AI 策略研判**：整合 Scrapling 新闻 + 期权分析的结构化推理（简讯/情绪/核心事件/风险/期权建议）

---

## 不需要的功能

不需要 Vercel，不需要网页，不需要服务器，不需要开VPN。
GitHub Actions 在美国服务器运行，直接访问 Twelve Data 和 Finnhub，没有任何封锁问题。
