import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock modules that might not be available in the test environment
import unittest.mock as mock
mock_requests = mock.MagicMock()
sys.modules["requests"] = mock_requests
sys.modules["longport"] = mock.MagicMock()
sys.modules["longport.openapi"] = mock.MagicMock()
sys.modules["scrapling"] = mock.MagicMock()
sys.modules["scrapling.fetchers"] = mock.MagicMock()

from push import get_vix_regime, get_signal, get_option_strategy, get_position, scrapling_news
import pytest
import unittest
from unittest.mock import patch, MagicMock

@pytest.mark.parametrize("vix, expected", [
    (14.9, {"label": "极度乐观", "emoji": "🟢", "mode": "进攻"}),
    (15, {"label": "正常偏低", "emoji": "🟢", "mode": "进攻偏稳"}),
    (19.9, {"label": "正常偏低", "emoji": "🟢", "mode": "进攻偏稳"}),
    (20, {"label": "正常波动", "emoji": "🟡", "mode": "均衡"}),
    (24.9, {"label": "正常波动", "emoji": "🟡", "mode": "均衡"}),
    (25, {"label": "警戒区间", "emoji": "🟠", "mode": "偏防守"}),
    (29.9, {"label": "警戒区间", "emoji": "🟠", "mode": "偏防守"}),
    (30, {"label": "恐慌升温", "emoji": "🔴", "mode": "防守"}),
    (34.9, {"label": "恐慌升温", "emoji": "🔴", "mode": "防守"}),
    (35, {"label": "重度恐慌", "emoji": "🔴", "mode": "危机"}),
    (44.9, {"label": "重度恐慌", "emoji": "🔴", "mode": "危机"}),
    (45, {"label": "极度恐慌", "emoji": "🚨", "mode": "崩溃预警"}),
    (100, {"label": "极度恐慌", "emoji": "🚨", "mode": "崩溃预警"}),
])
def test_get_vix_regime(vix, expected):
    assert get_vix_regime(vix) == expected

@pytest.mark.parametrize("score, expected", [
    (73, "强买 🔥"),
    (72, "买入 ✅"),
    (59, "买入 ✅"),
    (58, "中性 ➖"),
    (43, "中性 ➖"),
    (42, "回避 ⚠️"),
    (0, "回避 ⚠️"),
])
def test_get_signal(score, expected):
    assert get_signal(score) == expected

@pytest.mark.parametrize("score, vix, expected", [
    # vix < 20
    (66, 15, "买入 Call"),           # score > 65
    (65, 15, "Bull Call Spread"),   # 45 < score <= 65
    (46, 15, "Bull Call Spread"),
    (45, 15, "买入 Put"),           # score <= 45
    # 20 <= vix < 25
    (66, 20, "Bull Call Spread"),
    (65, 20, "Cash-Secured Put"),
    (45, 20, "Bear Put Spread"),
    # 25 <= vix < 30
    (66, 25, "Cash-Secured Put"),
    (65, 25, "Iron Condor"),
    (45, 25, "Bear Put Spread"),
    # 30 <= vix < 35
    (66, 30, "轮子策略(CSP+CC)"),
    (65, 30, "Wide Iron Condor"),
    (45, 30, "买入 Put"),
    # vix >= 35
    (66, 35, "⚠️暂缓/Strangle卖出"),
    (65, 35, "⚠️空仓观望"),
    (45, 35, "⚠️空仓观望"),
])
def test_get_option_strategy(score, vix, expected):
    assert get_option_strategy(score, vix) == expected

@pytest.mark.parametrize("score, vix, expected", [
    # vix < 20
    (71, 15, "重仓 8-10%"),
    (70, 15, "标配 5-7%"),
    (56, 15, "标配 5-7%"),
    (55, 15, "轻仓 2-3%"),
    # 20 <= vix < 25
    (71, 20, "标配 5-7%"),
    (70, 20, "轻仓 3-5%"),
    (56, 20, "轻仓 3-5%"),
    (55, 20, "观察 1-2%"),
    # 25 <= vix < 30
    (71, 25, "轻仓 3-5%"),
    (70, 25, "小仓 2-3%"),
    (56, 25, "小仓 2-3%"),
    (55, 25, "规避"),
    # 30 <= vix < 35
    (73, 30, "小仓 1-3%"),
    (72, 30, "规避/空仓"),
    # vix >= 35
    (100, 35, "空仓/对冲"),
])
def test_get_position(score, vix, expected):
    assert get_position(score, vix) == expected


@patch('push.translate_to_cn', return_value="Translated")
@patch('push._finhub_stock_news')
def test_scrapling_news_fallback(mock_finhub, mock_translate):
    mock_finhub.return_value = [{"headline": "Fallback", "source": "Finnhub", "ticker": "MSFT"}]

    # Test case: target not met
    news = scrapling_news(["MSFT"], min_total=20)
    assert any(n['headline'] == "Fallback" for n in news)
