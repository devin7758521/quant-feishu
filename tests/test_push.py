import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Mock the modules before importing push.py
sys.modules['requests'] = MagicMock()
sys.modules['longport'] = MagicMock()
sys.modules['scrapling'] = MagicMock()
sys.modules['scrapling.fetchers'] = MagicMock()

# Add parent directory to path so scripts.push can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.push import scrapling_news, get_option_strategy

class TestPush(unittest.TestCase):
    @patch('scripts.push.translate_to_cn', return_value="Translated")
    @patch('scripts.push._finhub_stock_news')
    def test_scrapling_news_fallback(self, mock_finhub, mock_translate):
        mock_finhub.return_value = [{"headline": "Fallback", "source": "Finnhub", "ticker": "MSFT"}]

        # Test case: target not met
        news = scrapling_news(["MSFT"], min_total=20)
        self.assertTrue(any(n['headline'] == "Fallback" for n in news))


    def test_get_option_strategy(self):
        cases = [
            (70, 15, "买入 Call"),
            (50, 15, "Bull Call Spread"),
            (40, 15, "买入 Put"),
            (70, 20, "Bull Call Spread"),
            (50, 20, "Cash-Secured Put"),
            (40, 20, "Bear Put Spread"),
            (70, 25, "Cash-Secured Put"),
            (50, 25, "Iron Condor"),
            (40, 25, "Bear Put Spread"),
            (70, 30, "轮子策略(CSP+CC)"),
            (50, 30, "Wide Iron Condor"),
            (40, 30, "买入 Put"),
            (70, 35, "⚠️暂缓/Strangle卖出"),
            (50, 35, "⚠️空仓观望"),
            (40, 35, "⚠️空仓观望"),
        ]
        for score, vix, expected in cases:
            with self.subTest(score=score, vix=vix):
                self.assertEqual(get_option_strategy(score, vix), expected)

if __name__ == '__main__':
    unittest.main()
