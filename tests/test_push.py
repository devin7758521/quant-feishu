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
from scripts.push import scrapling_news

class TestPush(unittest.TestCase):
    @patch('scripts.push.translate_to_cn', return_value="Translated")
    @patch('scripts.push._finhub_stock_news')
    def test_scrapling_news_fallback(self, mock_finhub, mock_translate):
        mock_finhub.return_value = [{"headline": "Fallback", "source": "Finnhub", "ticker": "MSFT"}]

        # Test case: target not met
        news = scrapling_news(["MSFT"], min_total=20)
        self.assertTrue(any(n['headline'] == "Fallback" for n in news))

if __name__ == '__main__':
    unittest.main()
