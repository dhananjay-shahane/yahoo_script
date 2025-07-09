"""
Market-related utilities and helper functions
"""

from datetime import datetime
from config import INDIA_TZ, MARKET_CONFIG, INDIAN_INDICES


class MarketUtils:
    """Utilities for market operations"""

    @staticmethod
    def is_market_open():
        """Check if Indian stock market is open now (Mon–Fri, 9:15–15:30 IST)"""
        now = datetime.now(INDIA_TZ)
        market_open = datetime.strptime(MARKET_CONFIG['open_time'], "%H:%M").time()
        market_close = datetime.strptime(MARKET_CONFIG['close_time'], "%H:%M").time()

        return (now.weekday() in MARKET_CONFIG['working_days'] and 
                market_open <= now.time() <= market_close)

    def get_yahoo_symbol(self, symbol):
        """Convert symbol to Yahoo Finance format with validation"""
        symbol = symbol.strip().upper()

        # Check if it's a known Indian index
        if symbol in INDIAN_INDICES:
            return INDIAN_INDICES[symbol]

        # If already in correct format, validate it
        if '.' in symbol or symbol.startswith("^"):
            if self.validate_symbol(symbol):
                return symbol
            return None

        # For Indian stocks, try multiple exchanges in order of preference
        suffixes_to_try = ['.NS', '.BO']  # NSE first, then BSE

        for suffix in suffixes_to_try:
            test_symbol = f"{symbol}{suffix}"
            print(f"Trying symbol: {test_symbol}")
            if self.validate_symbol(test_symbol):
                return test_symbol

        # Try raw symbol for international stocks
        print(f"Trying international symbol: {symbol}")
        if self.validate_symbol(symbol):
            return symbol

        print(f"[Error] Symbol '{symbol}' is invalid or not listed on Yahoo Finance.")
        print(f"Tried: {symbol}.NS, {symbol}.BO, {symbol}")
        return None

    @staticmethod
    def validate_symbol(symbol):
        """
        Validate a symbol by fetching its info from Yahoo Finance.
        Returns True if valid, False otherwise.
        """
        try:
            import yfinance as yf
            import time

            time.sleep(0.1)  # avoid rate limiting

            ticker = yf.Ticker(symbol)
            info = ticker.info

            if info and 'symbol' in info:
                return True
            else:
                print(f"Validation failed: No symbol info found for {symbol}")
                return False
        except Exception as e:
            print(f"Validation failed for {symbol}: {e}")
            return False