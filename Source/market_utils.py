"""
Utilities for market operations and symbol handling
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, time
import pytz
import time as time_module
from config import INDIA_TZ, MARKET_CONFIG, INDIAN_INDICES


class MarketUtils:
    """Utilities for market operations and symbol handling"""

    def __init__(self):
        pass

    def is_market_open(self):
        """Check if Indian market is currently open"""
        now = datetime.now(INDIA_TZ)
        current_time = now.time()
        current_day = now.weekday()

        # Parse market open and close times
        market_open = time.fromisoformat(MARKET_CONFIG['open_time'])
        market_close = time.fromisoformat(MARKET_CONFIG['close_time'])

        # Check if today is a working day (Monday=0 to Friday=4)
        is_working_day = current_day in MARKET_CONFIG['working_days']

        # Check if current time is within market hours
        is_market_hours = market_open <= current_time <= market_close

        return is_working_day and is_market_hours

    def _validate_symbol(self, symbol, max_retries=2):
        """Validate symbol with retry logic and rate limiting protection"""
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol)
                # Use a simple history check with minimal data to avoid rate limits
                hist = ticker.history(period="1d", interval="1d")
                if not hist.empty:
                    return True

            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    print(f"Rate limit hit for {symbol}, waiting longer...")
                    time_module.sleep(5 + (attempt * 2))  # Longer wait for rate limits
                else:
                    print(f"Validation attempt {attempt + 1} for {symbol}: {e}")
                    if attempt < max_retries - 1:
                        time_module.sleep(2)  # Standard wait
                continue
        return False

    def get_yahoo_symbol(self, symbol):
        """Convert symbol to Yahoo Finance format with basic validation"""
        symbol = symbol.strip().upper()

        # Step 1: Check if it's a known Indian index
        if symbol in INDIAN_INDICES:
            yahoo_symbol = INDIAN_INDICES[symbol]
            print(f"📊 Using mapped Indian index: {symbol} -> {yahoo_symbol}")
            return yahoo_symbol

        # Step 2: If already looks valid (like AAPL, ^NSEI, TSLA.BA), return as-is
        if '.' in symbol or symbol.startswith("^"):
            print(f"🔍 Symbol appears pre-formatted: {symbol}")
            return symbol

        # Step 3: For likely Indian stocks, use .NS format
        if symbol.isalpha() and len(symbol) <= 10:
            preferred_symbol = f"{symbol}.NS"
            print(f"🔍 Using Indian stock format: {symbol} -> {preferred_symbol}")
            return preferred_symbol

        # Step 4: For international stocks, return as-is
        print(f"🔍 Using international format: {symbol}")
        return symbol