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

    def _validate_symbol(self, symbol, max_retries=3):
        """Validate symbol with retry logic"""
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol)
                # Try to get basic info first
                info = ticker.info
                if info and len(info) > 1:  # Basic validation
                    return True

                # If info fails, try history
                hist = ticker.history(period="5d")
                if not hist.empty:
                    return True

            except Exception as e:
                print(f"Validation attempt {attempt + 1} for {symbol}: {e}")
                if attempt < max_retries - 1:
                    time_module.sleep(1)  # Wait before retry
                continue
        return False

    def get_yahoo_symbol(self, symbol):
        """Convert symbol to Yahoo Finance format with validation"""
        symbol = symbol.strip().upper()

        # Check if it's a known Indian index
        if symbol in INDIAN_INDICES:
            yahoo_symbol = INDIAN_INDICES[symbol]
            if self._validate_symbol(yahoo_symbol):
                return yahoo_symbol

        # If already in correct format (has dot or starts with ^)
        if '.' in symbol or symbol.startswith("^"):
            if self._validate_symbol(symbol):
                return symbol

        # Try with .NS suffix for Indian stocks
        indian_symbol = f"{symbol}.NS"
        print(f"🔍 Checking {indian_symbol}...")
        if self._validate_symbol(indian_symbol):
            print(f"✅ Found valid symbol: {indian_symbol}")
            return indian_symbol

        # Try with .BO suffix for BSE stocks
        bse_symbol = f"{symbol}.BO"
        print(f"🔍 Checking {bse_symbol}...")
        if self._validate_symbol(bse_symbol):
            print(f"✅ Found valid symbol: {bse_symbol}")
            return bse_symbol

        # Try raw symbol for international stocks
        print(f"🔍 Checking {symbol}...")
        if self._validate_symbol(symbol):
            print(f"✅ Found valid symbol: {symbol}")
            return symbol

        print(f"❌ Invalid symbol: {symbol}")
        return None