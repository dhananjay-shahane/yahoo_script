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
        """Get the correct Yahoo Finance symbol for a given stock symbol"""
        symbol = symbol.upper().strip()

        # Check if it's a known Indian index
        if symbol in self.INDIAN_INDICES:
            return self.INDIAN_INDICES[symbol]

        # If already looks valid (like AAPL, ^NSEI, TSLA.BA), return as-is
        if '.' in symbol or symbol.startswith("^"):
            return symbol

        # Try different variations with simpler validation
        variations = [
            f"{symbol}.NS",  # NSE
            symbol,          # International
            f"{symbol}.BO"   # BSE
        ]

        for variation in variations:
            exchange = "NSE" if variation.endswith(".NS") else "BSE" if variation.endswith(".BO") else "international"
            print(f"🔍 Checking {exchange}: {variation}")
            try:
                ticker = yf.Ticker(variation)
                # Try a simple 1-day history check with minimal data
                hist = ticker.history(period="1d", interval="1d")
                if not hist.empty and len(hist) > 0:
                    print(f"✅ Found valid symbol: {variation}")
                    return variation
                else:
                    print(f"❌ No data available for {variation}")
            except Exception as e:
                print(f"❌ Error checking {variation}: {str(e)[:50]}...")

        # If all variations fail, return the most likely one for Indian stocks
        if symbol in ['INFY', 'TCS', 'RELIANCE', 'HDFCBANK', 'ICICIBANK', 'WIPRO', 'BHARTIARTL', 'SBIN', 'ITC', 'KOTAKBANK']:
            preferred_symbol = f"{symbol}.NS"
            print(f"⚠️  Using preferred Indian symbol: {preferred_symbol} (validation failed but symbol is likely correct)")
            return preferred_symbol

        print(f"❌ Symbol '{symbol}' is invalid or not available")
        return None
```