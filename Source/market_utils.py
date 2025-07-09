
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
    
    @staticmethod
    def get_yahoo_symbol(symbol):
        """Convert symbol to Yahoo Finance format"""
        symbol = symbol.strip().upper()

        # Check if it's a known Indian index
        if symbol in INDIAN_INDICES:
            return INDIAN_INDICES[symbol]

        # If already looks valid (like AAPL, ^NSEI, TSLA.BA), return as-is
        if '.' in symbol or symbol.startswith("^"):
            return symbol

        # Try with .NS (Indian symbol)
        indian_symbol = f"{symbol}.NS"
        try:
            import yfinance as yf
            if not yf.Ticker(indian_symbol).history(period="1d").empty:
                return indian_symbol
        except:
            pass

        # Try raw symbol (international)
        try:
            import yfinance as yf
            if not yf.Ticker(symbol).history(period="1d").empty:
                return symbol
        except:
            pass

        # Could not find valid symbol
        print(f"[Error] Symbol '{symbol}' is invalid or not listed on Yahoo Finance.")
        return None
