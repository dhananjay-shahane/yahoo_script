
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

        # For Indian stocks, try .NS suffix first (most common)
        indian_symbol = f"{symbol}.NS"
        
        # Simple validation without fetching data to avoid API issues
        # Return the most likely format for Indian stocks
        if symbol in ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK', 'ITC', 'LT', 'SBIN', 'BHARTIARTL', 'ASIANPAINT']:
            return indian_symbol
        
        # For other symbols, try a quick validation with minimal data request
        try:
            import yfinance as yf
            import time
            
            # Add a small delay to avoid rate limiting
            time.sleep(0.1)
            
            # Try with .NS (Indian symbol) - use minimal request
            ticker = yf.Ticker(indian_symbol)
            info = ticker.info
            if info and 'symbol' in info:
                return indian_symbol
        except Exception as e:
            print(f"Info check failed for {indian_symbol}: {e}")
            pass

        # Try raw symbol (international) with minimal request
        try:
            import yfinance as yf
            import time
            
            time.sleep(0.1)
            ticker = yf.Ticker(symbol)
            info = ticker.info
            if info and 'symbol' in info:
                return symbol
        except Exception as e:
            print(f"Info check failed for {symbol}: {e}")
            pass

        # If validation fails, still return the most likely format
        # For Indian symbols, default to .NS format
        print(f"[Warning] Could not validate symbol '{symbol}', defaulting to {indian_symbol}")
        return indian_symbol
