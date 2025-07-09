
"""
Market-related utilities and helper functions
"""

from datetime import datetime, time
import yfinance as yf
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
    
    def get_yahoo_symbol(self, symbol):
        """Convert symbol to Yahoo Finance format with validation"""
        symbol = symbol.strip().upper()
        
        # Check if it's a known Indian index
        if symbol in INDIAN_INDICES:
            return INDIAN_INDICES[symbol]
        
        # If already in correct format (has dot or starts with ^)
        if '.' in symbol or symbol.startswith("^"):
            return symbol
        
        # Try with .NS suffix for Indian stocks
        indian_symbol = f"{symbol}.NS"
        if self._validate_symbol(indian_symbol):
            return indian_symbol
        
        # Try with .BO suffix for BSE stocks
        bse_symbol = f"{symbol}.BO"
        if self._validate_symbol(bse_symbol):
            return bse_symbol
        
        # Try raw symbol for international stocks
        if self._validate_symbol(symbol):
            return symbol
        
        print(f"[Error] Symbol '{symbol}' is invalid or not listed on Yahoo Finance.")
        return None
    
    def _validate_symbol(self, symbol):
        """Validate if a symbol exists on Yahoo Finance"""
        try:
            ticker = yf.Ticker(symbol)
            # Try to get recent data to validate
            data = ticker.history(period="1d")
            return not data.empty
        except Exception:
            return False
    
    def get_market_status_info(self):
        """Get detailed market status information"""
        now = datetime.now(INDIA_TZ)
        is_open = self.is_market_open()
        
        return {
            'is_open': is_open,
            'current_time': now,
            'current_day': now.strftime('%A'),
            'status': 'OPEN' if is_open else 'CLOSED'
        }
