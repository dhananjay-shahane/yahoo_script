
"""
Data fetching operations from Yahoo Finance
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from config import INDIA_TZ, DATA_CONFIG
from market_utils import MarketUtils


class DataFetcher:
    """Handles data fetching from Yahoo Finance"""
    
    def __init__(self):
        self.market_utils = MarketUtils()
    
    def fetch_data_by_period(self, symbol, time_period='5m', last_datetime=None):
        """Fetch market data for a symbol based on time period"""
        try:
            yahoo_symbol = self.market_utils.get_yahoo_symbol(symbol)
            if not yahoo_symbol:
                return pd.DataFrame()
                
            stock = yf.Ticker(yahoo_symbol)

            if time_period == '5m':
                if self.market_utils.is_market_open():
                    # Fetch 5-minute data during market hours
                    if last_datetime:
                        start_date = last_datetime
                        end_date = datetime.now(INDIA_TZ)
                    else:
                        end_date = datetime.now(INDIA_TZ)
                        start_date = end_date - timedelta(minutes=30)
                    
                    print(f"Fetching 5-minute data for {yahoo_symbol} from {start_date} to {end_date}")
                    new_data = stock.history(start=start_date, end=end_date, interval='5m')
                else:
                    print(f"Market is closed. Skipping 5-minute data fetch for {yahoo_symbol}")
                    return pd.DataFrame()
                    
            elif time_period == '1d':
                # Fetch daily data (can be done anytime)
                if last_datetime:
                    start_date = last_datetime.date()
                    end_date = datetime.now(INDIA_TZ).date()
                else:
                    end_date = datetime.now(INDIA_TZ).date()
                    start_date = end_date - timedelta(days=30)
                
                print(f"Fetching daily data for {yahoo_symbol} from {start_date} to {end_date}")
                new_data = stock.history(start=start_date, end=end_date, interval='1d')

            if new_data.empty:
                print(f"No data found for {yahoo_symbol}")
                return pd.DataFrame()

            new_data = new_data[['Open', 'High', 'Low', 'Close', 'Volume']]
            new_data.index.name = 'datetime'
            new_data.reset_index(inplace=True)
            
            # Filter to only include data after last_datetime if provided
            if last_datetime:
                new_data = new_data[new_data['datetime'] > last_datetime]
                
            return new_data

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()
