"""
Data fetching operations from Yahoo Finance
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import pytz
from config import INDIA_TZ, DATA_CONFIG
from market_utils import MarketUtils


class DataFetcher:
    """Handles data fetching from Yahoo Finance with robust error handling"""

    def __init__(self):
        self.market_utils = MarketUtils()

    def process_market_data(self, df):
        """
        Process market data by:
        1. Converting timestamps to IST (UTC+5:30)  
        2. Reversing row order so most recent is last

        Args:
            df: DataFrame with 'datetime' column containing UTC timestamps

        Returns:
            Processed DataFrame
        """
        if df.empty:
            return df

        # Make a copy to avoid modifying original
        df = df.copy()

        # Convert to datetime if not already
        df['datetime'] = pd.to_datetime(df['datetime'])

        # Convert UTC to IST (UTC+5:30)
        df['datetime'] = df['datetime'] + timedelta(hours=5, minutes=30)

        # Reverse row order so most recent is last (bottom of table)
        df = df.iloc[::-1].reset_index(drop=True)

        return df

    def fetch_data_by_period(self, symbol, time_period='5m', last_datetime=None):
        """
        Fetch market data for a symbol with robust error handling
        Based on working yahoo_finance_working.py approach

        Args:
            symbol: Stock symbol
            time_period: Data granularity ('5m' for 5-minute candles, '1d' for daily)
            last_datetime: Last datetime in database to fetch only newer data
        """
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                yahoo_symbol = self.market_utils.get_yahoo_symbol(symbol)
                if not yahoo_symbol:
                    print(f"❌ Unable to find valid Yahoo symbol for {symbol}")
                    return pd.DataFrame()

                print(f"📡 Attempt {attempt + 1}/{max_retries}: Fetching {time_period} data for {yahoo_symbol} (original: {symbol})")

                # Create ticker object
                stock = yf.Ticker(yahoo_symbol)

                # Determine fetch parameters based on time period and market status
                if time_period == '5m':
                    if self.market_utils.is_market_open():
                        print(f"📊 Market open - fetching 5-minute data")
                        # For 5-minute data during market hours, get recent data
                        if last_datetime:
                            # Fetch from last datetime
                            end_date = datetime.now(INDIA_TZ)
                            start_date = end_date - timedelta(minutes=15)  # Get last 15 minutes
                            new_data = stock.history(start=start_date, end=end_date, interval='5m')
                        else:
                            # Initial fetch - get last 7 days
                            new_data = stock.history(period='7d', interval='5m')
                    else:
                        print(f"🔒 Market closed - skipping 5-minute data")
                        return pd.DataFrame()

                elif time_period == '1d':
                    print(f"📊 Fetching daily data")
                    if last_datetime:
                        # Fetch from last datetime to now
                        start_date = last_datetime.date()
                        end_date = datetime.now(INDIA_TZ).date() + timedelta(days=1)
                        new_data = stock.history(start=start_date, end=end_date, interval='1d')
                    else:
                        # Initial fetch - get last 3 months
                        new_data = stock.history(period='3mo', interval='1d')
                else:
                    print(f"❌ Unsupported time_period: {time_period}")
                    return pd.DataFrame()

                # Check if data is empty
                if new_data.empty:
                    print(f"⚠️  No data returned for {yahoo_symbol} on attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        print(f"❌ No data found for {yahoo_symbol} after {max_retries} attempts")
                        return pd.DataFrame()

                # Process successful data
                print(f"✅ Retrieved {len(new_data)} records for {yahoo_symbol}")

                # Clean and prepare data
                new_data = new_data[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
                new_data.index.name = 'datetime'
                new_data.reset_index(inplace=True)

                # Filter to only include data after last_datetime if provided
                if last_datetime:
                    new_data = new_data[new_data['datetime'] > last_datetime]

                # Process the market data (convert to IST and reverse order)
                new_data = self.process_market_data(new_data)

                print(f"✅ Successfully processed {len(new_data)} records for {symbol}")
                return new_data

            except Exception as e:
                error_msg = str(e)
                print(f"❌ Attempt {attempt + 1} failed for {yahoo_symbol}: {e}")

                # Check for rate limiting
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    print(f"⚠️  Rate limit detected, waiting longer...")
                    time.sleep(10 + (attempt * 5))  # Longer wait for rate limits
                elif "Expecting value" in error_msg:
                    print(f"⚠️  JSON parsing error, possibly rate limited or server issue")
                    time.sleep(5 + (attempt * 3))
                else:
                    time.sleep(retry_delay)

                retry_delay *= 2  # Exponential backoff

        print(f"❌ Failed to fetch data for {symbol} after {max_retries} attempts")
        return pd.DataFrame()