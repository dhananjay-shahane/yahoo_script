"""
Data fetching operations from Yahoo Finance
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
from config import INDIA_TZ, DATA_CONFIG
from market_utils import MarketUtils


class DataFetcher:
    """Handles data fetching from Yahoo Finance"""

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

        # Reverse row order so most recent is last
        df = df.iloc[::-1].reset_index(drop=True)

        return df

    def __init__(self):
        self.market_utils = MarketUtils()
        self.last_request_time = {}  # Track last request time per symbol for throttling

    def fetch_data_by_period(self, symbol, time_period='5m', last_datetime=None):
        """Fetch market data for a symbol based on time period"""
        try:
            yahoo_symbol = self.market_utils.get_yahoo_symbol(symbol)
            if not yahoo_symbol:
                print(f"Unable to find valid Yahoo symbol for {symbol}")
                return pd.DataFrame()

            # Add delay to avoid rate limiting
            time.sleep(1)  # Increased delay

            stock = yf.Ticker(yahoo_symbol)
            print(f"Fetching data for {yahoo_symbol} (original: {symbol})")

            if time_period == '5m':
                if self.market_utils.is_market_open():
                    # Check if we need to throttle requests (minimum 60 seconds between requests per symbol)
                    current_time = time.time()
                    if symbol in self.last_request_time:
                        time_since_last = current_time - self.last_request_time[symbol]
                        if time_since_last < 60:  # 60 seconds minimum between requests
                            print(f"Throttling request for {symbol}. Last request was {time_since_last:.1f} seconds ago")
                            return pd.DataFrame()

                    # Update last request time
                    self.last_request_time[symbol] = current_time

                    # Fetch 5-minute data during market hours
                    if last_datetime:
                        start_date = last_datetime
                        end_date = datetime.now(INDIA_TZ)
                    else:
                        end_date = datetime.now(INDIA_TZ)
                        start_date = end_date - timedelta(minutes=30)

                    print(f"Fetching 5-minute data for {yahoo_symbol} from {start_date} to {end_date}")

                    # Try fetching data with retry logic
                    retry_count = 0
                    max_retries = 3
                    new_data = pd.DataFrame()

                    while retry_count < max_retries and new_data.empty:
                        try:
                            print(f"📡 Attempt {retry_count + 1}/{max_retries} for {yahoo_symbol}")
                            new_data = stock.history(start=start_date, end=end_date, interval='5m')
                            if not new_data.empty:
                                print(f"✅ Successfully fetched {len(new_data)} 5-minute records")
                                break
                            else:
                                print(f"⚠️  No data returned on attempt {retry_count + 1}")
                        except Exception as e:
                            print(f"❌ Attempt {retry_count + 1} failed: {e}")

                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 2 ** retry_count  # Exponential backoff
                            print(f"⏳ Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)

                    if new_data.empty:
                        print(f"❌ No 5-minute data found for {yahoo_symbol} after {max_retries} attempts")
                        return pd.DataFrame()
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

                # Try fetching data with retry logic
                retry_count = 0
                max_retries = 3
                new_data = pd.DataFrame()

                while retry_count < max_retries and new_data.empty:
                    try:
                        print(f"📡 Daily attempt {retry_count + 1}/{max_retries} for {yahoo_symbol}")
                        new_data = stock.history(start=start_date, end=end_date, interval='1d')
                        if not new_data.empty:
                            print(f"✅ Successfully fetched {len(new_data)} daily records")
                            break
                        else:
                            print(f"⚠️  No daily data returned on attempt {retry_count + 1}")
                    except Exception as e:
                        print(f"❌ Daily attempt {retry_count + 1} failed: {e}")

                    retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 3 * retry_count  # Linear backoff for daily data
                            print(f"⏳ Retrying daily fetch in {wait_time} seconds...")
                            time.sleep(wait_time)

                if new_data.empty:
                    print(f"❌ No daily data found for {yahoo_symbol} after {max_retries} attempts")
                    return pd.DataFrame()

            new_data = new_data[['Open', 'High', 'Low', 'Close', 'Volume']]
            new_data.index.name = 'datetime'
            new_data.reset_index(inplace=True)

            # Filter to only include data after last_datetime if provided
            if last_datetime:
                new_data = new_data[new_data['datetime'] > last_datetime]

            # Process the market data (convert to IST and reverse order)
            new_data = self.process_market_data(new_data)

            return new_data

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()