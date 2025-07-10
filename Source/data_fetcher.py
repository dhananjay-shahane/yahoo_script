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
    """Data fetching operations from Yahoo Finance"""

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

    def fetch_data_by_period(self, symbol, interval, last_datetime=None):
        """
        Fetch market data for a symbol with specified interval
        
        Args:
            symbol: Stock symbol
            interval: Data interval (1m, 5m, 1h, 1d, etc.)
            last_datetime: Last datetime in database to fetch only newer data
            
        Returns:
            Processed DataFrame with IST timestamps and proper ordering
        """
        try:
            yahoo_symbol = self.market_utils.get_yahoo_symbol(symbol)
            if not yahoo_symbol:
                print(f"❌ Invalid symbol: {symbol}")
                return pd.DataFrame()

            # Determine the period based on interval
            if interval in ['1m', '5m']:
                period = '7d'  # 7 days for intraday data
            elif interval == '1h':
                period = '1mo'  # 1 month for hourly data
            else:
                period = '3mo'  # 3 months for daily data

            print(f"🔄 Fetching {interval} data for {yahoo_symbol} (period: {period})")
            
            ticker = yf.Ticker(yahoo_symbol)
            new_data = ticker.history(period=period, interval=interval)
            
            if new_data.empty:
                print(f"❌ No data available for {yahoo_symbol}")
                return pd.DataFrame()

            # Keep only OHLCV data
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
            print(f"❌ Error fetching data for {symbol}: {e}")
            return pd.DataFrame()


class DataFetcher:
    """Handles data fetching from Yahoo Finance"""
    
    def __init__(self):
        self.market_utils = MarketUtils()
    
    def fetch_data_by_period(self, symbol, interval='5m', last_datetime=None):
        """Fetch data for a symbol with specified interval"""
        yahoo_symbol = self.market_utils.get_yahoo_symbol(symbol)
        
        if not yahoo_symbol:
            print(f"❌ Cannot fetch data for invalid symbol: {symbol}")
            return pd.DataFrame()
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print(f"📡 Attempt {retry_count + 1}/{max_retries} fetching data for {yahoo_symbol}")
                
                # Determine the period based on interval
                if interval == '5m':
                    period = '7d'  # Get last 7 days of 5m data
                elif interval == '1d':
                    period = '1mo'  # Get last month of daily data
                else:
                    period = '1mo'
                
                ticker = yf.Ticker(yahoo_symbol)
                
                if last_datetime:
                    # Fetch from last datetime to now
                    start_date = last_datetime.date()
                    end_date = datetime.now(INDIA_TZ).date() + timedelta(days=1)
                    data = ticker.history(start=start_date, end=end_date, interval=interval)
                else:
                    # Fetch initial data
                    data = ticker.history(period=period, interval=interval)
                
                if not data.empty:
                    # Clean and prepare data
                    data = data[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
                    data.reset_index(inplace=True)
                    
                    # Filter out data before last_datetime if specified
                    if last_datetime:
                        data = data[data['Datetime'] > last_datetime]
                    
                    if not data.empty:
                        data.rename(columns={'Datetime': 'datetime'}, inplace=True)
                        print(f"✅ Successfully fetched {len(data)} records for {yahoo_symbol}")
                        return data
                    else:
                        print(f"ℹ️  No new data available for {yahoo_symbol}")
                        return pd.DataFrame()
                else:
                    print(f"⚠️  No data returned for {yahoo_symbol} on attempt {retry_count + 1}")
                    
            except Exception as e:
                print(f"❌ Attempt {retry_count + 1} failed for {yahoo_symbol}: {e}")
            
            retry_count += 1
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        print(f"❌ Failed to fetch data for {yahoo_symbol} after {max_retries} attempts")
        return pd.DataFrame()


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
        """
        Fetch market data for a symbol based on time period
        
        Args:
            symbol: Stock symbol
            time_period: Data granularity ('5m' for 5-minute candles, '1d' for daily)
            last_datetime: Last datetime in database to fetch only newer data
        """
        try:
            yahoo_symbol = self.market_utils.get_yahoo_symbol(symbol)
            if not yahoo_symbol:
                print(f"❌ Unable to find valid Yahoo symbol for {symbol}")
                return pd.DataFrame()

            # Rate limiting: Add delay to avoid hitting API limits
            config = DATA_CONFIG.get('5M' if time_period == '5m' else 'DAILY', {})
            min_throttle = config.get('min_throttle_seconds', 60)
            
            # Check throttling for this symbol
            current_time = time.time()
            if symbol in self.last_request_time:
                time_since_last = current_time - self.last_request_time[symbol]
                if time_since_last < min_throttle:
                    print(f"⏸️  Throttling {symbol}: Last request {time_since_last:.1f}s ago (min: {min_throttle}s)")
                    return pd.DataFrame()

            # Update last request time
            self.last_request_time[symbol] = current_time
            
            # Add base delay between requests
            time.sleep(3)

            stock = yf.Ticker(yahoo_symbol)
            print(f"📡 Fetching {time_period} data for {yahoo_symbol} (original: {symbol})")

            # Only fetch 5-minute data during market hours
            if time_period == '5m':
                if not self.market_utils.is_market_open():
                    print(f"🔒 Market closed - skipping 5-minute data for {yahoo_symbol}")
                    return pd.DataFrame()

                # Fetch 5-minute data during market hours
                if last_datetime:
                    start_date = last_datetime
                    end_date = datetime.now(INDIA_TZ)
                else:
                    end_date = datetime.now(INDIA_TZ)
                    start_date = end_date - timedelta(minutes=30)  # Last 30 minutes

                print(f"📊 Fetching 5-minute candles from {start_date.strftime('%H:%M')} to {end_date.strftime('%H:%M')}")

                # Fetch with retry logic
                new_data = self._fetch_with_retry(stock, start_date, end_date, '5m', yahoo_symbol)

            elif time_period == '1d':
                # Fetch daily data (less frequent, can be done anytime)
                if last_datetime:
                    start_date = last_datetime.date()
                    end_date = datetime.now(INDIA_TZ).date()
                else:
                    end_date = datetime.now(INDIA_TZ).date()
                    start_date = end_date - timedelta(days=7)  # Last 7 days

                print(f"📊 Fetching daily candles from {start_date} to {end_date}")

                # Fetch with retry logic
                new_data = self._fetch_with_retry(stock, start_date, end_date, '1d', yahoo_symbol)

            else:
                print(f"❌ Unsupported time_period: {time_period}")
                return pd.DataFrame()

            if new_data.empty:
                print(f"ℹ️  No new data available for {yahoo_symbol}")
                return pd.DataFrame()

            # Process and clean data
            new_data = new_data[['Open', 'High', 'Low', 'Close', 'Volume']]
            new_data.index.name = 'datetime'
            new_data.reset_index(inplace=True)

            # Filter to only include data after last_datetime if provided
            if last_datetime:
                new_data = new_data[new_data['datetime'] > last_datetime]

            # Process the market data (convert to IST and reverse order)
            new_data = self.process_market_data(new_data)

            print(f"✅ Processed {len(new_data)} records for {symbol}")
            return new_data

        except Exception as e:
            print(f"❌ Error fetching {time_period} data for {symbol}: {e}")
            return pd.DataFrame()

    def _fetch_with_retry(self, stock, start_date, end_date, interval, symbol):
        """Fetch data with retry logic and rate limit handling"""
        max_retries = 3
        retry_delays = [5, 10, 15]  # Progressive delays
        
        for attempt in range(max_retries):
            try:
                print(f"   📡 Attempt {attempt + 1}/{max_retries} for {symbol}")
                
                data = stock.history(start=start_date, end=end_date, interval=interval)
                
                if not data.empty:
                    print(f"   ✅ Fetched {len(data)} records")
                    return data
                else:
                    print(f"   ⚠️  No data returned on attempt {attempt + 1}")
                    
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    print(f"   ❌ Rate limit hit on attempt {attempt + 1}")
                    wait_time = 20 + (attempt * 10)  # Longer wait for rate limits
                else:
                    print(f"   ❌ Attempt {attempt + 1} failed: {e}")
                    wait_time = retry_delays[attempt] if attempt < len(retry_delays) else 15

                if attempt < max_retries - 1:
                    print(f"   ⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)

        print(f"   ❌ Failed to fetch data after {max_retries} attempts")
        return pd.DataFrame()