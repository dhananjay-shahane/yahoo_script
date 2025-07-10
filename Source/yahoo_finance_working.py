
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import time
from psycopg2 import Error
import pytz
from sqlalchemy import create_engine
from database import DatabaseManager

# Mapping for Indian indices to their Yahoo Finance symbols
INDIAN_INDICES = {
    'NSEI': '^NSEI',  # Nifty 50
    'BSESN': '^BSESN',  # Sensex
    'NSEBANK': '^NSEBANK'  # Nifty Bank
}

class WorkingYahooFinance:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.india_tz = pytz.timezone("Asia/Kolkata")

    def get_yahoo_symbol(self, symbol):
        """Get the correct Yahoo Finance symbol for a given stock symbol"""
        symbol = symbol.strip().upper()

        # Step 1: Check if it's a known Indian index
        if symbol in INDIAN_INDICES:
            return INDIAN_INDICES[symbol]

        # Step 2: If already looks valid (like AAPL, ^NSEI, TSLA.BA), return as-is
        if '.' in symbol or symbol.startswith("^"):
            return symbol

        # Step 3: Try with .NS (Indian symbol) - but don't validate with API call
        indian_symbol = f"{symbol}.NS"
        
        # For known Indian stocks, prefer .NS suffix
        known_indian_stocks = ['INFY', 'TCS', 'RELIANCE', 'HDFCBANK', 'ICICIBANK', 'WIPRO', 'BHARTIARTL', 'SBIN', 'ITC', 'KOTAKBANK']
        if symbol in known_indian_stocks:
            return indian_symbol

        # Step 4: For international stocks, try raw symbol first
        # Common international stocks
        known_international = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NVDA']
        if symbol in known_international:
            return symbol

        # Step 5: Default to Indian symbol for unknown symbols
        return indian_symbol

    def is_market_open(self):
        """Check if Indian stock market is open now (Mon–Fri, 9:15–15:30 IST)"""
        now = datetime.now(self.india_tz)
        market_open = datetime.strptime("09:15", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()
        
        return now.weekday() < 5 and market_open <= now.time() <= market_close

    def fetch_new_data(self, symbol, minutes_back=5, interval='5m'):
        """Fetch recent market data for a symbol with fallback if market is closed"""
        try:
            yahoo_symbol = self.get_yahoo_symbol(symbol)
            print(f"📊 Using Yahoo symbol: {yahoo_symbol}")

            if interval == '5m' and self.is_market_open():
                # Fetch last minutes_back minutes of 5-minute data
                end_date = datetime.now(self.india_tz)
                start_date = end_date - timedelta(minutes=minutes_back)
                print(f"Fetching 5-minute data for {yahoo_symbol} from {start_date} to {end_date}")
                stock = yf.Ticker(yahoo_symbol)
                new_data = stock.history(start=start_date, end=end_date, interval='5m')
            else:
                # Market is closed or interval is not '5m', fallback to daily data
                print(f"Fetching daily data for {yahoo_symbol}")
                stock = yf.Ticker(yahoo_symbol)
                new_data = stock.history(period='5d', interval='1d')

            if new_data.empty:
                print(f"No data found for {yahoo_symbol}")
                return pd.DataFrame()

            new_data = new_data[['Open', 'High', 'Low', 'Close', 'Volume']]
            new_data.index.name = 'datetime'
            new_data.reset_index(inplace=True)
            return new_data

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()

    def create_symbol_tables(self, symbol):
        """Create both 5M and DAILY tables for a symbol"""
        symbol = symbol.upper().strip()
        
        print(f"📋 Creating tables for {symbol}...")
        table_5m = self.db_manager.check_or_create_symbol_table(f"{symbol}_5M")
        table_daily = self.db_manager.check_or_create_symbol_table(f"{symbol}_DAILY")
        
        if table_5m and table_daily:
            print(f"✅ Successfully created tables for {symbol}")
            print(f"   • 5-minute table: {table_5m}")
            print(f"   • Daily table: {table_daily}")
            return True
        else:
            print(f"❌ Failed to create tables for {symbol}")
            return False

    def update_symbol_data(self, symbol, table_type='5M'):
        """Update data for a specific symbol"""
        symbol = symbol.upper().strip()
        
        if table_type == '5M':
            table_name = f"SYMBOLS.{symbol}_5M"
            interval = '5m'
        elif table_type == 'DAILY':
            table_name = f"SYMBOLS.{symbol}_DAILY"
            interval = '1d'
        else:
            print(f"❌ Invalid table type: {table_type}")
            return False

        print(f"🔄 Updating {symbol} ({table_type})")
        
        # Fetch new data
        new_data = self.fetch_new_data(symbol, minutes_back=15, interval=interval)
        
        if not new_data.empty:
            rows_inserted = self.db_manager.save_data_to_db(table_name, new_data)
            if rows_inserted > 0:
                print(f"💾 Saved {rows_inserted} new records to {table_name}")
            
            # Display latest data
            self.db_manager.display_latest_data(table_name, symbol, 5)
            return True
        else:
            print(f"ℹ️  No new data available for {symbol}")
            return False

    def add_single_symbol(self, symbol):
        """Add a single symbol with both 5M and DAILY tables"""
        symbol = symbol.upper().strip()
        
        print(f"➕ Adding new symbol: {symbol}")
        
        # Create tables first
        if not self.create_symbol_tables(symbol):
            return False
            
        # Try to fetch initial data
        print(f"📊 Fetching initial data for {symbol}...")
        
        # Fetch daily data
        daily_success = self.update_symbol_data(symbol, 'DAILY')
        
        # Fetch 5M data only if market is open
        if self.is_market_open():
            m5_success = self.update_symbol_data(symbol, '5M')
        else:
            print(f"🔒 Market closed - skipping 5-minute data fetch")
            m5_success = True
        
        if daily_success or m5_success:
            print(f"✅ Successfully added {symbol} to the system")
        else:
            print(f"✅ Tables created for {symbol} (data will be fetched when available)")
        
        return True

    def add_multiple_symbols(self, symbols):
        """Add multiple symbols to the system"""
        symbols = [symbol.upper().strip() for symbol in symbols]
        print(f"\n🔄 Processing {len(symbols)} symbols...")
        print("=" * 60)
        
        successful_adds = []
        failed_adds = []
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] Processing symbol: {symbol}")
            print("-" * 40)
            
            try:
                # Add delay between symbols to avoid rate limiting
                if i > 1:
                    print("⏳ Waiting 3 seconds before processing next symbol...")
                    time.sleep(3)
                
                if self.add_single_symbol(symbol):
                    successful_adds.append(symbol)
                    print(f"✅ Successfully processed {symbol}")
                else:
                    failed_adds.append(symbol)
                    print(f"❌ Failed to process {symbol}")
                    
            except Exception as e:
                print(f"❌ Error processing {symbol}: {e}")
                failed_adds.append(symbol)
            
            # Progress indicator
            progress = (i / len(symbols)) * 100
            print(f"📊 Progress: {i}/{len(symbols)} ({progress:.1f}%)")
        
        # Summary
        print("\n" + "=" * 60)
        print("MULTIPLE SYMBOL ADDITION SUMMARY")
        print("=" * 60)
        
        if successful_adds:
            print(f"✅ Successfully added {len(successful_adds)} symbols:")
            for symbol in successful_adds:
                print(f"   • {symbol} (tables: {symbol}_5M, {symbol}_DAILY)")
        
        if failed_adds:
            print(f"\n❌ Failed to add {len(failed_adds)} symbols:")
            for symbol in failed_adds:
                print(f"   • {symbol}")
        
        print(f"\n📊 Total: {len(successful_adds)} successful, {len(failed_adds)} failed")
        
        return successful_adds, failed_adds

# Example usage
if __name__ == "__main__":
    yahoo_finance = WorkingYahooFinance()
    
    # Add single symbol
    symbol = input("Enter stock symbol (e.g., INFY, RELIANCE, AAPL): ").strip()
    if symbol:
        yahoo_finance.add_single_symbol(symbol)
    
    # Or add multiple symbols
    # symbols = ['INFY', 'TCS', 'RELIANCE', 'HDFCBANK']
    # yahoo_finance.add_multiple_symbols(symbols)
