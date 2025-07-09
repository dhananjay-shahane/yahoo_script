
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import time
from psycopg2 import Error
import pytz
from sqlalchemy import create_engine
from utils import DB_URL, create_db_connection, check_or_create_symbol_table

# Mapping for Indian indices to their Yahoo Finance symbols
INDIAN_INDICES = {
    'NSEI': '^NSEI',  # Nifty 50
    'BSESN': '^BSESN',  # Sensex
    'NSEBANK': '^NSEBANK'  # Nifty Bank
}

def get_yahoo_symbol(symbol):
    symbol = symbol.strip().upper()

    # Step 1: Check if it's a known Indian index
    if symbol in INDIAN_INDICES:
        return INDIAN_INDICES[symbol]

    # Step 2: If already looks valid (like AAPL, ^NSEI, TSLA.BA), return as-is
    if '.' in symbol or symbol.startswith("^"):
        return symbol

    # Step 3: Try with .NS (Indian symbol)
    indian_symbol = f"{symbol}.NS"
    try:
        if not yf.Ticker(indian_symbol).history(period="1d").empty:
            return indian_symbol
    except:
        pass

    # Step 4: Try raw symbol (international)
    try:
        if not yf.Ticker(symbol).history(period="1d").empty:
            return symbol
    except:
        pass

    # Step 5: Could not find valid symbol
    print(f"[Error] Symbol '{symbol}' is invalid or not listed on Yahoo Finance.")
    return None


def is_market_open():
    """Check if Indian stock market is open now (Mon–Fri, 9:00–15:30 IST)"""
    india_tz = pytz.timezone("Asia/Kolkata")
    now = datetime.now(india_tz)
    market_open = datetime.strptime("09:00", "%H:%M").time()
    market_close = datetime.strptime("15:30", "%H:%M").time()
    
    return now.weekday() < 5 and market_open <= now.time() <= market_close

def fetch_data_by_period(symbol, time_period='5m', last_datetime=None):
    """Fetch market data for a symbol based on time period"""
    try:
        yahoo_symbol = get_yahoo_symbol(symbol)
        if not yahoo_symbol:
            return pd.DataFrame()
            
        india_tz = pytz.timezone("Asia/Kolkata")
        stock = yf.Ticker(yahoo_symbol)

        if time_period == '5m':
            if is_market_open():
                # Fetch 5-minute data during market hours
                if last_datetime:
                    start_date = last_datetime
                    end_date = datetime.now(india_tz)
                else:
                    end_date = datetime.now(india_tz)
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
                end_date = datetime.now(india_tz).date()
            else:
                end_date = datetime.now(india_tz).date()
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

def save_data_to_db(table_name, df):
    """Save data to the symbol table, avoiding duplicates"""
    if df.empty:
        return
    
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Prepare new data to insert
            new_records = []
            for _, row in df.iterrows():
                new_records.append((
                    row['datetime'],
                    row['Open'],
                    row['High'],
                    row['Low'],
                    row['Close'],
                    row['Volume']
                ))
            
            # Insert new records with ON CONFLICT DO NOTHING to avoid duplicates
            if new_records:
                cur.executemany(f"""
                    INSERT INTO {table_name} (datetime, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (datetime) DO NOTHING
                """, new_records)
                conn.commit()
                print(f"Inserted {cur.rowcount} new records into {table_name}")
            else:
                print(f"No new records to insert for {table_name}")
            
        except Exception as e:
            print(f"Error saving data to {table_name}: {e}")
            conn.rollback()
        finally:
            conn.close()

def get_last_datetime(table_name):
    """Get the last datetime from a table"""
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(datetime) FROM {table_name}")
            result = cur.fetchone()
            return result[0] if result[0] else None
        except Exception as e:
            print(f"Error getting last datetime for {table_name}: {e}")
            return None
        finally:
            conn.close()

def display_latest_data(table_name, symbol, limit=10):
    """Display the latest data for a symbol"""
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            cur.execute(f"""
                SELECT datetime, open, high, low, close, volume
                FROM {table_name}
                ORDER BY datetime DESC
                LIMIT %s
            """, (limit,))
            
            data = cur.fetchall()
            if not data:
                print(f"No data available for {symbol}")
                return
                
            # Format and display data
            df = pd.DataFrame(data, columns=['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['Datetime'] = pd.to_datetime(df['Datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            print("\n" + "="*80)
            print(f"Latest Data for {symbol} - Table: {table_name}")
            print("="*80)
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
            print("\n" + "="*80)
            
        except Exception as e:
            print(f"Error displaying data: {e}")
        finally:
            conn.close()

def update_symbol_data(symbol):
    """Update both 5-minute and daily data for a symbol"""
    # Create tables for both time periods
    table_5m = check_or_create_symbol_table(f"{symbol}_5M")
    table_daily = check_or_create_symbol_table(f"{symbol}_DAILY")
    
    if not table_5m or not table_daily:
        print(f"Failed to initialize tables for {symbol}")
        return
    
    # Update 5-minute data (only during market hours)
    if is_market_open():
        last_datetime_5m = get_last_datetime(table_5m)
        new_data_5m = fetch_data_by_period(symbol, '5m', last_datetime_5m)
        if not new_data_5m.empty:
            save_data_to_db(table_5m, new_data_5m)
            display_latest_data(table_5m, symbol, 5)
        else:
            print(f"No new 5-minute data for {symbol}")
    else:
        print(f"Market closed - skipping 5-minute data for {symbol}")
    
    # Update daily data (can be done anytime)
    last_datetime_daily = get_last_datetime(table_daily)
    new_data_daily = fetch_data_by_period(symbol, '1d', last_datetime_daily)
    if not new_data_daily.empty:
        save_data_to_db(table_daily, new_data_daily)
        display_latest_data(table_daily, symbol, 5)
    else:
        print(f"No new daily data for {symbol}")

def main_loop(symbols, fetching_interval=300):  # 5 minutes default
    """Main data collection loop for multiple symbols"""
    try:
        print(f"Starting data collection for symbols: {symbols}")
        print(f"Fetching interval: {fetching_interval} seconds")
        print(f"Market hours: 09:00 - 15:30 IST (Mon-Fri)")
        
        while True:
            current_time = datetime.now(pytz.timezone("Asia/Kolkata"))
            print(f"\n--- Update cycle at {current_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            for symbol in symbols:
                print(f"\nProcessing {symbol}...")
                update_symbol_data(symbol)
            
            print(f"\nWaiting {fetching_interval} seconds until next update...")
            time.sleep(fetching_interval)
            
    except KeyboardInterrupt:
        print("\nStopping data collection...")

if __name__ == "__main__":
    symbols_input = input("Enter stock symbols separated by commas (e.g., RELIANCE,NSEI,AAPL): ").strip()
    symbols = [s.strip().upper() for s in symbols_input.split(',') if s.strip()]
    
    fetching_interval = int(input("Enter fetching interval in seconds (default 300 for 5 minutes): ") or 300)
    
    print(f"\nStarting data collection for {symbols} with {fetching_interval}s interval (Ctrl+C to stop)...")
    main_loop(symbols, fetching_interval)
