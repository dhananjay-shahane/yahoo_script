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
    if not yf.Ticker(indian_symbol).history(period="1d").empty:
        return indian_symbol

    # Step 4: Try raw symbol (international)
    if not yf.Ticker(symbol).history(period="1d").empty:
        return symbol

    # Step 5: Could not find valid symbol
    print(f"[Error] Symbol '{symbol}' is invalid or not listed on Yahoo Finance.")
    return None


def is_market_open():
    """Check if Indian stock market is open now (Mon–Fri, 9:15–15:30 IST)"""
    india_tz = pytz.timezone("Asia/Kolkata")
    now = datetime.now(india_tz)
    market_open = datetime.strptime("09:15", "%H:%M").time()
    market_close = datetime.strptime("15:30", "%H:%M").time()
    
    return now.weekday() < 5 and market_open <= now.time() <= market_close

def fetch_new_data(symbol, minutes_back=5, interval='5m'):
    """Fetch recent market data for a symbol with fallback if market is closed"""
    try:
        yahoo_symbol = get_yahoo_symbol(symbol)
        india_tz = pytz.timezone("Asia/Kolkata")

        if interval == '5m' and is_market_open():
            # Fetch last `minutes_back` minutes of 1-minute data
            end_date = datetime.now(india_tz)
            start_date = end_date - timedelta(minutes=minutes_back)
            print(f"Fetching 1-minute data for {yahoo_symbol} from {start_date} to {end_date}")
            stock = yf.Ticker(yahoo_symbol)
            new_data = stock.history(start=start_date, end=end_date, interval='5m')
        else:
            # Market is closed or interval is not '1m', fallback to 1-day data
            print(f"Market is closed or not using 5m. Fetching latest 1-day data for {yahoo_symbol}")
            stock = yf.Ticker(yahoo_symbol)
            new_data = stock.history(period='1d', interval='1d')  # 1 days

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
def save_data_to_db(table_name, df):
    """Save data to the symbol table, avoiding duplicates"""
    if df.empty:
        return
    
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Get last datetime to only fetch newer data
            cur.execute(f"SELECT MAX(datetime) FROM {table_name}")
            last_datetime = cur.fetchone()[0]
            
            # Prepare new data to insert
            new_records = []
            for _, row in df.iterrows():
                if last_datetime is None or row['datetime'] > last_datetime:
                    new_records.append((
                        row['datetime'],
                        row['Open'],
                        row['High'],
                        row['Low'],
                        row['Close'],
                        row['Volume']
                    ))
            
            # Insert new records
            if new_records:
                cur.executemany(f"""
                    INSERT INTO {table_name} (datetime, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, new_records)
                conn.commit()
                print(f"Inserted {len(new_records)} new records into {table_name}")
            else:
                print(f"No new records to insert for {table_name}")
            
        except Exception as e:
            print(f"Error saving data to {table_name}: {e}")
            conn.rollback()
        finally:
            conn.close()

def display_latest_data(table_name, symbol, limit=1000):
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
            df['Datetime'] = pd.to_datetime(df['Datetime']).dt.strftime('%Y-%m-%d %H:%M:%S%z')
            
            print("\n" + "="*80)
            print(f"Latest Data for {symbol}")
            print("="*80)
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
            print("\n" + "="*80)
            
        except Exception as e:
            print(f"Error displaying data: {e}")
        finally:
            conn.close()

def main_loop(symbol):
    """Main data collection loop"""
    try:
        # Create or verify table for this symbol
        table_name = check_or_create_symbol_table(symbol)
        if not table_name:
            print(f"Failed to initialize table for {symbol}")
            return
        
        while True:
            # Get recent data (last 15 minutes)
            new_data = fetch_new_data(symbol, minutes_back=15)
            
            if not new_data.empty:
                save_data_to_db(table_name, new_data)
            
            # Display latest data
            display_latest_data(table_name, symbol)
            
            # Wait 1 minute before next update
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\nStopping data collection...")

if __name__ == "__main__":
    symbol = input("Enter stock symbol (e.g., AAPL, NSEI, RELIANCE): ").strip().upper()
    print(f"\nStarting data collection for {symbol} (Ctrl+C to stop)...")
    main_loop(symbol)

