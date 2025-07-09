
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import time
import psycopg2
import pytz
from threading import Timer
import schedule
from config import DB_CONFIG, CONFIG, INDIAN_INDICES

def create_db_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def get_yahoo_symbol(symbol):
    """Convert symbol to Yahoo Finance format"""
    symbol = symbol.strip().upper()
    
    # Check if it's a known Indian index
    if symbol in INDIAN_INDICES:
        return INDIAN_INDICES[symbol]
    
    # If already in correct format
    if '.' in symbol or symbol.startswith("^"):
        return symbol
    
    # Try with .NS suffix for Indian stocks
    indian_symbol = f"{symbol}.NS"
    try:
        if not yf.Ticker(indian_symbol).history(period="1d").empty:
            return indian_symbol
    except:
        pass
    
    # Try raw symbol for international stocks
    try:
        if not yf.Ticker(symbol).history(period="1d").empty:
            return symbol
    except:
        pass
    
    print(f"[Error] Invalid symbol: {symbol}")
    return None

def is_market_open():
    """Check if Indian market is open (Mon-Fri, 9:15-15:30 IST)"""
    india_tz = pytz.timezone("Asia/Kolkata")
    now = datetime.now(india_tz)
    market_open = datetime.strptime("09:15", "%H:%M").time()
    market_close = datetime.strptime("15:30", "%H:%M").time()
    
    return now.weekday() < 5 and market_open <= now.time() <= market_close

def create_table_if_not_exists(symbol, table_type):
    """Create table if it doesn't exist in symbols schema"""
    table_name = f"{symbol}_{CONFIG[table_type]['table_suffix']}"
    conn = create_db_connection()
    
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'symbols' 
                AND table_name = '{table_name.lower()}'
            );
        """)
        exists = cur.fetchone()[0]
        
        if not exists:
            # Create the table
            cur.execute(f"""
                CREATE TABLE symbols.{table_name} (
                    datetime TIMESTAMP PRIMARY KEY,
                    open DECIMAL(15, 2),
                    high DECIMAL(15, 2),
                    low DECIMAL(15, 2),
                    close DECIMAL(15, 2),
                    volume BIGINT
                );
            """)
            conn.commit()
            print(f"Created table symbols.{table_name}")
        return True
        
    except Exception as e:
        print(f"Error creating table symbols.{table_name}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def fetch_market_data(symbol, table_type):
    """Fetch market data based on configuration"""
    config = CONFIG[table_type]
    yahoo_symbol = get_yahoo_symbol(symbol)
    
    if not yahoo_symbol:
        return pd.DataFrame()
    
    try:
        print(f"\nFetching {config['display_name']} data for {yahoo_symbol}")
        
        ticker = yf.Ticker(yahoo_symbol)
        df = ticker.history(
            period=config['request_period'],
            interval=config['data_interval']
        )
        
        if df.empty:
            print(f"No {table_type} data available for {yahoo_symbol}")
            return pd.DataFrame()
        
        # Process the data
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
        df.index.name = 'datetime'
        df.reset_index(inplace=True)
        
        return df
    
    except Exception as e:
        print(f"Error fetching {table_type} data: {e}")
        return pd.DataFrame()

def save_to_database(symbol, df, table_type):
    """Save data to the appropriate table"""
    if df.empty:
        return
    
    table_name = f"{symbol}_{CONFIG[table_type]['table_suffix']}"
    conn = create_db_connection()
    
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Get most recent record
        cur.execute(f"SELECT MAX(datetime) FROM symbols.{table_name}")
        last_dt = cur.fetchone()[0]
        
        # Filter new records
        new_data = []
        for _, row in df.iterrows():
            if last_dt is None or row['datetime'] > last_dt:
                new_data.append((
                    row['datetime'],
                    row['Open'],
                    row['High'],
                    row['Low'],
                    row['Close'],
                    row['Volume']
                ))
        
        # Insert new records
        if new_data:
            cur.executemany(f"""
                INSERT INTO symbols.{table_name} 
                (datetime, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, new_data)
            conn.commit()
            print(f"Inserted {len(new_data)} records to symbols.{table_name}")
        
    except Exception as e:
        print(f"Error saving to symbols.{table_name}: {e}")
        conn.rollback()
    finally:
        conn.close()

def display_data(symbol, table_type, limit=10):
    """Display recent data from database"""
    table_name = f"{symbol}_{CONFIG[table_type]['table_suffix']}"
    conn = create_db_connection()
    
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'symbols' 
                AND table_name = '{table_name.lower()}'
            );
        """)
        exists = cur.fetchone()[0]
        
        if not exists:
            print(f"Table symbols.{table_name} doesn't exist")
            return
        
        # Fetch data
        cur.execute(f"""
            SELECT datetime, open, high, low, close, volume
            FROM symbols.{table_name}
            ORDER BY datetime DESC
            LIMIT %s
        """, (limit,))
        
        data = cur.fetchall()
        
        if not data:
            print(f"No data in symbols.{table_name}")
            return
        
        # Format and display
        df = pd.DataFrame(data, columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['DateTime'] = pd.to_datetime(df['DateTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n{CONFIG[table_type]['display_name']} Data for {symbol}")
        print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
        
    except Exception as e:
        print(f"Error displaying data: {e}")
    finally:
        conn.close()

def job(symbol, table_type):
    """Scheduled job to fetch and store data"""
    if table_type == 'intraday' and not is_market_open():
        print("Market closed - skipping intraday update")
        return
    
    data = fetch_market_data(symbol, table_type)
    save_to_database(symbol, data, table_type)
    display_data(symbol, table_type)

def schedule_jobs(symbol):
    """Schedule periodic jobs for data collection"""
    # Initialize tables
    for table_type in ['intraday', 'daily']:
        create_table_if_not_exists(symbol, table_type)
    
    # Schedule intraday job (every 5 minutes when market is open)
    schedule.every(5).minutes.do(job, symbol, 'intraday')
    
    # Schedule daily job (once per day at 18:00 IST)
    schedule.every().day.at("18:00").do(job, symbol, 'daily')
    
    print("\nScheduled jobs:")
    print(f"- Intraday: Every 5 minutes (when market open)")
    print(f"- Daily: Daily at 18:00 IST")
    
    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    symbol = input("Enter stock symbol (e.g., RELIANCE, NSEI): ").strip().upper()
    print(f"\nStarting data collection for {symbol}...")
    
    try:
        schedule_jobs(symbol)
    except KeyboardInterrupt:
        print("\nStopping data collection...")
