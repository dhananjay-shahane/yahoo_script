from datetime import datetime, timedelta
from utils import create_db_connection, DB_URL
from yahoo_finanace import fetch_new_data, save_data_to_db, display_latest_data, get_yahoo_symbol, is_market_open
from psycopg2 import Error
from utils import get_all_symbol_tables
import pandas as pd 
import yfinance as yf
import pytz
import datetime
import time
import sqlalchemy


def fetch_new_data(symbol, last_datetime=None, interval='5m'):
    """Fetch recent market data for a symbol with fallback if market is closed"""
    try:
        yahoo_symbol = get_yahoo_symbol(symbol)
        india_tz = pytz.timezone("Asia/Kolkata")

        if interval == '5m' and is_market_open():
            # If we have last datetime, fetch data from that point forward
            if last_datetime:
                start_date = last_datetime
                end_date = datetime.now(india_tz)
            else:
                # No last datetime, fetch last 15 minutes
                end_date = datetime.now(india_tz)
                start_date = end_date - timedelta(minutes=15)
            
            print(f"Fetching 5-minute data for {yahoo_symbol} from {start_date} to {end_date}")
            stock = yf.Ticker(yahoo_symbol)
            new_data = stock.history(start=start_date, end=end_date, interval='5m')
        else:
            # Market is closed or interval is not '5m', fallback to 1-day data
            print(f"Market is closed or not using 5m. Fetching latest 1-day data for {yahoo_symbol}")
            stock = yf.Ticker(yahoo_symbol)
            new_data = stock.history(period='1d', interval='1d')

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

def get_last_datetime_for_tables():
    """Get the last datetime for all tables in symbols schema"""
    tables = get_all_symbol_tables()
    last_datetimes = {}
    
    for table in tables:
        conn = create_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT MAX(datetime) FROM symbols.{table}")
                result = cur.fetchone()
                last_datetimes[table] = result[0] if result[0] else None
            except Exception as e:
                print(f"Error getting last datetime for {table}: {e}")
                last_datetimes[table] = None
            finally:
                conn.close()
    
    return last_datetimes

def update_all_tables():
    """Update all tables in symbols schema with new data"""
    last_datetimes = get_last_datetime_for_tables()
    
    for table_name in last_datetimes.keys():
        symbol = table_name.upper()  # Assuming table name is the symbol in uppercase
        last_datetime = last_datetimes[table_name]
        
        print(f"\nProcessing {symbol} (last datetime: {last_datetime})")
        
        new_data = fetch_new_data(symbol, last_datetime)
        save_data_to_db(table_name, new_data)
        display_latest_data(table_name, symbol)

update_all_tables()