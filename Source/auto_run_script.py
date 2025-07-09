
from datetime import datetime, timedelta
from utils import create_db_connection, DB_URL, get_all_symbol_tables, get_tables_by_time_period
from yahoo_finanace import fetch_data_by_period, save_data_to_db, display_latest_data, get_yahoo_symbol, is_market_open, get_last_datetime
from psycopg2 import Error
import pandas as pd 
import yfinance as yf
import pytz
import time
import sqlalchemy

def update_tables_by_period(time_period='5M'):
    """Update all tables for a specific time period"""
    tables = get_tables_by_period(time_period)
    
    if not tables:
        print(f"No tables found for time period {time_period}")
        return
    
    print(f"\nUpdating {len(tables)} tables for time period {time_period}")
    
    for table_name in tables:
        # Extract symbol from table name (remove _5M or _DAILY suffix)
        if table_name.endswith('_5M'):
            symbol = table_name[:-3]
            interval = '5m'
        elif table_name.endswith('_DAILY'):
            symbol = table_name[:-6]
            interval = '1d'
        else:
            continue
            
        print(f"\nProcessing {symbol} for {time_period} data...")
        
        # Skip 5-minute updates if market is closed
        if time_period == '5M' and not is_market_open():
            print(f"Market closed - skipping 5-minute data for {symbol}")
            continue
        
        full_table_name = f"symbols.{table_name}"
        last_datetime = get_last_datetime(full_table_name)
        
        print(f"Last datetime for {table_name}: {last_datetime}")
        
        new_data = fetch_data_by_period(symbol, interval, last_datetime)
        
        if not new_data.empty:
            save_data_to_db(full_table_name, new_data)
            display_latest_data(full_table_name, symbol, 3)
        else:
            print(f"No new data for {symbol}")

def run_continuous_updates(fetching_interval_5m=300, fetching_interval_daily=3600):
    """Run continuous updates for both 5-minute and daily data"""
    last_daily_update = datetime.min
    
    try:
        print("Starting continuous data updates...")
        print(f"5-minute data fetching interval: {fetching_interval_5m} seconds")
        print(f"Daily data fetching interval: {fetching_interval_daily} seconds")
        print(f"Market hours: 09:00 - 15:30 IST (Mon-Fri)")
        
        while True:
            current_time = datetime.now(pytz.timezone("Asia/Kolkata"))
            print(f"\n--- Update cycle at {current_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            # Update 5-minute data (only during market hours)
            update_tables_by_period('5M')
            
            # Update daily data (less frequently)
            if (current_time - last_daily_update).total_seconds() >= fetching_interval_daily:
                update_tables_by_period('DAILY')
                last_daily_update = current_time
            
            print(f"\nWaiting {fetching_interval_5m} seconds until next 5-minute update...")
            time.sleep(fetching_interval_5m)
            
    except KeyboardInterrupt:
        print("\nStopping continuous updates...")

def update_all_tables():
    """Update all existing tables once"""
    print("Updating all 5-minute tables...")
    update_tables_by_period('5M')
    
    print("\nUpdating all daily tables...")
    update_tables_by_period('DAILY')

if __name__ == "__main__":
    print("Stock Data Auto-Update Script")
    print("1. Run once (update all existing tables)")
    print("2. Run continuously")
    
    choice = input("Enter your choice (1 or 2): ").strip()
    
    if choice == '1':
        update_all_tables()
    elif choice == '2':
        # Get custom intervals
        interval_5m = int(input("Enter 5-minute data fetching interval in seconds (default 300): ") or 300)
        interval_daily = int(input("Enter daily data fetching interval in seconds (default 3600): ") or 3600)
        run_continuous_updates(interval_5m, interval_daily)
    else:
        print("Invalid choice. Running once...")
        update_all_tables()
