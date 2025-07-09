
"""
Main stock data management system
"""

from datetime import datetime, timedelta
import time
import pytz
from config import DATA_CONFIG, INDIA_TZ
from database import DatabaseManager
from data_fetcher import DataFetcher
from market_utils import MarketUtils


class StockDataManager:
    """Main class for managing stock data collection and storage"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.data_fetcher = DataFetcher()
        self.market_utils = MarketUtils()
    
    def update_symbol_data(self, symbol, time_period='5M'):
        """Update data for a specific symbol and time period"""
        print(f"\n🔄 Processing {symbol} ({time_period})")
        
        # Determine table name and interval
        if time_period == '5M':
            table_name = f"{symbol}_5M"
            interval = '5m'
        elif time_period == 'DAILY':
            table_name = f"{symbol}_DAILY"
            interval = '1d'
        else:
            print(f"❌ Invalid time period: {time_period}")
            return
        
        # Create table if needed
        full_table_name = self.db_manager.check_or_create_symbol_table(table_name)
        if not full_table_name:
            print(f"❌ Failed to initialize table for {symbol}")
            return
        
        # Skip 5-minute updates if market is closed
        if time_period == '5M' and not self.market_utils.is_market_open():
            print(f"🔒 Market closed - skipping 5-minute data for {symbol}")
            return
        
        # Get last datetime and fetch new data
        last_datetime = self.db_manager.get_last_datetime(full_table_name)
        if last_datetime:
            print(f"📅 Last data: {last_datetime}")
        else:
            print(f"📅 No existing data - fetching initial data")
        
        new_data = self.data_fetcher.fetch_data_by_period(symbol, interval, last_datetime)
        
        if not new_data.empty:
            print(f"✅ Found {len(new_data)} new records")
            self.db_manager.save_data_to_db(full_table_name, new_data)
            self.db_manager.display_latest_data(full_table_name, symbol, 3)
        else:
            print(f"ℹ️  No new data available for {symbol}")
        
        print(f"✅ Completed {symbol} ({time_period})")
    
    def update_tables_by_period(self, time_period='5M'):
        """Update all tables for a specific time period"""
        tables = self.db_manager.get_tables_by_time_period(time_period)
        
        if not tables:
            print(f"No tables found for time period {time_period}")
            return
        
        print(f"\nUpdating {len(tables)} tables for time period {time_period}")
        
        for table_name in tables:
            # Extract symbol from table name
            if table_name.endswith('_5M'):
                symbol = table_name[:-3]
            elif table_name.endswith('_DAILY'):
                symbol = table_name[:-6]
            else:
                continue
                
            print(f"\nProcessing {symbol} for {time_period} data...")
            self.update_symbol_data(symbol, time_period)
    
    def run_continuous_updates(self, fetching_interval_5m=300, fetching_interval_daily=3600):
        """Run continuous updates for both 5-minute and daily data"""
        last_daily_update = datetime.min.replace(tzinfo=INDIA_TZ)
        cycle_count = 0
        
        try:
            print("="*80)
            print("STARTING CONTINUOUS DATA COLLECTION")
            print("="*80)
            print(f"5-minute data interval: {fetching_interval_5m} seconds ({fetching_interval_5m//60} minutes)")
            print(f"Daily data interval: {fetching_interval_daily} seconds ({fetching_interval_daily//60} minutes)")
            print(f"Market hours: 09:00 - 15:30 IST (Mon-Fri)")
            print(f"Current time: {datetime.now(INDIA_TZ).strftime('%Y-%m-%d %H:%M:%S IST')}")
            
            # Get list of tables
            tables_5m = self.db_manager.get_tables_by_time_period('5M')
            tables_daily = self.db_manager.get_tables_by_time_period('DAILY')
            print(f"Monitoring {len(tables_5m)} 5-minute tables and {len(tables_daily)} daily tables")
            print("="*80)
            
            while True:
                cycle_count += 1
                current_time = datetime.now(INDIA_TZ)
                is_market_open = self.market_utils.is_market_open()
                
                print(f"\n{'='*20} CYCLE {cycle_count} {'='*20}")
                print(f"Time: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
                print(f"Market Status: {'OPEN' if is_market_open else 'CLOSED'}")
                print("-" * 60)
                
                # Update 5-minute data (only during market hours)
                if is_market_open:
                    print("📈 Updating 5-minute data...")
                    self.update_tables_by_period('5M')
                else:
                    print("🔒 Market closed - skipping 5-minute updates")
                
                # Update daily data (less frequently)
                time_since_daily = (current_time - last_daily_update).total_seconds()
                if time_since_daily >= fetching_interval_daily:
                    print("📊 Updating daily data...")
                    self.update_tables_by_period('DAILY')
                    last_daily_update = current_time
                else:
                    remaining_daily = (fetching_interval_daily - time_since_daily) // 60
                    print(f"📊 Next daily update in {remaining_daily:.0f} minutes")
                
                print(f"\n💤 Waiting {fetching_interval_5m} seconds until next cycle...")
                print(f"⏰ Next update at: {(current_time + timedelta(seconds=fetching_interval_5m)).strftime('%H:%M:%S')}")
                time.sleep(fetching_interval_5m)
                
        except KeyboardInterrupt:
            print("\n" + "="*80)
            print("STOPPING CONTINUOUS UPDATES...")
            print(f"Completed {cycle_count} update cycles")
            print("="*80)
    
    def update_all_tables(self):
        """Update all existing tables once"""
        print("Updating all 5-minute tables...")
        self.update_tables_by_period('5M')
        
        print("\nUpdating all daily tables...")
        self.update_tables_by_period('DAILY')
    
    def add_new_symbol(self, symbol):
        """Add a new symbol to the system"""
        print(f"Adding new symbol: {symbol}")
        
        # Create tables for both time periods
        table_5m = self.db_manager.check_or_create_symbol_table(f"{symbol}_5M")
        table_daily = self.db_manager.check_or_create_symbol_table(f"{symbol}_DAILY")
        
        if table_5m and table_daily:
            print(f"Successfully added {symbol} to the system")
            # Fetch initial data
            self.update_symbol_data(symbol, '5M')
            self.update_symbol_data(symbol, 'DAILY')
        else:
            print(f"Failed to add {symbol} to the system")
