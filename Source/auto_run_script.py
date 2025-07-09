
"""
Enhanced auto run script for stock data collection system
Optimized for continuous data collection with better error handling
"""

from stock_data_manager import StockDataManager
import time


def main():
    """Main function for auto run script"""
    manager = StockDataManager()

    print("🚀 Stock Data Auto-Update Script")
    print("=" * 50)
    print("1. Run once (update all existing tables)")
    print("2. Run continuously (recommended) ⭐")
    print("3. Add new symbol")
    print("4. Exit")

    choice = input("\nEnter your choice (1, 2, 3, or 4) [default: 2]: ").strip()
    
    # Default to continuous mode if no choice is made
    if not choice:
        choice = '2'

    if choice == '1':
        print("\n🔄 Running single update for all tables...")
        manager.update_all_tables()
        
    elif choice == '2':
        print("\n🔄 Setting up continuous updates...")
        
        # Get custom intervals with reasonable defaults
        try:
            print("\nConfiguration:")
            interval_5m = input("5-minute data interval in seconds [300]: ").strip()
            interval_5m = int(interval_5m) if interval_5m else 300
            
            interval_daily = input("Daily data interval in seconds [3600]: ").strip()
            interval_daily = int(interval_daily) if interval_daily else 3600
            
            print(f"\n✅ Starting continuous updates:")
            print(f"   • 5-minute data: every {interval_5m} seconds ({interval_5m//60} minutes)")
            print(f"   • Daily data: every {interval_daily} seconds ({interval_daily//60} minutes)")
            print("\n⚠️  Press Ctrl+C to stop\n")
            
            time.sleep(2)  # Give user time to read
            manager.run_continuous_updates(interval_5m, interval_daily)
            
        except ValueError:
            print("❌ Invalid input. Using default intervals (5min=300s, daily=3600s).")
            manager.run_continuous_updates()
        except KeyboardInterrupt:
            print("\n\n🛑 Stopped by user")
            
    elif choice == '3':
        symbol = input("Enter symbol to add (e.g., RELIANCE, AAPL, NSEI): ").strip().upper()
        if symbol:
            print(f"\n➕ Adding new symbol: {symbol}")
            manager.add_new_symbol(symbol)
        else:
            print("❌ Invalid symbol.")
            
    elif choice == '4':
        print("👋 Exiting...")
        
    else:
        print("❌ Invalid choice. Running single update...")
        manager.update_all_tables()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Program interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("💡 Please check your configuration and try again")
