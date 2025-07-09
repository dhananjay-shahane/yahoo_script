"""
Auto run script for stock data collection system
Simplified to use the new organized structure
"""

from stock_data_manager import StockDataManager


def main():
    """Main function for auto run script"""
    manager = StockDataManager()

    print("Stock Data Auto-Update Script")
    print("1. Run once (update all existing tables)")
    print("2. Run continuously")
    print("3. Add new symbol")

    choice = input("Enter your choice (1, 2, or 3): ").strip()

    if choice == '1':
        manager.update_all_tables()
    elif choice == '2':
        # Get custom intervals
        try:
            interval_5m = int(input("Enter 5-minute data fetching interval in seconds (default 300): ") or 300)
            interval_daily = int(input("Enter daily data fetching interval in seconds (default 3600): ") or 3600)
            manager.run_continuous_updates(interval_5m, interval_daily)
        except ValueError:
            print("Invalid input. Using default intervals.")
            manager.run_continuous_updates()
    elif choice == '3':
        symbol = input("Enter symbol to add (e.g., RELIANCE, AAPL, NSEI): ").strip().upper()
        if symbol:
            manager.add_new_symbol(symbol)
        else:
            print("Invalid symbol.")
    else:
        print("Invalid choice. Running once...")
        manager.update_all_tables()


if __name__ == "__main__":
    main()