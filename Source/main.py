
"""
Main entry point for the stock data collection system
"""

from stock_data_manager import StockDataManager


def main():
    """Main function"""
    manager = StockDataManager()
    
    print("=== Stock Data Collection System ===")
    print("1. Run once (update all existing tables)")
    print("2. Run continuously")
    print("3. Add new symbol")
    print("4. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            manager.update_all_tables()
            break
            
        elif choice == '2':
            # Get custom intervals
            try:
                interval_5m = int(input("Enter 5-minute data fetching interval in seconds (default 300): ") or 300)
                interval_daily = int(input("Enter daily data fetching interval in seconds (default 3600): ") or 3600)
                manager.run_continuous_updates(interval_5m, interval_daily)
            except ValueError:
                print("Invalid input. Using default intervals.")
                manager.run_continuous_updates()
            break
            
        elif choice == '3':
            symbol = input("Enter symbol to add (e.g., RELIANCE, AAPL): ").strip().upper()
            if symbol:
                manager.add_new_symbol(symbol)
            else:
                print("Invalid symbol.")
                
        elif choice == '4':
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please enter 1, 2, 3, or 4.")


if __name__ == "__main__":
    main()
