
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
    print("4. Add multiple symbols")
    print("5. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-5): ").strip()
        
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
            print("\n➕ Adding multiple symbols...")
            print("Enter symbols separated by commas (e.g., RELIANCE, AAPL, NSEI, TCS)")
            symbols_input = input("Symbols: ").strip()
            
            if symbols_input:
                # Parse the input and clean up symbols
                symbols = [s.strip().upper() for s in symbols_input.split(',') if s.strip()]
                
                if symbols:
                    print(f"\n📝 Found {len(symbols)} symbols to add: {', '.join(symbols)}")
                    manager.add_multiple_symbols(symbols)
                else:
                    print("❌ No valid symbols found.")
            else:
                print("❌ No symbols entered.")
                
        elif choice == '5':
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")


if __name__ == "__main__":
    main()
