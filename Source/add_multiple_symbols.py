
"""
Standalone script to add multiple symbols to the stock data collection system
"""

from stock_data_manager import StockDataManager
import sys


def add_multiple_symbols():
    """Add multiple symbols to the system"""
    manager = StockDataManager()
    
    print("=== Add Multiple Symbols ===")
    print("Enter symbols separated by commas (e.g., RELIANCE, AAPL, NSEI, TCS, INFY)")
    print("Or press Enter to use default Indian stock symbols")
    
    symbols_input = input("Symbols: ").strip()
    
    # Default symbols if none provided
    if not symbols_input:
        default_symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "BHARTIARTL", "ASIANPAINT", "MARUTI", "NSEI"]
        print(f"Using default symbols: {', '.join(default_symbols)}")
        symbols = default_symbols
    else:
        # Parse the input and clean up symbols
        symbols = [s.strip().upper() for s in symbols_input.split(',') if s.strip()]
    
    if not symbols:
        print("❌ No valid symbols found.")
        return
    
    print(f"\n📝 Found {len(symbols)} symbols to add: {', '.join(symbols)}")
    
    # Confirm before processing
    confirm = input("Do you want to proceed? (y/n): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("❌ Operation cancelled.")
        return
    
    try:
        successful, failed = manager.add_multiple_symbols(symbols)
        
        if successful:
            print(f"\n🎉 Successfully added {len(successful)} symbols to the system!")
            print("You can now run continuous monitoring to collect data for these symbols.")
        
        if failed:
            print(f"\n⚠️  {len(failed)} symbols failed to add. Please check the error messages above.")
        
    except KeyboardInterrupt:
        print("\n\n🛑 Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    add_multiple_symbols()
