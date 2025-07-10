
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
    print("Note: Each symbol will get two tables: {SYMBOL}_5M and {SYMBOL}_DAILY")
    
    symbols_input = input("Symbols: ").strip()
    
    # Default symbols if none provided
    if not symbols_input:
        default_symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
        print(f"Using default symbols: {', '.join(default_symbols)}")
        symbols = default_symbols
    else:
        # Parse the input and clean up symbols
        symbols = [s.strip().upper() for s in symbols_input.split(',') if s.strip()]
    
    if not symbols:
        print("❌ No valid symbols found.")
        return
    
    print(f"\n📝 Found {len(symbols)} symbols to add: {', '.join(symbols)}")
    print("Each symbol will create:")
    for symbol in symbols:
        print(f"  • {symbol}_5M (for 5-minute data)")
        print(f"  • {symbol}_DAILY (for daily data)")
    
    # Confirm before processing
    confirm = input(f"\nDo you want to proceed with adding {len(symbols)} symbols? (y/n): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("❌ Operation cancelled.")
        return
    
    try:
        successful, failed = manager.add_multiple_symbols(symbols)
        
        print(f"\n🎉 FINAL SUMMARY:")
        print(f"✅ Successfully added: {len(successful)} symbols")
        print(f"❌ Failed to add: {len(failed)} symbols")
        
        if successful:
            print(f"\n✅ Successfully added symbols with tables:")
            for symbol in successful:
                print(f"   • {symbol}_5M and {symbol}_DAILY")
            print("\n💡 You can now run continuous monitoring to collect data for these symbols.")
        
        if failed:
            print(f"\n⚠️  Failed symbols (check error messages above):")
            for symbol in failed:
                print(f"   • {symbol}")
        
    except KeyboardInterrupt:
        print("\n\n🛑 Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    add_multiple_symbols()
