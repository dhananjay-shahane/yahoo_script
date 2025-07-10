
"""
Simple script to add new symbols to the stock data collection system
"""

from stock_data_manager import StockDataManager


def add_symbol():
    """Add a new symbol to the system"""
    manager = StockDataManager()
    
    print("=== Add New Symbol ===")
    print("This will create two tables for the symbol:")
    print("  • {SYMBOL}_5M (for 5-minute intraday data)")
    print("  • {SYMBOL}_DAILY (for daily historical data)")
    
    symbol = input("\nEnter symbol to add (e.g., RELIANCE, AAPL, NSEI): ").strip().upper()
    
    if not symbol:
        print("❌ Invalid symbol. Please enter a valid symbol.")
        return
    
    print(f"\n🔄 Adding symbol: {symbol}")
    print(f"Will create tables: {symbol}_5M and {symbol}_DAILY")
    
    try:
        success = manager.add_new_symbol(symbol)
        if success:
            print(f"\n🎉 Successfully added {symbol} to the system!")
            print(f"✅ Created tables: {symbol}_5M and {symbol}_DAILY")
            print(f"💡 You can now include this symbol in continuous monitoring.")
        else:
            print(f"\n❌ Failed to add {symbol}. Please check the error messages above.")
    except Exception as e:
        print(f"\n❌ Error adding symbol {symbol}: {e}")


if __name__ == "__main__":
    add_symbol()
