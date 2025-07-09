
"""
Simple script to add new symbols to the stock data collection system
"""

from stock_data_manager import StockDataManager


def add_symbol():
    """Add a new symbol to the system"""
    manager = StockDataManager()
    
    print("=== Add New Symbol ===")
    symbol = input("Enter symbol to add (e.g., RELIANCE, AAPL, NSEI): ").strip().upper()
    
    if not symbol:
        print("Invalid symbol. Please enter a valid symbol.")
        return
    
    print(f"Adding symbol: {symbol}")
    try:
        manager.add_new_symbol(symbol)
        print(f"Successfully added {symbol} to the system!")
    except Exception as e:
        print(f"Error adding symbol {symbol}: {e}")


if __name__ == "__main__":
    add_symbol()
