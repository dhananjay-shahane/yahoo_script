
"""
Run script for legacy yahoo finance mode with scheduling
"""

from legacy_yahoo_finance import schedule_jobs

def main():
    """Main function for legacy mode"""
    print("=== Legacy Yahoo Finance Data Collection ===")
    symbol = input("Enter stock symbol (e.g., RELIANCE, NSEI): ").strip().upper()
    
    if not symbol:
        print("Invalid symbol. Please enter a valid symbol.")
        return
    
    print(f"\nStarting data collection for {symbol}...")
    print("This will use the legacy scheduling system from yahoo_finanace.py")
    
    try:
        schedule_jobs(symbol)
    except KeyboardInterrupt:
        print("\nStopping data collection...")

if __name__ == "__main__":
    main()
