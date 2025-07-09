
#!/usr/bin/env python3
"""
Standalone Yahoo Finance data collector runner
"""

from yahoo_finance_standalone import schedule_jobs

def main():
    print("=== Yahoo Finance Data Collector ===")
    print("This is a standalone version that works independently.")
    print()
    
    symbol = input("Enter stock symbol (e.g., RELIANCE, NSEI, TCS, INFY): ").strip().upper()
    
    if not symbol:
        print("Invalid symbol entered!")
        return
    
    print(f"\nStarting data collection for {symbol}...")
    print("Press Ctrl+C to stop the data collection")
    
    try:
        schedule_jobs(symbol)
    except KeyboardInterrupt:
        print("\nStopping data collection...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
