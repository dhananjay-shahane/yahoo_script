"""
Configuration settings for the stock data collection system
"""

import pytz
from datetime import time

# Timezone configuration
INDIA_TZ = pytz.timezone("Asia/Kolkata")

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'stock_data',
    'user': 'postgres',
    'password': 'yourpassword'
}

# Database URL and schema for other modules
DB_URL = "postgresql://kotak_trading_db_user:JRUlk8RutdgVcErSiUXqljDUdK8sBsYO@dpg-d1cjd66r433s73fsp4n0-a.oregon-postgres.render.com/kotak_trading_db"
SCHEMA_NAME = "SYMBOLS"

# Market configuration
MARKET_CONFIG = {
    'open_time': '09:15',
    'close_time': '15:30',
    'working_days': [0, 1, 2, 3, 4]  # Monday to Friday
}

# Data collection configuration
# IMPORTANT: fetching_time_interval and time_period are different concepts:
#
# fetching_time_interval: How often to make requests to Yahoo Finance API (in seconds)
#                         - Controls API request frequency to avoid rate limits
#                         - Should be reasonable to avoid hitting API limits
#
# time_period: The granularity/interval of data requested from Yahoo Finance
#             - Format: Yahoo Finance interval format ('5m', '1h', '1d', etc.)
#             - Determines the resolution of each data candle
#
# Example: fetching_time_interval=300 with time_period='5m' means:
#          "Fetch 5-minute candles every 5 minutes"

DATA_CONFIG = {
    '5M': {
        'fetching_time_interval': 300,    # How often to make requests (5 minutes)
        'time_period': '5m',              # Data granularity (5-minute candles)
        'min_throttle_seconds': 10,       # Reduced to 10 seconds between requests per symbol
        'table_suffix': '_5M',
        'display_name': '5-minute',
        'description': 'Focus on 5-minute data during market hours'
    },
    'DAILY': {
        'fetching_time_interval': 3600,   # How often to make requests (1 hour)
        'time_period': '1d',              # Data granularity (daily candles)
        'min_throttle_seconds': 10,      # Reduced to 10 seconds between requests per symbol
        'table_suffix': '_DAILY',
        'display_name': 'Daily',
        'description': 'Daily data updates (less frequent)'
    }
}

# Rate limiting configuration
RATE_LIMIT_CONFIG = {
    'max_requests_per_minute': 10,        # Maximum 10 requests per minute across all symbols
    'delay_between_symbols': 5,           # 5 seconds delay when processing multiple symbols
    'retry_delays': [2, 5, 10],          # Progressive retry delays in seconds
    'max_retries': 3                      # Maximum retry attempts
}

# Indian indices mapping
INDIAN_INDICES = {
    'NSEI': '^NSEI',  # Nifty 50
    'BSESN': '^BSESN',  # Sensex
    'NSEBANK': '^NSEBANK'  # Nifty Bank
}

# Default configuration for continuous updates
DEFAULT_CONFIG = {
    'primary_fetching_interval': 300,    # 5 minutes for 5M data during market hours
    'daily_fetching_interval': 3600,     # 1 hour for daily data updates
    'focus_on_5m_only': True             # Only fetch 5-minute data during market hours
}