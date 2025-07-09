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

# Market configuration
MARKET_CONFIG = {
    'open_time': '09:15',
    'close_time': '15:30',
    'working_days': [0, 1, 2, 3, 4]  # Monday to Friday
}

# Data collection configuration
CONFIG = {
    'intraday': {
        'fetch_interval': 300,        # 5 minutes (in seconds)
        'request_period': '7d',       # Request 7 days of historical data
        'data_interval': '5m',        # 5-minute interval data
        'table_suffix': '_intraday',
        'display_name': 'Intraday (5m)'
    },
    'daily': {
        'fetch_interval': 86400,      # 24 hours (in seconds)
        'request_period': '1mo',      # Request 1 month of historical data
        'data_interval': '1d',        # Daily interval data
        'table_suffix': '_daily',
        'display_name': 'Daily'
    }
}

# Data configuration (for compatibility)
DATA_CONFIG = {
    '5M': {
        'interval': '5m',
        'table_suffix': '_5M',
        'display_name': '5-minute'
    },
    'DAILY': {
        'interval': '1d', 
        'table_suffix': '_DAILY',
        'display_name': 'Daily'
    }
}

# Indian indices mapping
INDIAN_INDICES = {
    'NSEI': '^NSEI',      # Nifty 50
    'BSESN': '^BSESN',    # Sensex
    'NSEBANK': '^NSEBANK' # Nifty Bank
}