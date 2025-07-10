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
# fetching_time_interval: How often to make requests to Yahoo Finance (in seconds)
# time_period: The granularity of data (5m, 1h, 1d, etc.)
CONFIG = {
    'intraday': {
        'fetching_time_interval': 300,  # How often to fetch data (5 minutes in seconds)
        'time_period': '5m',           # Data granularity (5-minute candles)
        'request_period': '7d',        # Historical data range to request
        'table_suffix': '_intraday',
        'display_name': 'Intraday (5m)'
    },
    'daily': {
        'fetching_time_interval': 86400,  # How often to fetch data (24 hours in seconds)
        'time_period': '1d',             # Data granularity (daily candles)
        'request_period': '1mo',         # Historical data range to request
        'table_suffix': '_daily',
        'display_name': 'Daily'
    }
}

# Data configuration (for compatibility)
# fetching_time_interval: How often to make requests (in seconds)
# time_period: The granularity of data requested from Yahoo Finance
DATA_CONFIG = {
    '5M': {
        'fetching_time_interval': 300,  # Fetch every 5 minutes
        'time_period': '5m',           # 5-minute candles
        'table_suffix': '_5M',
        'display_name': '5-minute'
    },
    'DAILY': {
        'fetching_time_interval': 86400,  # Fetch every 24 hours
        'time_period': '1d',             # Daily candles
        'table_suffix': '_DAILY',
        'display_name': 'Daily'
    }
}

# Indian indices mapping
INDIAN_INDICES = {
    'NSEI': '^NSEI',  # Nifty 50
    'BSESN': '^BSESN',  # Sensex
    'NSEBANK': '^NSEBANK'  # Nifty Bank
}