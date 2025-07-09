
"""
Configuration settings for the stock data collection system
"""

import pytz

# Database configuration
DB_URL = "postgresql://kotak_trading_db_user:JRUlk8RutdgVcErSiUXqljDUdK8sBsYO@dpg-d1cjd1r433s73fsp4n0-a.oregon-postgres.render.com/kotak_trading_db"

# Time zone configuration
INDIA_TZ = pytz.timezone("Asia/Kolkata")

# Market configuration
MARKET_CONFIG = {
    'open_time': '09:00',
    'close_time': '15:30',
    'working_days': [0, 1, 2, 3, 4]  # Monday to Friday
}

# Data fetching configuration
DATA_CONFIG = {
    'intraday': {
        'fetching_time_interval': 300,  # 5 minutes in seconds
        'time_period': '5m',
        'table_suffix': '_5M',
        'yahoo_interval': '5m'
    },
    'daily': {
        'fetching_time_interval': 3600,  # 1 hour in seconds  
        'time_period': '1d',
        'table_suffix': '_DAILY',
        'yahoo_interval': '1d'
    }
}

# Schema configuration
SCHEMA_NAME = 'symbols'

# Indian indices mapping
INDIAN_INDICES = {
    'NSEI': '^NSEI',      # Nifty 50
    'BSESN': '^BSESN',    # Sensex
    'NSEBANK': '^NSEBANK' # Nifty Bank
}
