
# Data Processing and Configuration Guide

## Key Terminology

### `fetching_time_interval` vs `time_period`

These are two different concepts that often get confused:

#### `fetching_time_interval` (How Often to Fetch)
- **Definition**: How frequently the system makes requests to Yahoo Finance API
- **Unit**: Seconds
- **Examples**:
  - `300` = Fetch every 5 minutes
  - `3600` = Fetch every hour
  - `86400` = Fetch every 24 hours
- **Purpose**: Controls API request frequency to avoid rate limits

#### `time_period` (Data Granularity)
- **Definition**: The interval/granularity of each data point from Yahoo Finance
- **Unit**: Yahoo Finance interval format
- **Examples**:
  - `'5m'` = Each data point represents 5 minutes of market data
  - `'1h'` = Each data point represents 1 hour of market data
  - `'1d'` = Each data point represents 1 day of market data
- **Purpose**: Determines the resolution of market data

### Example Scenarios

1. **High-frequency 5-minute data during market hours**:
   - `fetching_time_interval = 300` (fetch every 5 minutes)
   - `time_period = '5m'` (each candle represents 5 minutes)

2. **Daily data updates**:
   - `fetching_time_interval = 86400` (fetch once per day)
   - `time_period = '1d'` (each candle represents 1 day)

3. **Hourly data with frequent updates**:
   - `fetching_time_interval = 1800` (fetch every 30 minutes)
   - `time_period = '1h'` (each candle represents 1 hour)

## Data Processing Flow

### 1. Data Fetching
```python
# Fetch data from Yahoo Finance
ticker = yf.Ticker(symbol)
data = ticker.history(period='7d', interval='5m')  # time_period = '5m'
```

### 2. Data Processing (`process_market_data`)
The system automatically processes all fetched data:

#### UTC to IST Conversion
- Yahoo Finance returns timestamps in UTC
- System adds 5 hours 30 minutes to convert to IST
- Example: `2024-01-15 09:15:00 UTC` → `2024-01-15 14:45:00 IST`

#### Row Order Reversal
- Yahoo Finance returns data with most recent first
- System reverses order so most recent data is at the bottom
- This matches traditional financial data presentation

### 3. Database Storage
- Processed data is stored in PostgreSQL
- Each symbol has separate tables for different time periods
- Table naming: `{SYMBOL}_{TIME_PERIOD}` (e.g., `RELIANCE_5M`, `RELIANCE_DAILY`)

## Configuration Examples

### Current Configuration
```python
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
```

### Custom Configuration Example
```python
# For 1-minute data with 2-minute fetching
'1M': {
    'fetching_time_interval': 120,  # Fetch every 2 minutes
    'time_period': '1m',           # 1-minute candles
    'table_suffix': '_1M',
    'display_name': '1-minute'
}
```

## Best Practices

1. **Rate Limiting**: Don't set `fetching_time_interval` too low to avoid API rate limits
2. **Market Hours**: 5-minute data is only fetched during market hours (9:15-15:30 IST)
3. **Data Consistency**: Most recent data always appears at the bottom of tables
4. **Timezone**: All stored data uses IST timezone for consistency
