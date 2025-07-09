"""
Database operations and utilities
"""

import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine, inspect
import pandas as pd
from config import DB_URL, SCHEMA_NAME


class DatabaseManager:
    """Handles all database operations"""

    def __init__(self):
        self.db_url = DB_URL
        self.schema_name = SCHEMA_NAME

    def create_connection(self):
        """Create a database connection with retry logic"""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                # Add SSL settings to handle connection issues
                conn = psycopg2.connect(
                    self.db_url,
                    sslmode='require',
                    connect_timeout=30
                )
                return conn
            except Exception as e:
                print(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"All {max_retries} connection attempts failed")
                    return None

    def get_all_symbol_tables(self):
        """Get all table names in the symbols schema"""
        try:
            engine = create_engine(self.db_url)
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema=self.schema_name)
            return tables
        except Exception as e:
            print(f"Error getting tables from {self.schema_name} schema: {e}")
            return []

    def get_tables_by_time_period(self, time_period='5M'):
        """Get all tables for a specific time period (5M or DAILY)"""
        tables = self.get_all_symbol_tables()
        return [table for table in tables if table.endswith(f'_{time_period}')]

    def check_or_create_symbol_table(self, table_identifier):
        """Check if table exists in schema, create if not"""
        conn = self.create_connection()
        if not conn:
            return None

        try:
            with conn.cursor() as cur:
                # Ensure schema exists
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS {self.schema_name};')
                conn.commit()

                table_name = table_identifier.upper()
                full_table_name = f'{self.schema_name}.{table_name}'

                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = %s
                        AND table_name = %s
                    );
                """, (self.schema_name, table_name))

                result = cur.fetchone()
                if result is not None and not result[0]:
                    # Create table if it doesn't exist
                    cur.execute(f"""
                        CREATE TABLE {full_table_name} (
                            datetime TIMESTAMP WITH TIME ZONE PRIMARY KEY,
                            open DOUBLE PRECISION,
                            high DOUBLE PRECISION,
                            low DOUBLE PRECISION,
                            close DOUBLE PRECISION,
                            volume DOUBLE PRECISION
                        );
                        CREATE INDEX idx_{table_name.lower()}_datetime ON {full_table_name} (datetime);
                    """)
                    conn.commit()
                    print(f"Created new table {full_table_name}")
                else:
                    print(f"Using existing table {full_table_name}")

                return full_table_name

        except psycopg2.errors.DuplicateTable:
            # Handle duplicate table error specifically
            conn.rollback()
            table_name = table_identifier.upper()
            full_table_name = f'{self.schema_name}.{table_name}'
            print(f"Table {full_table_name} already exists")
            return full_table_name
        except Exception as e:
            print(f"Error checking/creating table for {table_identifier}: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()

    def save_data_to_db(self, table_name, df):
        """Save data to the symbol table, avoiding duplicates"""
        if df.empty:
            return 0

        conn = self.create_connection()
        if conn:
            try:
                cur = conn.cursor()

                # Prepare new data to insert
                new_records = []
                for _, row in df.iterrows():
                    new_records.append((
                        row['datetime'],
                        row['Open'],
                        row['High'],
                        row['Low'],
                        row['Close'],
                        row['Volume']
                    ))

                # Insert new records with ON CONFLICT DO NOTHING to avoid duplicates
                if new_records:
                    cur.executemany(f"""
                        INSERT INTO {table_name} (datetime, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (datetime) DO NOTHING
                    """, new_records)
                    conn.commit()
                    rows_inserted = cur.rowcount
                    print(f"💾 Inserted {rows_inserted} new records into {table_name}")
                    return rows_inserted
                else:
                    print(f"No new records to insert for {table_name}")
                    return 0

            except Exception as e:
                print(f"❌ Error saving data to {table_name}: {e}")
                conn.rollback()
                return 0
            finally:
                conn.close()
        return 0

    def get_last_datetime(self, table_name):
        """Get the last datetime from a table"""
        conn = self.create_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT MAX(datetime) FROM {table_name}")
                result = cur.fetchone()
                return result[0] if result[0] else None
            except Exception as e:
                print(f"Error getting last datetime for {table_name}: {e}")
                return None
            finally:
                conn.close()

    def display_latest_data(self, table_name, symbol, limit=10):
        """Display the latest data for a symbol"""
        conn = self.create_connection()
        if conn:
            try:
                cur = conn.cursor()

                cur.execute(f"""
                    SELECT datetime, open, high, low, close, volume
                    FROM {table_name}
                    ORDER BY datetime DESC
                    LIMIT %s
                """, (limit,))

                data = cur.fetchall()
                if not data:
                    print(f"No data available for {symbol}")
                    return

                # Format and display data
                from tabulate import tabulate
                df = pd.DataFrame(data, columns=['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['Datetime'] = pd.to_datetime(df['Datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

                print("\n" + "="*80)
                print(f"Latest Data for {symbol} - Table: {table_name}")
                print("="*80)
                print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
                print("\n" + "="*80)

            except Exception as e:
                print(f"Error displaying data: {e}")
            finally:
                conn.close()