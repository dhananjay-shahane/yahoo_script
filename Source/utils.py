
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine, inspect
# Replace with your actual PostgreSQL connection URL
DB_URL = "postgresql://kotak_trading_db_user:JRUlk8RutdgVcErSiUXqljDUdK8sBsYO@dpg-d1cjd66r433s73fsp4n0-a.oregon-postgres.render.com/kotak_trading_db"

def get_all_symbol_tables():
    """Get all table names in the symbols schema"""
    try:
        engine = create_engine(DB_URL)
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema='symbols')
        return tables
    except Exception as e:
        print(f"Error getting tables from symbols schema: {e}")
        return []

def create_db_connection():
    """Create a database connection"""
    try:
        conn = psycopg2.connect(DB_URL)
        print("Database connection established successfully.")
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def load_existing_schema_tables(schema_name='symbols'):
    """Load existing table names from specified schema"""
    conn = create_db_connection()
    if not conn:
        return []
    
    existing_tables = []
    try:
        with conn.cursor() as cur:
            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE';
            """
            cur.execute(query, (schema_name,))
            existing_tables = [row[0] for row in cur.fetchall()]
        return existing_tables
    except Error as e:
        print(f"Error loading tables from schema '{schema_name}': {e}")
        return []
    finally:
        conn.close()

def check_or_create_symbol_table(table_identifier, schema_name='symbols'):
    """Check if table exists in schema, create if not. table_identifier should include time period like RELIANCE_5M or RELIANCE_DAILY"""
    conn = create_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            # Ensure schema exists
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
            conn.commit()
            print(f"Ensured schema '{schema_name}' exists.")
            
            table_name = table_identifier.upper()
            full_table_name = f'{schema_name}.{table_name}'
            
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = %s
                    AND table_name = %s
                );
            """, (schema_name, table_name))
            
            if not cur.fetchone()[0]:
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
                
    except Exception as e:
        print(f"Error checking/creating table for {table_identifier}: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

def get_tables_by_time_period(time_period='5M'):
    """Get all tables for a specific time period (5M or DAILY)"""
    tables = get_all_symbol_tables()
    return [table for table in tables if table.endswith(f'_{time_period}')]

# Run and test
if __name__ == "__main__":
    existing = load_existing_schema_tables('symbols')
    print("Existing tables in symbols schema:", existing)

    # Test creating tables for different time periods
    check_or_create_symbol_table("RELIANCE_5M")
    check_or_create_symbol_table("RELIANCE_DAILY")
