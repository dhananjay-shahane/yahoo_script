from utils import load_existing_schema_tables
from yahoo_finanace import check_or_create_symbol_table


tables = load_existing_schema_tables('SYMBOLS')
print("Existing tables in SYMBOLS schema:", tables)

check_or_create_symbol_table("RELIANCE", schema_name="SYMBOLS")