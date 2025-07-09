
"""
Connection pooling for better database performance and reliability
"""

import psycopg2
from psycopg2 import pool
import urllib.parse as urlparse
from config import DB_URL
import threading
import time

class DatabasePool:
    """Singleton database connection pool"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.pool = None
            self._initialized = True
            self._create_pool()
    
    def _create_pool(self):
        """Create connection pool with fallback strategies"""
        # Try pooled URL first
        pool_url = DB_URL.replace('.oregon-postgres.render.com', '-pooler.oregon-postgres.render.com')
        
        urls_to_try = [
            (pool_url, "Pooled connection"),
            (DB_URL, "Direct connection")
        ]
        
        for url, description in urls_to_try:
            try:
                print(f"Creating connection pool using: {description}")
                self.pool = psycopg2.pool.SimpleConnectionPool(
                    1, 5,  # min and max connections
                    url,
                    sslmode='require',
                    connect_timeout=15,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=5,
                    keepalives_count=3
                )
                print(f"✅ Connection pool created successfully")
                return
            except Exception as e:
                print(f"❌ Failed to create pool with {description}: {e}")
        
        print("❌ Failed to create connection pool")
    
    def get_connection(self):
        """Get connection from pool"""
        if self.pool:
            try:
                return self.pool.getconn()
            except Exception as e:
                print(f"Error getting connection from pool: {e}")
                return None
        return None
    
    def return_connection(self, conn):
        """Return connection to pool"""
        if self.pool and conn:
            try:
                self.pool.putconn(conn)
            except Exception as e:
                print(f"Error returning connection to pool: {e}")
    
    def close_pool(self):
        """Close all connections in pool"""
        if self.pool:
            try:
                self.pool.closeall()
                print("Connection pool closed")
            except Exception as e:
                print(f"Error closing pool: {e}")

# Global pool instance
db_pool = DatabasePool()
