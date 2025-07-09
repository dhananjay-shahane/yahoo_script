
"""
Test script to diagnose database connection issues
"""

import os
import psycopg2
import urllib.parse as urlparse
from config import DB_URL


def test_connection():
    """Test database connection with different configurations"""
    
    print("Testing database connection...")
    
    # Check for Replit's built-in database first
    if 'DATABASE_URL' in os.environ:
        print("🎯 Replit PostgreSQL detected - testing...")
        replit_url = os.environ['DATABASE_URL']
        if test_specific_url(replit_url, "Replit PostgreSQL"):
            return "replit"
    
    print(f"Testing external database: {urlparse.urlparse(DB_URL).hostname}")
    
    # Test pooled connection first
    pool_url = DB_URL.replace('.oregon-postgres.render.com', '-pooler.oregon-postgres.render.com')
    if test_specific_url(pool_url, "Pooled connection"):
        return "pooled"
    
    if test_specific_url(DB_URL, "Direct connection"):
        return "direct"
    
    return None

def test_specific_url(url, description):
    """Test specific database URL with different SSL modes"""
    print(f"\n--- Testing {description} ---")
    ssl_modes = ['require', 'prefer', 'allow']
    
    for ssl_mode in ssl_modes:
        print(f"\n--- Testing SSL mode: {ssl_mode} ---")
        
        try:
            parsed = urlparse.urlparse(url)
            
            conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path[1:] if parsed.path else 'postgres',
                sslmode=ssl_mode,
                connect_timeout=15,
                application_name='test_connection'
            )
            
            # Test the connection
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                print(f"✅ SUCCESS - Connected with {ssl_mode}")
                print(f"PostgreSQL version: {version}")
            
            conn.close()
            print(f"✅ Connection closed successfully")
            return True  # Return success
            
        except Exception as e:
            print(f"❌ FAILED - {ssl_mode}: {e}")
    
    print(f"\n❌ All connection attempts failed for {description}")
    return False


if __name__ == "__main__":
    successful_mode = test_connection()
    if successful_mode:
        print(f"\n🎉 Recommended SSL mode: {successful_mode}")
    else:
        print("\n💡 Try checking your network connection or contact your database provider")
