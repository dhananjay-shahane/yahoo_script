
"""
Test script to diagnose database connection issues
"""

import psycopg2
import urllib.parse as urlparse
from config import DB_URL


def test_connection():
    """Test database connection with different configurations"""
    
    print("Testing database connection...")
    print(f"Database URL host: {urlparse.urlparse(DB_URL).hostname}")
    
    ssl_modes = ['disable', 'allow', 'prefer', 'require']
    
    for ssl_mode in ssl_modes:
        print(f"\n--- Testing SSL mode: {ssl_mode} ---")
        
        try:
            parsed = urlparse.urlparse(DB_URL)
            
            conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path[1:],
                sslmode=ssl_mode,
                connect_timeout=10
            )
            
            # Test the connection
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                print(f"✅ SUCCESS - Connected with {ssl_mode}")
                print(f"PostgreSQL version: {version}")
            
            conn.close()
            print(f"✅ Connection closed successfully")
            return ssl_mode  # Return first successful mode
            
        except Exception as e:
            print(f"❌ FAILED - {ssl_mode}: {e}")
    
    print("\n❌ All connection attempts failed")
    return None


if __name__ == "__main__":
    successful_mode = test_connection()
    if successful_mode:
        print(f"\n🎉 Recommended SSL mode: {successful_mode}")
    else:
        print("\n💡 Try checking your network connection or contact your database provider")
