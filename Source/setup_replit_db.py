
"""
Script to help setup Replit's built-in PostgreSQL database
This is recommended for better reliability and performance
"""

import os

def setup_replit_database():
    """Guide user through setting up Replit PostgreSQL"""
    
    print("🔧 Replit PostgreSQL Setup Guide")
    print("=" * 50)
    
    # Check if DATABASE_URL is available (Replit's env var)
    if 'DATABASE_URL' in os.environ:
        print("✅ Replit PostgreSQL database detected!")
        print(f"Database URL: {os.environ['DATABASE_URL'][:50]}...")
        
        # Update config to use Replit's database
        config_update = f'''
# Update your config.py with Replit's database URL:
import os
DB_URL = os.environ.get('DATABASE_URL', '{DB_URL}')
'''
        print("\n📝 To use Replit's database, update your config.py:")
        print(config_update)
        
        return os.environ['DATABASE_URL']
    else:
        print("❌ Replit PostgreSQL not detected")
        print("\n📖 To set up Replit PostgreSQL:")
        print("1. Open a new tab in Replit")
        print("2. Type 'Database' and select PostgreSQL") 
        print("3. Click 'Create a database'")
        print("4. Replit will automatically set DATABASE_URL environment variable")
        print("5. Restart your repl to load the new environment variable")
        
        return None

if __name__ == "__main__":
    db_url = setup_replit_database()
    if db_url:
        print("\n🎉 Ready to use Replit PostgreSQL!")
        print("💡 This will provide better connection stability than external databases")
