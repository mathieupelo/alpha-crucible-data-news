#!/usr/bin/env python3
"""Test script to verify database connection and ticker query."""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import from main
from main import get_distinct_tickers, get_main_db_connection, get_ore_db_connection

def test_connections():
    """Test database connections and ticker query."""
    main_conn = None
    ore_conn = None
    
    try:
        print("=" * 60)
        print("Testing Database Connections")
        print("=" * 60)
        
        # Test main database connection
        print("\n1. Testing main database connection...")
        main_conn = get_main_db_connection()
        print("   ✓ Main database connection successful")
        
        # Test ORE database connection
        print("\n2. Testing ORE database connection...")
        ore_conn = get_ore_db_connection()
        print("   ✓ ORE database connection successful")
        
        # Test ticker query
        print("\n3. Testing ticker query from varrock.tickers...")
        tickers = get_distinct_tickers(main_conn)
        print(f"   ✓ Found {len(tickers)} distinct tickers")
        
        if tickers:
            print(f"\n   Sample tickers (first 10):")
            for i, ticker in enumerate(sorted(tickers)[:10], 1):
                print(f"   {i}. {ticker}")
            if len(tickers) > 10:
                print(f"   ... and {len(tickers) - 10} more")
        else:
            print("   ⚠ No tickers found in varrock.tickers table")
        
        print("\n" + "=" * 60)
        print("All tests passed successfully!")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if main_conn:
            main_conn.close()
            print("\n✓ Closed main database connection")
        if ore_conn:
            ore_conn.close()
            print("✓ Closed ORE database connection")

if __name__ == "__main__":
    success = test_connections()
    sys.exit(0 if success else 1)

