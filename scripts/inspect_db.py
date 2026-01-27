#!/usr/bin/env python3
"""
Inspect DuckDB database structure.
"""

import sys
from pathlib import Path
import duckdb


def main():
    """Main function."""
    db_path = Path(__file__).parent.parent / "data" / "finloom.dev.duckdb"
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return 1
    
    print(f"Database: {db_path}")
    print(f"Size: {db_path.stat().st_size / (1024*1024):.2f} MB\n")
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    # List all tables
    print("Tables in database:")
    print("="*60)
    
    try:
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        
        if not tables:
            print("No tables found in database.")
            print("\nThe database exists but hasn't been initialized.")
            print("Run the following to initialize:")
            print("  python -c \"from src.storage.database import initialize_database; initialize_database()\"")
        else:
            for (table,) in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table:.<40} {count:>10,} rows")
    except Exception as e:
        print(f"Error: {e}")
    
    conn.close()
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
