#!/usr/bin/env python3
"""
Inspect the main DuckDB database in data/database/.
"""

import sys
from pathlib import Path
import duckdb


def inspect_database(db_path):
    """Inspect a database file."""
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    print(f"\nDatabase: {db_path}")
    print(f"Size: {db_path.stat().st_size / (1024*1024):.2f} MB")
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    # List all tables
    print("\nTables:")
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
        else:
            for (table,) in tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    print(f"  {table:.<40} {count:>10,} rows")
                except Exception as e:
                    print(f"  {table:.<40} ERROR: {e}")
    except Exception as e:
        print(f"Error: {e}")
    
    conn.close()


def main():
    """Main function."""
    base_path = Path(__file__).parent.parent / "data" / "database"
    
    print("="*60)
    print("FINLOOM DATABASE INSPECTION")
    print("="*60)
    
    # Check finloom.duckdb
    main_db = base_path / "finloom.duckdb"
    inspect_database(main_db)
    
    # Check backup if exists
    backups = list(base_path.glob("finloom_backup_*.duckdb"))
    if backups:
        print(f"\n\nFound {len(backups)} backup(s):")
        for backup in sorted(backups, reverse=True)[:3]:
            print(f"  {backup.name} ({backup.stat().st_size / (1024*1024):.2f} MB)")
    
    print("\n" + "="*60)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
