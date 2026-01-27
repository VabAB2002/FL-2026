# Database Migrations

This directory contains versioned database schema migrations for FinLoom.

## Overview

Database migrations track the evolution of the database schema over time. Each migration is:
- **Versioned**: Numbered sequentially (001, 002, 003, ...)
- **Idempotent**: Safe to run multiple times
- **Documented**: Includes description, date, and change list
- **Tested**: Includes verification step

## Migration Files

| Number | Name | Date | Status | Description |
|--------|------|------|--------|-------------|
| 001 | `unstructured_schema` | 2024-01-15 | ✅ Applied | Adds unstructured data extraction capabilities (sections metadata, footnotes table, chunks table) |

## Naming Convention

Migrations follow this naming pattern:
```
{number:03d}_{descriptive_name}.py

Examples:
- 001_unstructured_schema.py
- 002_add_user_auth.py
- 003_index_optimization.py
```

## Running Migrations

### Option 1: Run Directly

```bash
# Run a specific migration
python migrations/001_unstructured_schema.py

# Run from project root
cd /path/to/FinLoom-2026
python migrations/001_unstructured_schema.py
```

### Option 2: Import in Python

```python
from pathlib import Path
from migrations.migration_001_unstructured_schema import run_migration

db_path = Path("data/database/finloom.duckdb")
success = run_migration(db_path)

if success:
    print("✅ Migration successful")
else:
    print("❌ Migration failed")
```

## Migration Structure

Each migration file should include:

```python
"""
Migration {number}: {Name}

Date: YYYY-MM-DD
Applied: Yes/No

Description:
    Detailed description of what this migration does

Changes:
    - table_name: +X columns, -Y columns
    - NEW TABLE: table_name (description)
    - NEW INDEX: index_name

Dependencies:
    - Requires migration 00X
    - Requires table Y

Notes:
    - Important notes
    - Breaking changes
    - Data migration steps
"""

# Migration metadata
MIGRATION_NUMBER = 1
MIGRATION_NAME = "descriptive_name"
MIGRATION_DATE = "2024-01-15"

def run_migration(db_path: Path) -> bool:
    """Run the migration."""
    # Migration logic here
    pass

def main():
    """CLI entry point."""
    pass

if __name__ == "__main__":
    main()
```

## Best Practices

### 1. Idempotency

Always make migrations idempotent using:
- `CREATE TABLE IF NOT EXISTS`
- `ALTER TABLE ADD COLUMN IF NOT EXISTS` (or check existing columns first)
- `CREATE INDEX IF NOT EXISTS`

### 2. Verification

Always include a verification step:
```python
def verify_migration(conn) -> bool:
    """Verify migration was successful."""
    # Check tables exist
    # Check columns exist
    # Return True/False
```

### 3. Error Handling

```python
try:
    conn = duckdb.connect(db_path)
    # Run migration
    conn.close()
except Exception as e:
    logger.error(f"Migration failed: {e}")
    return False
```

### 4. Documentation

Document:
- What changed
- Why it changed
- Dependencies
- Breaking changes
- Data migration steps (if any)

### 5. Testing

Test migrations on:
- Fresh database (no existing tables)
- Existing database (with data)
- Re-running (idempotency test)

## Migration Workflow

### Creating a New Migration

1. **Determine Changes**
   - What tables/columns need to change?
   - Are there dependencies?
   - Is data migration needed?

2. **Create Migration File**
   ```bash
   # Next number is 002
   touch migrations/002_your_migration_name.py
   ```

3. **Write Migration Code**
   - Use template above
   - Make it idempotent
   - Add verification
   - Document changes

4. **Test Migration**
   ```bash
   # Test on dev database
   python migrations/002_your_migration_name.py
   
   # Test re-running (should be safe)
   python migrations/002_your_migration_name.py
   ```

5. **Update This README**
   - Add row to migration table
   - Document any special notes

6. **Commit**
   ```bash
   git add migrations/002_your_migration_name.py
   git add migrations/README.md
   git commit -m "Add migration 002: your migration name"
   ```

### Applying Migrations in Production

```bash
# 1. Backup database first!
cp data/database/finloom.duckdb data/database/finloom.duckdb.backup

# 2. Run migration
python migrations/00X_migration_name.py

# 3. Verify
python finloom.py status --verify-integrity

# 4. If successful, delete backup
# If failed, restore backup
```

## Future Enhancements

Potential improvements to the migration system:

1. **Migration Tracker Table**
   ```sql
   CREATE TABLE schema_migrations (
       migration_number INTEGER PRIMARY KEY,
       migration_name VARCHAR,
       applied_at TIMESTAMP,
       success BOOLEAN
   );
   ```

2. **Migration Runner**
   ```bash
   # Run all pending migrations
   python migrations/run_migrations.py --all
   
   # Run up to specific migration
   python migrations/run_migrations.py --to 005
   ```

3. **Rollback Support**
   ```python
   def rollback_migration(db_path: Path) -> bool:
       """Rollback this migration."""
       # Drop created tables
       # Remove added columns
       # Restore previous state
   ```

4. **Dry Run Mode**
   ```bash
   # Show what would be changed without applying
   python migrations/002_migration.py --dry-run
   ```

## Troubleshooting

### Migration Failed Mid-Way

If a migration fails partway through:

1. Check error log
2. Restore from backup if needed
3. Fix migration code
4. Re-run (should be idempotent)

### Column Already Exists Error

Migrations should check for existing columns before adding:

```python
existing_cols = conn.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'your_table'
""").fetchall()

if 'new_column' not in {col[0] for col in existing_cols}:
    conn.execute("ALTER TABLE your_table ADD COLUMN new_column VARCHAR")
```

### Foreign Key Constraint Error

Ensure parent tables/columns exist before creating foreign keys:

```python
# Check parent table exists
tables = conn.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_name = 'parent_table'
""").fetchall()

if not tables:
    raise ValueError("Parent table 'parent_table' must exist before running this migration")
```

## References

- [DuckDB ALTER TABLE](https://duckdb.org/docs/sql/statements/alter_table)
- [DuckDB CREATE TABLE](https://duckdb.org/docs/sql/statements/create_table)
- [Database Migration Best Practices](https://www.prisma.io/dataguide/types/relational/what-are-database-migrations)
