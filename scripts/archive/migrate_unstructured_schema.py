"""
Database schema migration for unstructured data system.

Adds new columns to existing sections and tables tables for enhanced metadata.
Creates new chunks and footnotes tables.
"""

import duckdb
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("finloom.migration")


def migrate_sections_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Add new metadata columns to sections table."""
    logger.info("Migrating sections table...")
    
    # Check if columns exist before adding
    existing_columns = conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'sections'
    """).fetchall()
    
    existing_cols = {col[0] for col in existing_columns}
    
    new_columns = [
        ("section_part", "VARCHAR"),
        ("parent_section_id", "INTEGER"),
        ("subsections", "JSON"),
        ("contains_tables", "INTEGER", "0"),
        ("contains_lists", "INTEGER", "0"),
        ("contains_footnotes", "INTEGER", "0"),
        ("cross_references", "JSON"),
        ("page_numbers", "JSON"),
        ("heading_hierarchy", "JSON"),
        ("extraction_quality", "DECIMAL(5, 4)"),
        ("extraction_issues", "JSON"),
    ]
    
    for col_info in new_columns:
        col_name = col_info[0]
        col_type = col_info[1]
        default = col_info[2] if len(col_info) > 2 else None
        
        if col_name not in existing_cols:
            default_clause = f" DEFAULT {default}" if default else ""
            sql = f"ALTER TABLE sections ADD COLUMN {col_name} {col_type}{default_clause}"
            logger.info(f"Adding column: {col_name}")
            conn.execute(sql)
        else:
            logger.info(f"Column {col_name} already exists, skipping")
    
    # Add new index
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sections_part ON sections(section_part)")
        logger.info("Created index idx_sections_part")
    except Exception as e:
        logger.warning(f"Index creation skipped: {e}")
    
    logger.info("✅ Sections table migration complete")


def migrate_tables_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Add new metadata columns to tables table."""
    logger.info("Migrating tables table...")
    
    # Check if columns exist before adding
    existing_columns = conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'tables'
    """).fetchall()
    
    existing_cols = {col[0] for col in existing_columns}
    
    new_columns = [
        ("table_caption", "VARCHAR"),
        ("table_markdown", "TEXT"),
        ("is_financial_statement", "BOOLEAN", "FALSE"),
        ("table_category", "VARCHAR"),
        ("parent_table_id", "INTEGER"),
        ("footnote_refs", "JSON"),
        ("cell_metadata", "JSON"),
        ("extraction_quality", "DECIMAL(5, 4)"),
    ]
    
    for col_info in new_columns:
        col_name = col_info[0]
        col_type = col_info[1]
        default = col_info[2] if len(col_info) > 2 else None
        
        if col_name not in existing_cols:
            default_clause = f" DEFAULT {default}" if default else ""
            sql = f"ALTER TABLE tables ADD COLUMN {col_name} {col_type}{default_clause}"
            logger.info(f"Adding column: {col_name}")
            conn.execute(sql)
        else:
            logger.info(f"Column {col_name} already exists, skipping")
    
    logger.info("✅ Tables table migration complete")


def create_footnotes_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create footnotes table."""
    logger.info("Creating footnotes table...")
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS footnotes (
            footnote_id VARCHAR PRIMARY KEY,
            accession_number VARCHAR NOT NULL,
            section_id INTEGER,
            table_id INTEGER,
            
            marker VARCHAR,
            footnote_text TEXT NOT NULL,
            footnote_type VARCHAR,
            
            ref_links JSON,
            referenced_by JSON,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (accession_number) REFERENCES filings(accession_number),
            FOREIGN KEY (section_id) REFERENCES sections(id),
            FOREIGN KEY (table_id) REFERENCES tables(id)
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_footnotes_accession ON footnotes(accession_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_footnotes_section ON footnotes(section_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_footnotes_table ON footnotes(table_id)")
    
    logger.info("✅ Footnotes table created")


def create_chunks_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create chunks table."""
    logger.info("Creating chunks table...")
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id VARCHAR PRIMARY KEY,
            accession_number VARCHAR NOT NULL,
            section_id INTEGER,
            parent_chunk_id VARCHAR,
            
            chunk_level INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_text TEXT NOT NULL,
            chunk_markdown TEXT,
            
            token_count INTEGER,
            char_start INTEGER,
            char_end INTEGER,
            
            heading VARCHAR,
            section_type VARCHAR,
            contains_tables BOOLEAN DEFAULT FALSE,
            contains_lists BOOLEAN DEFAULT FALSE,
            contains_numbers BOOLEAN DEFAULT FALSE,
            
            cross_references JSON,
            
            s3_path VARCHAR,
            embedding_vector FLOAT[],
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (accession_number) REFERENCES filings(accession_number),
            FOREIGN KEY (section_id) REFERENCES sections(id)
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_accession ON chunks(accession_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_section ON chunks(section_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_level ON chunks(chunk_level)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_parent ON chunks(parent_chunk_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_section_type ON chunks(section_type)")
    
    logger.info("✅ Chunks table created")


def verify_migration(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify migration was successful."""
    logger.info("Verifying migration...")
    
    # Check sections table
    sections_cols = conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'sections'
    """).fetchall()
    
    required_sections_cols = {
        'section_part', 'subsections', 'contains_tables', 'cross_references',
        'heading_hierarchy', 'extraction_quality'
    }
    
    actual_sections_cols = {col[0] for col in sections_cols}
    missing = required_sections_cols - actual_sections_cols
    
    if missing:
        logger.error(f"❌ Missing sections columns: {missing}")
        return False
    
    # Check tables table
    tables_cols = conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'tables'
    """).fetchall()
    
    required_tables_cols = {
        'table_markdown', 'is_financial_statement', 'table_category'
    }
    
    actual_tables_cols = {col[0] for col in tables_cols}
    missing = required_tables_cols - actual_tables_cols
    
    if missing:
        logger.error(f"❌ Missing tables columns: {missing}")
        return False
    
    # Check new tables exist
    tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
    """).fetchall()
    
    table_names = {t[0] for t in tables}
    
    if 'chunks' not in table_names:
        logger.error("❌ chunks table not created")
        return False
    
    if 'footnotes' not in table_names:
        logger.error("❌ footnotes table not created")
        return False
    
    logger.info("✅ Migration verification passed")
    return True


def main():
    """Run migration."""
    db_path = project_root / "data" / "database" / "finloom.duckdb"
    
    if not db_path.exists():
        logger.error(f"Database not found at {db_path}")
        sys.exit(1)
    
    logger.info(f"Connecting to database: {db_path}")
    
    try:
        conn = duckdb.connect(str(db_path))
        
        # Run migrations
        migrate_sections_table(conn)
        migrate_tables_table(conn)
        create_footnotes_table(conn)
        create_chunks_table(conn)
        
        # Verify
        if verify_migration(conn):
            logger.info("✅ Migration completed successfully")
        else:
            logger.error("❌ Migration verification failed")
            sys.exit(1)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
