"""
Staging Table Manager for Idempotent Data Pipeline.

Provides isolated staging tables for parallel workers to write without
lock contention. Data is merged to production tables atomically.

Industry patterns used:
- Write-ahead staging (workers write to isolated tables)
- Single-writer merge (coordinator merges sequentially)
- Idempotent operations (DELETE + INSERT pattern)
"""

import duckdb
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..core.exceptions import StorageError
from ..utils.logger import get_logger

logger = get_logger("finloom.storage.staging")


class StagingManager:
    """
    Manages staging tables for parallel data extraction.

    Each extraction run gets its own set of staging tables,
    identified by a unique run_id. This prevents any conflicts
    between parallel workers.
    """

    # Tables that support staging
    STAGING_TABLES = ["sections", "tables", "footnotes", "chunks"]

    def __init__(self, db_path: str):
        """
        Initialize staging manager.

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = db_path

    def generate_run_id(self) -> str:
        """Generate unique run ID based on timestamp."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def create_staging_tables(self, run_id: str) -> None:
        """
        Create staging tables for a specific run.

        Creates empty tables with same schema as production tables,
        named with the run_id suffix (e.g., sections_staging_20240125_143022).

        Args:
            run_id: Unique identifier for this extraction run
        """
        conn = duckdb.connect(self.db_path)

        try:
            for table in self.STAGING_TABLES:
                staging_name = f"{table}_staging_{run_id}"

                # Create empty table with same schema
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {staging_name}
                    AS SELECT * FROM {table} WHERE 1=0
                """)

                logger.info(f"Created staging table: {staging_name}")

            logger.info(f"Created {len(self.STAGING_TABLES)} staging tables for run {run_id}")

        finally:
            conn.close()

    def drop_staging_tables(self, run_id: str) -> None:
        """
        Drop staging tables for a specific run.

        Called after successful merge to clean up.

        Args:
            run_id: Run identifier
        """
        conn = duckdb.connect(self.db_path)

        try:
            for table in self.STAGING_TABLES:
                staging_name = f"{table}_staging_{run_id}"
                conn.execute(f"DROP TABLE IF EXISTS {staging_name}")
                logger.debug(f"Dropped staging table: {staging_name}")

            logger.info(f"Cleaned up staging tables for run {run_id}")

        finally:
            conn.close()

    def get_staging_table_name(self, base_table: str, run_id: str) -> str:
        """
        Get the staging table name for a given base table and run.

        Args:
            base_table: Production table name (e.g., "sections")
            run_id: Run identifier

        Returns:
            Staging table name (e.g., "sections_staging_20240125_143022")
        """
        return f"{base_table}_staging_{run_id}"

    def get_staging_stats(self, run_id: str) -> dict:
        """
        Get row counts for staging tables.

        Args:
            run_id: Run identifier

        Returns:
            Dict mapping table names to row counts
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        stats = {}

        try:
            for table in self.STAGING_TABLES:
                staging_name = f"{table}_staging_{run_id}"
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {staging_name}").fetchone()[0]
                    stats[table] = count
                except duckdb.CatalogException:
                    # Table doesn't exist yet - expected for tables not yet populated
                    stats[table] = 0

            return stats

        finally:
            conn.close()

    def list_active_staging_runs(self) -> list[str]:
        """
        List all active staging run IDs.

        Returns:
            List of run IDs that have staging tables
        """
        conn = duckdb.connect(self.db_path, read_only=True)

        try:
            # Find all staging tables
            tables = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name LIKE '%_staging_%'
            """).fetchall()

            # Extract unique run IDs
            run_ids = set()
            for (table_name,) in tables:
                # Extract run_id from table name like "sections_staging_20240125_143022"
                parts = table_name.split("_staging_")
                if len(parts) == 2:
                    run_ids.add(parts[1])

            return sorted(run_ids)

        finally:
            conn.close()

    def cleanup_orphaned_staging(self) -> int:
        """
        Remove any orphaned staging tables from failed runs.

        Returns:
            Number of tables cleaned up
        """
        conn = duckdb.connect(self.db_path)
        cleaned = 0

        try:
            tables = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name LIKE '%_staging_%'
            """).fetchall()

            for (table_name,) in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                cleaned += 1
                logger.info(f"Cleaned up orphaned staging table: {table_name}")

            return cleaned

        finally:
            conn.close()
