"""
Merge Coordinator for Idempotent Data Pipeline.

Single-writer component that atomically merges staging data into
production tables. Uses DELETE + INSERT pattern for idempotency.

Industry patterns used:
- Single-writer principle (avoids lock contention)
- Idempotent operations (safe to re-run)
- Transactional integrity (atomic commits)
- Validation before commit (pre-commit hooks)
- Health checks integration
"""

import duckdb
from typing import Optional, Callable
from dataclasses import dataclass, field

from ..utils.logger import get_logger
from .staging_manager import StagingManager

logger = get_logger("finloom.storage.merge")


# Type alias for validation hooks
ValidationHook = Callable[[str, str, duckdb.DuckDBPyConnection], tuple[bool, list[str]]]


@dataclass
class MergeResult:
    """Result of a merge operation."""
    success: bool
    accession_number: str
    sections_merged: int = 0
    tables_merged: int = 0
    footnotes_merged: int = 0
    chunks_merged: int = 0
    error_message: Optional[str] = None


class MergeCoordinator:
    """
    Coordinates merging of staging data into production tables.

    This is a SINGLE-WRITER component - only one instance should
    be merging at a time to avoid lock contention in DuckDB.

    Merge Pattern:
    1. Run pre-commit validation hooks
    2. BEGIN TRANSACTION
    3. DELETE existing data for filing (idempotent)
    4. INSERT FROM staging tables
    5. UPDATE filing status
    6. COMMIT (or ROLLBACK on error)

    Pre-commit Hooks:
    - Validation hooks can be registered to run before merge
    - If any hook fails, merge is aborted
    - Hooks receive (accession_number, run_id, connection)
    """

    def __init__(self, db_path: str, strict_mode: bool = False):
        """
        Initialize merge coordinator.

        Args:
            db_path: Path to DuckDB database
            strict_mode: If True, treat warnings as errors
        """
        self.db_path = db_path
        self.staging_manager = StagingManager(db_path)
        self.strict_mode = strict_mode
        self._validation_hooks: list[ValidationHook] = []

        # Register default validation hooks
        self._register_default_hooks()

    def _register_default_hooks(self) -> None:
        """Register default validation hooks."""
        # Hook 1: Check for empty content
        self.register_validation_hook(self._validate_content_not_empty)

        # Hook 2: Check for duplicate section types in staging
        self.register_validation_hook(self._validate_no_duplicate_sections)

    def register_validation_hook(self, hook: ValidationHook) -> None:
        """
        Register a pre-commit validation hook.

        Args:
            hook: Function that takes (accession_number, run_id, connection)
                  and returns (is_valid, list of error messages)
        """
        self._validation_hooks.append(hook)
        logger.debug(f"Registered validation hook: {hook.__name__}")

    def _validate_content_not_empty(
        self,
        accession_number: str,
        run_id: str,
        conn: duckdb.DuckDBPyConnection
    ) -> tuple[bool, list[str]]:
        """Validate that sections have content."""
        errors = []
        sections_staging = self.staging_manager.get_staging_table_name("sections", run_id)

        try:
            empty_count = conn.execute(f"""
                SELECT COUNT(*) FROM {sections_staging}
                WHERE accession_number = ?
                AND (content_text IS NULL OR LENGTH(content_text) < 100)
            """, [accession_number]).fetchone()[0]

            if empty_count > 0:
                msg = f"{empty_count} sections have empty or very short content"
                if self.strict_mode:
                    errors.append(msg)
                else:
                    logger.warning(f"{accession_number}: {msg}")

        except Exception as e:
            # Table might not exist yet, which is OK
            pass

        return len(errors) == 0, errors

    def _validate_no_duplicate_sections(
        self,
        accession_number: str,
        run_id: str,
        conn: duckdb.DuckDBPyConnection
    ) -> tuple[bool, list[str]]:
        """Validate no duplicate section types in staging."""
        errors = []
        sections_staging = self.staging_manager.get_staging_table_name("sections", run_id)

        try:
            dup_count = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT section_type, COUNT(*)
                    FROM {sections_staging}
                    WHERE accession_number = ?
                    GROUP BY section_type
                    HAVING COUNT(*) > 1
                )
            """, [accession_number]).fetchone()[0]

            if dup_count > 0:
                errors.append(f"{dup_count} duplicate section types found in staging")

        except Exception as e:
            # Table might not exist yet, which is OK
            pass

        return len(errors) == 0, errors

    def merge_filing(
        self,
        accession_number: str,
        run_id: str,
        validate: bool = True
    ) -> MergeResult:
        """
        Atomically merge staging data for a single filing into production.

        This operation is IDEMPOTENT - safe to call multiple times.
        Existing data for the filing is deleted before inserting new data.

        Args:
            accession_number: Filing accession number
            run_id: Staging run identifier
            validate: Whether to validate data before merging

        Returns:
            MergeResult with counts and status
        """
        conn = duckdb.connect(self.db_path)

        try:
            # Validate staging data if requested
            if validate:
                validation_error = self._validate_staging_data(
                    conn, accession_number, run_id
                )
                if validation_error:
                    return MergeResult(
                        success=False,
                        accession_number=accession_number,
                        error_message=validation_error
                    )

            # Get staging table names
            sections_staging = self.staging_manager.get_staging_table_name("sections", run_id)
            tables_staging = self.staging_manager.get_staging_table_name("tables", run_id)
            footnotes_staging = self.staging_manager.get_staging_table_name("footnotes", run_id)
            chunks_staging = self.staging_manager.get_staging_table_name("chunks", run_id)

            # Begin atomic transaction
            conn.execute("BEGIN TRANSACTION")

            try:
                # Note: Section/table/footnote deletion removed in markdown-only architecture
                # Only chunks remain (if implemented in future)
                conn.execute(
                    "DELETE FROM chunks WHERE accession_number = ?",
                    [accession_number]
                )

                # Step 2: INSERT from staging tables (simplified - markdown only)
                # Note: sections_count, tables_count, footnotes_count no longer used
                sections_count = 0
                tables_count = 0
                footnotes_count = 0
                chunks_count = self._insert_from_staging(
                    conn, chunks_staging, "chunks", accession_number
                )

                # Step 3: Update filing status
                conn.execute("""
                    UPDATE filings
                    SET sections_processed = TRUE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE accession_number = ?
                """, [accession_number])

                # Step 4: COMMIT
                conn.execute("COMMIT")

                logger.info(
                    f"Merged {accession_number}: "
                    f"{sections_count} sections, {tables_count} tables, "
                    f"{footnotes_count} footnotes, {chunks_count} chunks"
                )

                return MergeResult(
                    success=True,
                    accession_number=accession_number,
                    sections_merged=sections_count,
                    tables_merged=tables_count,
                    footnotes_merged=footnotes_count,
                    chunks_merged=chunks_count
                )

            except Exception as e:
                conn.execute("ROLLBACK")
                logger.error(f"Merge failed for {accession_number}, rolled back: {e}")
                raise

        except Exception as e:
            return MergeResult(
                success=False,
                accession_number=accession_number,
                error_message=str(e)
            )

        finally:
            conn.close()

    def _insert_from_staging(
        self,
        conn: duckdb.DuckDBPyConnection,
        staging_table: str,
        production_table: str,
        accession_number: str
    ) -> int:
        """
        Insert data from staging table to production table.

        Returns:
            Number of rows inserted
        """
        try:
            # Check if staging table exists and has data
            count = conn.execute(f"""
                SELECT COUNT(*) FROM {staging_table}
                WHERE accession_number = ?
            """, [accession_number]).fetchone()[0]

            if count > 0:
                conn.execute(f"""
                    INSERT INTO {production_table}
                    SELECT * FROM {staging_table}
                    WHERE accession_number = ?
                """, [accession_number])

            return count

        except Exception as e:
            logger.warning(f"Could not insert from {staging_table}: {e}")
            return 0

    def _validate_staging_data(
        self,
        conn: duckdb.DuckDBPyConnection,
        accession_number: str,
        run_id: str
    ) -> Optional[str]:
        """
        Validate staging data before merge using registered hooks.

        Runs all registered validation hooks. If any hook fails,
        returns the combined error messages.

        Returns:
            Error message if validation fails, None if valid
        """
        sections_staging = self.staging_manager.get_staging_table_name("sections", run_id)
        all_errors = []

        try:
            # Basic check: staging table exists and has data
            section_count = conn.execute(f"""
                SELECT COUNT(*) FROM {sections_staging}
                WHERE accession_number = ?
            """, [accession_number]).fetchone()[0]

            if section_count == 0:
                return f"No sections found in staging for {accession_number}"

            # Run all registered validation hooks
            for hook in self._validation_hooks:
                try:
                    is_valid, errors = hook(accession_number, run_id, conn)
                    if not is_valid:
                        all_errors.extend(errors)
                except Exception as e:
                    logger.warning(f"Validation hook {hook.__name__} failed: {e}")
                    if self.strict_mode:
                        all_errors.append(f"Hook {hook.__name__} error: {e}")

            if all_errors:
                return "; ".join(all_errors)

            return None  # Validation passed

        except Exception as e:
            return f"Validation error: {e}"

    def run_pre_commit_checks(
        self,
        accession_number: str,
        run_id: str
    ) -> tuple[bool, list[str]]:
        """
        Run all pre-commit validation checks.

        This is a public method that can be called independently
        to validate staging data before merge.

        Args:
            accession_number: Filing accession number
            run_id: Staging run identifier

        Returns:
            Tuple of (is_valid, list of error/warning messages)
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        messages = []

        try:
            error = self._validate_staging_data(conn, accession_number, run_id)
            if error:
                messages.append(error)
                return False, messages

            return True, []

        finally:
            conn.close()

    def merge_all_from_run(
        self,
        run_id: str,
        validate: bool = True
    ) -> list[MergeResult]:
        """
        Merge all filings from a staging run into production.

        Args:
            run_id: Staging run identifier
            validate: Whether to validate before each merge

        Returns:
            List of MergeResult for each filing
        """
        conn = duckdb.connect(self.db_path, read_only=True)

        try:
            # Get all accession numbers in staging
            sections_staging = self.staging_manager.get_staging_table_name("sections", run_id)
            accession_numbers = conn.execute(f"""
                SELECT DISTINCT accession_number FROM {sections_staging}
            """).fetchall()

        finally:
            conn.close()

        results = []
        for (accession_number,) in accession_numbers:
            result = self.merge_filing(accession_number, run_id, validate)
            results.append(result)

        # Summary
        success_count = sum(1 for r in results if r.success)
        logger.info(
            f"Merge complete: {success_count}/{len(results)} filings merged successfully"
        )

        return results

    def cleanup_staging_after_merge(self, run_id: str) -> None:
        """
        Clean up staging tables after successful merge.

        Args:
            run_id: Staging run identifier
        """
        self.staging_manager.drop_staging_tables(run_id)
        logger.info(f"Cleaned up staging tables for run {run_id}")
