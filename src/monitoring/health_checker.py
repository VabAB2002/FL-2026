"""
Database Health Checker for FinLoom.

Provides database-level monitoring and quality checks:
- Duplicate detection across tables
- Referential integrity validation
- Data completeness statistics
- Pre-commit validation hooks

Industry patterns used:
- Health check endpoints (like /health in microservices)
- Data quality scoring
- Automated anomaly detection
"""

import duckdb
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from ..utils.logger import get_logger

logger = get_logger("finloom.monitoring.health")


@dataclass
class DuplicateReport:
    """Report of duplicates found in a table."""
    table_name: str
    unique_columns: list[str]
    duplicate_count: int
    sample_duplicates: list[dict] = field(default_factory=list)

    @property
    def has_duplicates(self) -> bool:
        return self.duplicate_count > 0


@dataclass
class IntegrityReport:
    """Report of referential integrity issues."""
    orphan_sections: int = 0
    orphan_tables: int = 0
    orphan_footnotes: int = 0
    orphan_chunks: int = 0

    @property
    def has_issues(self) -> bool:
        return (self.orphan_sections + self.orphan_tables +
                self.orphan_footnotes + self.orphan_chunks) > 0

    @property
    def total_orphans(self) -> int:
        return (self.orphan_sections + self.orphan_tables +
                self.orphan_footnotes + self.orphan_chunks)


@dataclass
class CompletenessReport:
    """Report of data completeness."""
    total_filings: int
    filings_with_sections: int
    filings_with_tables: int
    filings_with_footnotes: int
    filings_with_chunks: int

    total_sections: int = 0
    total_tables: int = 0
    total_footnotes: int = 0
    total_chunks: int = 0

    database_size_mb: float = 0.0

    @property
    def sections_coverage(self) -> float:
        return (self.filings_with_sections / self.total_filings * 100
                if self.total_filings > 0 else 0)

    @property
    def tables_coverage(self) -> float:
        return (self.filings_with_tables / self.total_filings * 100
                if self.total_filings > 0 else 0)

    @property
    def footnotes_coverage(self) -> float:
        return (self.filings_with_footnotes / self.total_filings * 100
                if self.total_filings > 0 else 0)

    @property
    def chunks_coverage(self) -> float:
        return (self.filings_with_chunks / self.total_filings * 100
                if self.total_filings > 0 else 0)


@dataclass
class HealthReport:
    """Complete database health report."""
    timestamp: datetime
    status: str  # "healthy", "warning", "critical"

    completeness: CompletenessReport
    integrity: IntegrityReport
    duplicates: dict[str, DuplicateReport] = field(default_factory=dict)

    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def is_healthy(self) -> bool:
        return self.status == "healthy"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "status": self.status,
            "completeness": {
                "total_filings": self.completeness.total_filings,
                "sections_coverage": f"{self.completeness.sections_coverage:.1f}%",
                "tables_coverage": f"{self.completeness.tables_coverage:.1f}%",
                "footnotes_coverage": f"{self.completeness.footnotes_coverage:.1f}%",
                "chunks_coverage": f"{self.completeness.chunks_coverage:.1f}%",
                "database_size_mb": f"{self.completeness.database_size_mb:.1f}",
            },
            "integrity": {
                "has_issues": self.integrity.has_issues,
                "total_orphans": self.integrity.total_orphans,
            },
            "duplicates": {
                name: {
                    "count": report.duplicate_count,
                    "columns": report.unique_columns,
                }
                for name, report in self.duplicates.items()
            },
            "warnings": self.warnings,
            "errors": self.errors,
        }


class DatabaseHealthChecker:
    """
    Checks database health and data quality.

    Provides three types of checks:
    1. Duplicate detection - finds duplicate records
    2. Referential integrity - finds orphaned records
    3. Completeness - checks data coverage

    Usage:
        checker = DatabaseHealthChecker("data/database/finloom.duckdb")
        report = checker.full_health_check()

        if not report.is_healthy:
            print(f"Issues found: {report.warnings + report.errors}")
    """

    # Define unique constraints for each table
    UNIQUE_CONSTRAINTS = {
        "sections": ["accession_number", "section_type"],
        "tables": ["accession_number", "table_index"],
        "footnotes": ["accession_number", "footnote_id"],
        "chunks": ["accession_number", "chunk_index"],
    }

    def __init__(self, db_path: str):
        """
        Initialize health checker.

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = db_path

    def check_no_duplicates(
        self,
        table: str,
        unique_cols: Optional[list[str]] = None,
        return_samples: bool = True,
        sample_limit: int = 5
    ) -> DuplicateReport:
        """
        Check for duplicate records in a table.

        Args:
            table: Table name to check
            unique_cols: Columns that should be unique together.
                        If None, uses predefined constraints.
            return_samples: Whether to return sample duplicates
            sample_limit: Max number of sample duplicates to return

        Returns:
            DuplicateReport with duplicate count and samples
        """
        if unique_cols is None:
            unique_cols = self.UNIQUE_CONSTRAINTS.get(table, ["id"])

        cols_str = ", ".join(unique_cols)

        conn = duckdb.connect(self.db_path, read_only=True)

        try:
            # Count duplicates
            count_query = f"""
                SELECT COUNT(*) as dup_count
                FROM (
                    SELECT {cols_str}, COUNT(*) as cnt
                    FROM {table}
                    GROUP BY {cols_str}
                    HAVING COUNT(*) > 1
                )
            """
            dup_count = conn.execute(count_query).fetchone()[0]

            # Get sample duplicates if requested
            samples = []
            if return_samples and dup_count > 0:
                sample_query = f"""
                    SELECT {cols_str}, COUNT(*) as duplicate_count
                    FROM {table}
                    GROUP BY {cols_str}
                    HAVING COUNT(*) > 1
                    ORDER BY COUNT(*) DESC
                    LIMIT {sample_limit}
                """
                results = conn.execute(sample_query).fetchall()
                columns = unique_cols + ["duplicate_count"]
                samples = [dict(zip(columns, row)) for row in results]

            return DuplicateReport(
                table_name=table,
                unique_columns=unique_cols,
                duplicate_count=dup_count,
                sample_duplicates=samples
            )

        finally:
            conn.close()

    def check_referential_integrity(self) -> IntegrityReport:
        """
        Check for orphaned records (records without parent filing).

        Returns:
            IntegrityReport with counts of orphaned records
        """
        conn = duckdb.connect(self.db_path, read_only=True)

        try:
            # Check orphan sections
            orphan_sections = conn.execute("""
                SELECT COUNT(*) FROM sections s
                LEFT JOIN filings f ON s.accession_number = f.accession_number
                WHERE f.accession_number IS NULL
            """).fetchone()[0]

            # Check orphan tables
            orphan_tables = conn.execute("""
                SELECT COUNT(*) FROM tables t
                LEFT JOIN filings f ON t.accession_number = f.accession_number
                WHERE f.accession_number IS NULL
            """).fetchone()[0]

            # Check orphan footnotes
            orphan_footnotes = conn.execute("""
                SELECT COUNT(*) FROM footnotes fn
                LEFT JOIN filings f ON fn.accession_number = f.accession_number
                WHERE f.accession_number IS NULL
            """).fetchone()[0]

            # Check orphan chunks
            orphan_chunks = conn.execute("""
                SELECT COUNT(*) FROM chunks c
                LEFT JOIN filings f ON c.accession_number = f.accession_number
                WHERE f.accession_number IS NULL
            """).fetchone()[0]

            return IntegrityReport(
                orphan_sections=orphan_sections,
                orphan_tables=orphan_tables,
                orphan_footnotes=orphan_footnotes,
                orphan_chunks=orphan_chunks
            )

        finally:
            conn.close()

    def check_completeness(self) -> CompletenessReport:
        """
        Check data completeness and coverage statistics.

        Returns:
            CompletenessReport with coverage percentages
        """
        conn = duckdb.connect(self.db_path, read_only=True)

        try:
            # Total filings
            total_filings = conn.execute("""
                SELECT COUNT(*) FROM filings
                WHERE download_status = 'completed'
            """).fetchone()[0]

            # Filings with sections
            filings_with_sections = conn.execute("""
                SELECT COUNT(DISTINCT accession_number) FROM sections
            """).fetchone()[0]

            # Filings with tables
            filings_with_tables = conn.execute("""
                SELECT COUNT(DISTINCT accession_number) FROM tables
            """).fetchone()[0]

            # Filings with footnotes
            filings_with_footnotes = conn.execute("""
                SELECT COUNT(DISTINCT accession_number) FROM footnotes
            """).fetchone()[0]

            # Filings with chunks
            filings_with_chunks = conn.execute("""
                SELECT COUNT(DISTINCT accession_number) FROM chunks
            """).fetchone()[0]

            # Total counts
            total_sections = conn.execute("SELECT COUNT(*) FROM sections").fetchone()[0]
            total_tables = conn.execute("SELECT COUNT(*) FROM tables").fetchone()[0]
            total_footnotes = conn.execute("SELECT COUNT(*) FROM footnotes").fetchone()[0]
            total_chunks = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]

            # Database size (approximate from file)
            import os
            db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0

            return CompletenessReport(
                total_filings=total_filings,
                filings_with_sections=filings_with_sections,
                filings_with_tables=filings_with_tables,
                filings_with_footnotes=filings_with_footnotes,
                filings_with_chunks=filings_with_chunks,
                total_sections=total_sections,
                total_tables=total_tables,
                total_footnotes=total_footnotes,
                total_chunks=total_chunks,
                database_size_mb=db_size_mb
            )

        finally:
            conn.close()

    def full_health_check(self) -> HealthReport:
        """
        Run complete health check on database.

        Returns:
            HealthReport with all checks combined
        """
        timestamp = datetime.now()
        warnings = []
        errors = []

        # Check completeness
        completeness = self.check_completeness()

        # Check for incomplete coverage
        if completeness.sections_coverage < 100:
            warnings.append(
                f"Sections coverage is {completeness.sections_coverage:.1f}% "
                f"({completeness.filings_with_sections}/{completeness.total_filings})"
            )
        if completeness.tables_coverage < 100:
            warnings.append(
                f"Tables coverage is {completeness.tables_coverage:.1f}% "
                f"({completeness.filings_with_tables}/{completeness.total_filings})"
            )

        # Check integrity
        integrity = self.check_referential_integrity()

        if integrity.has_issues:
            errors.append(
                f"Found {integrity.total_orphans} orphaned records "
                f"(sections: {integrity.orphan_sections}, "
                f"tables: {integrity.orphan_tables}, "
                f"footnotes: {integrity.orphan_footnotes}, "
                f"chunks: {integrity.orphan_chunks})"
            )

        # Check duplicates for all tables
        duplicates = {}
        for table in self.UNIQUE_CONSTRAINTS.keys():
            try:
                report = self.check_no_duplicates(table)
                duplicates[table] = report

                if report.has_duplicates:
                    errors.append(
                        f"Found {report.duplicate_count} duplicate combinations "
                        f"in {table} table"
                    )
            except Exception as e:
                warnings.append(f"Could not check duplicates in {table}: {e}")

        # Determine overall status
        if errors:
            status = "critical"
        elif warnings:
            status = "warning"
        else:
            status = "healthy"

        return HealthReport(
            timestamp=timestamp,
            status=status,
            completeness=completeness,
            integrity=integrity,
            duplicates=duplicates,
            warnings=warnings,
            errors=errors
        )

    def validate_staging_data(
        self,
        run_id: str,
        accession_number: str
    ) -> tuple[bool, list[str]]:
        """
        Validate staging data before merge (pre-commit hook).

        Args:
            run_id: Staging run identifier
            accession_number: Filing accession number

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        conn = duckdb.connect(self.db_path, read_only=True)

        try:
            sections_staging = f"sections_staging_{run_id}"

            # Check staging table exists
            tables = conn.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_name = ?
            """, [sections_staging]).fetchall()

            if not tables:
                errors.append(f"Staging table {sections_staging} does not exist")
                return False, errors

            # Check we have sections
            section_count = conn.execute(f"""
                SELECT COUNT(*) FROM {sections_staging}
                WHERE accession_number = ?
            """, [accession_number]).fetchone()[0]

            if section_count == 0:
                errors.append(f"No sections found in staging for {accession_number}")

            # Check for empty content
            empty_count = conn.execute(f"""
                SELECT COUNT(*) FROM {sections_staging}
                WHERE accession_number = ?
                AND (content_text IS NULL OR LENGTH(content_text) < 100)
            """, [accession_number]).fetchone()[0]

            if empty_count > 0:
                errors.append(f"{empty_count} sections have empty or very short content")

            # Check for duplicate section types in staging
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

            return len(errors) == 0, errors

        except Exception as e:
            errors.append(f"Validation error: {e}")
            return False, errors

        finally:
            conn.close()

    def get_filing_health(self, accession_number: str) -> dict:
        """
        Get health status for a specific filing.

        Args:
            accession_number: Filing accession number

        Returns:
            Dict with filing health details
        """
        conn = duckdb.connect(self.db_path, read_only=True)

        try:
            # Get counts for this filing
            sections = conn.execute("""
                SELECT COUNT(*) FROM sections WHERE accession_number = ?
            """, [accession_number]).fetchone()[0]

            tables = conn.execute("""
                SELECT COUNT(*) FROM tables WHERE accession_number = ?
            """, [accession_number]).fetchone()[0]

            footnotes = conn.execute("""
                SELECT COUNT(*) FROM footnotes WHERE accession_number = ?
            """, [accession_number]).fetchone()[0]

            chunks = conn.execute("""
                SELECT COUNT(*) FROM chunks WHERE accession_number = ?
            """, [accession_number]).fetchone()[0]

            # Check for duplicate sections
            dup_sections = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT section_type, COUNT(*)
                    FROM sections
                    WHERE accession_number = ?
                    GROUP BY section_type
                    HAVING COUNT(*) > 1
                )
            """, [accession_number]).fetchone()[0]

            return {
                "accession_number": accession_number,
                "sections": sections,
                "tables": tables,
                "footnotes": footnotes,
                "chunks": chunks,
                "has_duplicates": dup_sections > 0,
                "is_complete": sections > 0 and tables > 0,
            }

        finally:
            conn.close()
    
    def verify_system_integrity(self) -> dict:
        """
        Comprehensive system verification for production readiness.
        
        Checks:
        1. Database schema (required tables exist)
        2. Extraction progress (filings processed, sections, chunks, footnotes)
        3. Top processed companies
        4. Quality metrics (average quality scores)
        5. Metadata features (tables, lists, parts)
        6. Hierarchical chunking distribution
        
        Returns:
            Dict with comprehensive system statistics and health status
        
        Example:
            checker = DatabaseHealthChecker(db_path)
            report = checker.verify_system_integrity()
            
            if report['status'] == 'healthy':
                print("✅ System is production ready!")
            else:
                print(f"⚠️ Issues: {report['issues']}")
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        
        try:
            result = {
                'status': 'healthy',
                'issues': [],
                'warnings': []
            }
            
            # 1. Schema Verification
            required_tables = ['sections', 'tables', 'footnotes', 'chunks', 'filings', 'companies']
            existing_tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            existing_table_names = {t[0] for t in existing_tables}
            
            missing_tables = [t for t in required_tables if t not in existing_table_names]
            if missing_tables:
                result['issues'].append(f"Missing required tables: {', '.join(missing_tables)}")
                result['status'] = 'critical'
            
            result['schema'] = {
                'required_tables': required_tables,
                'existing_tables': sorted(list(existing_table_names)),
                'missing_tables': missing_tables
            }
            
            # 2. Extraction Stats
            stats = conn.execute("""
                SELECT 
                    COUNT(DISTINCT f.accession_number) as total_filings,
                    COUNT(DISTINCT CASE WHEN f.sections_processed THEN f.accession_number END) as processed_filings,
                    COUNT(DISTINCT s.accession_number) as filings_with_sections,
                    COUNT(s.id) as total_sections,
                    COUNT(t.id) as total_tables,
                    COUNT(fn.id) as total_footnotes,
                    COUNT(c.chunk_id) as total_chunks
                FROM filings f
                LEFT JOIN sections s ON f.accession_number = s.accession_number
                LEFT JOIN tables t ON f.accession_number = t.accession_number
                LEFT JOIN footnotes fn ON f.accession_number = fn.accession_number
                LEFT JOIN chunks c ON f.accession_number = c.accession_number
                WHERE f.download_status = 'completed'
            """).fetchone()
            
            total_filings = stats[0] or 0
            processed_filings = stats[1] or 0
            filings_with_sections = stats[2] or 0
            
            processing_rate = (processed_filings / total_filings * 100) if total_filings > 0 else 0
            section_rate = (filings_with_sections / total_filings * 100) if total_filings > 0 else 0
            
            result['extraction'] = {
                'total_filings': total_filings,
                'processed_filings': processed_filings,
                'filings_with_sections': filings_with_sections,
                'processing_rate': round(processing_rate, 1),
                'section_rate': round(section_rate, 1),
                'total_sections': stats[3] or 0,
                'total_tables': stats[4] or 0,
                'total_footnotes': stats[5] or 0,
                'total_chunks': stats[6] or 0
            }
            
            # Warn if processing rate is low
            if total_filings > 0 and section_rate < 80:
                result['warnings'].append(
                    f"Section extraction coverage is {section_rate:.1f}% "
                    f"({filings_with_sections}/{total_filings} filings)"
                )
                if result['status'] == 'healthy':
                    result['status'] = 'warning'
            
            # 3. Top Companies
            companies = conn.execute("""
                SELECT c.ticker, c.company_name, COUNT(DISTINCT s.accession_number) as processed_count
                FROM companies c
                JOIN filings f ON c.cik = f.cik
                LEFT JOIN sections s ON f.accession_number = s.accession_number
                WHERE f.download_status = 'completed'
                GROUP BY c.ticker, c.company_name
                HAVING COUNT(DISTINCT s.accession_number) > 0
                ORDER BY processed_count DESC
                LIMIT 10
            """).fetchall()
            
            result['top_companies'] = [
                {
                    'ticker': ticker,
                    'name': name,
                    'processed_filings': count
                }
                for ticker, name, count in companies
            ]
            
            # 4. Quality Metrics
            quality = conn.execute("""
                SELECT 
                    AVG(extraction_confidence) as avg_confidence,
                    COUNT(*) as total_with_confidence,
                    MIN(extraction_confidence) as min_confidence,
                    MAX(extraction_confidence) as max_confidence
                FROM sections
                WHERE extraction_confidence IS NOT NULL AND extraction_confidence > 0
            """).fetchone()
            
            if quality[0] is not None:
                result['quality'] = {
                    'avg_confidence': round(quality[0], 3),
                    'scored_sections': quality[1],
                    'min_confidence': round(quality[2], 3),
                    'max_confidence': round(quality[3], 3)
                }
                
                # Warn if average quality is low
                if quality[0] < 0.7:
                    result['warnings'].append(
                        f"Average extraction confidence is low: {quality[0]:.2f}"
                    )
                    if result['status'] == 'healthy':
                        result['status'] = 'warning'
            else:
                result['quality'] = None
            
            # 5. Feature Verification
            features = conn.execute("""
                SELECT 
                    SUM(CASE WHEN section_part IS NOT NULL AND section_part != '' THEN 1 ELSE 0 END) as with_parts,
                    SUM(CASE WHEN contains_tables THEN 1 ELSE 0 END) as with_tables,
                    SUM(CASE WHEN contains_lists THEN 1 ELSE 0 END) as with_lists,
                    COUNT(*) as total_sections
                FROM sections
            """).fetchone()
            
            total_sections = features[3] or 0
            if total_sections > 0:
                result['features'] = {
                    'sections_with_parts': features[0] or 0,
                    'sections_with_tables': features[1] or 0,
                    'sections_with_lists': features[2] or 0,
                    'total_sections': total_sections,
                    'parts_rate': round((features[0] or 0) / total_sections * 100, 1),
                    'tables_rate': round((features[1] or 0) / total_sections * 100, 1),
                    'lists_rate': round((features[2] or 0) / total_sections * 100, 1)
                }
            else:
                result['features'] = None
            
            # 6. Hierarchical Chunking
            chunk_levels = conn.execute("""
                SELECT chunk_level, COUNT(*) as count
                FROM chunks
                GROUP BY chunk_level
                ORDER BY chunk_level
            """).fetchall()
            
            level_names = {1: 'Section', 2: 'Topic', 3: 'Paragraph'}
            result['chunking'] = [
                {
                    'level': level,
                    'name': level_names.get(level, f'Level {level}'),
                    'count': count
                }
                for level, count in chunk_levels
            ]
            
            # 7. Database Health
            import os
            db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            
            result['database'] = {
                'path': self.db_path,
                'size_mb': round(db_size_mb, 2),
                'read_only': conn.execute("PRAGMA database_list").fetchone()[2] == 1
            }
            
            return result
            
        except Exception as e:
            logger.error(f"System verification failed: {e}", exc_info=True)
            return {
                'status': 'critical',
                'issues': [f"Verification failed: {str(e)}"],
                'warnings': [],
                'error': str(e)
            }
        
        finally:
            conn.close()