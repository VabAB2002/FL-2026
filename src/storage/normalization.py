"""
Normalization repository for standardized metrics, mappings, and data quality.

Handles standardized metric definitions, concept mappings, normalized financials,
duplicate detection, and data quality operations.
"""

from typing import List, Optional

import pandas as pd

from ..infrastructure.logger import get_logger
from .connection import Database

logger = get_logger("finloom.storage.normalization")


class NormalizationRepository:
    """Repository for data normalization and quality operations."""
    
    def __init__(self, db: Database):
        """
        Initialize repository with database connection.
        
        Args:
            db: Database instance to use for queries
        """
        self.db = db
    
    def upsert_standardized_metric(
        self,
        metric_id: str,
        metric_name: str,
        display_label: str,
        category: str,
        data_type: Optional[str] = None,
        description: Optional[str] = None,
        calculation_rule: Optional[str] = None,
    ) -> None:
        """Insert or update a standardized metric definition."""
        sql = """
            INSERT INTO standardized_metrics (
                metric_id, metric_name, display_label, category,
                data_type, description, calculation_rule, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (metric_id) DO UPDATE SET
                metric_name = EXCLUDED.metric_name,
                display_label = EXCLUDED.display_label,
                category = EXCLUDED.category,
                data_type = COALESCE(EXCLUDED.data_type, standardized_metrics.data_type),
                description = COALESCE(EXCLUDED.description, standardized_metrics.description),
                calculation_rule = COALESCE(EXCLUDED.calculation_rule, standardized_metrics.calculation_rule)
        """
        self.db.connection.execute(sql, [
            metric_id, metric_name, display_label, category,
            data_type, description, calculation_rule
        ])
    
    def insert_concept_mapping(
        self,
        metric_id: str,
        concept_name: str,
        priority: int,
        confidence_score: float = 1.0,
        applies_to_industry: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Insert a concept mapping."""
        id_result = self.db.connection.execute("SELECT nextval('concept_mappings_id_seq')").fetchone()
        mapping_id = id_result[0]
        
        sql = """
            INSERT INTO concept_mappings (
                mapping_id, metric_id, concept_name, priority,
                confidence_score, applies_to_industry, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (metric_id, concept_name) DO UPDATE SET
                priority = EXCLUDED.priority,
                confidence_score = EXCLUDED.confidence_score,
                applies_to_industry = COALESCE(EXCLUDED.applies_to_industry, concept_mappings.applies_to_industry),
                notes = COALESCE(EXCLUDED.notes, concept_mappings.notes)
        """
        self.db.connection.execute(sql, [
            mapping_id, metric_id, concept_name, priority,
            confidence_score, applies_to_industry, notes
        ])
        return mapping_id
    
    def get_latest_filing_per_period(
        self,
        ticker: Optional[str] = None,
        form_types: List[str] = None
    ) -> list:
        """
        Get the latest filing per fiscal period with priority logic.
        
        Uses the fiscal year from the facts data (MAX period_end year) to determine
        the fiscal period, since filing_date may be bulk-imported and period_of_report may be NULL.
        
        Priority:
        1. Amendments (10-K/A) over originals (10-K)
        2. Latest filing_date if multiple of same type
        
        Returns only ONE filing per (cik, fiscal_year)
        
        Args:
            ticker: Optional ticker to filter by company
            form_types: List of form types (defaults to ['10-K', '10-K/A'])
        
        Returns:
            List of tuples: (accession_number, cik, ticker, form_type, filing_date, fiscal_year)
        """
        if form_types is None:
            form_types = ['10-K', '10-K/A']
        
        sql = """
            WITH filing_years AS (
                SELECT 
                    f.accession_number,
                    f.cik,
                    f.form_type,
                    f.filing_date,
                    MAX(EXTRACT(YEAR FROM fa.period_end)) as fiscal_year
                FROM filings f
                LEFT JOIN facts fa ON f.accession_number = fa.accession_number
                WHERE f.xbrl_processed = TRUE
                  AND f.form_type IN ("""
        
        # Add placeholders for form_types
        placeholders = ', '.join(['?' for _ in form_types])
        sql += placeholders + ")"
        
        params = form_types.copy()
        
        sql += """
                GROUP BY f.accession_number, f.cik, f.form_type, f.filing_date
            ),
            ranked_filings AS (
                SELECT 
                    fy.accession_number,
                    fy.cik,
                    c.ticker,
                    fy.form_type,
                    fy.filing_date,
                    fy.fiscal_year,
                    ROW_NUMBER() OVER (
                        PARTITION BY fy.cik, fy.fiscal_year
                        ORDER BY 
                            CASE 
                                WHEN fy.form_type LIKE '%/A' THEN 1
                                ELSE 2
                            END ASC,
                            fy.filing_date DESC
                    ) as priority_rank
                FROM filing_years fy
                JOIN companies c ON fy.cik = c.cik
                WHERE fy.fiscal_year IS NOT NULL
        """
        
        if ticker:
            sql += " AND c.ticker = ?"
            params.append(ticker)
        
        sql += """
            )
            SELECT accession_number, cik, ticker, form_type, filing_date, fiscal_year
            FROM ranked_filings
            WHERE priority_rank = 1
            ORDER BY fiscal_year DESC
        """
        
        return self.db.connection.execute(sql, params).fetchall()
    
    def get_concept_mappings(self, metric_id: Optional[str] = None) -> list[dict]:
        """Get concept mappings, optionally filtered by metric."""
        if metric_id:
            sql = "SELECT * FROM concept_mappings WHERE metric_id = ? ORDER BY priority"
            results = self.db.connection.execute(sql, [metric_id]).fetchall()
        else:
            sql = "SELECT * FROM concept_mappings ORDER BY metric_id, priority"
            results = self.db.connection.execute(sql).fetchall()
        
        columns = [desc[0] for desc in self.db.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def insert_normalized_metric(
        self,
        company_ticker: str,
        fiscal_year: int,
        metric_id: str,
        metric_value: float,
        source_concept: Optional[str] = None,
        source_accession: Optional[str] = None,
        confidence_score: float = 1.0,
        fiscal_quarter: Optional[int] = None,
    ) -> int:
        """
        Insert or update a normalized metric value.
        
        Uses check-then-upsert pattern to avoid duplicate records when
        confidence scores are equal.
        """
        
        # Check if record already exists
        existing = self.db.connection.execute("""
            SELECT id, confidence_score
            FROM normalized_financials
            WHERE company_ticker = ?
              AND fiscal_year = ?
              AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
              AND metric_id = ?
        """, [company_ticker, fiscal_year, fiscal_quarter, metric_id]).fetchone()
        
        if existing:
            existing_id, existing_confidence = existing
            
            # Only update if new confidence is higher or equal
            if confidence_score >= existing_confidence:
                self.db.connection.execute("""
                    UPDATE normalized_financials
                    SET metric_value = ?,
                        source_concept = ?,
                        source_accession = ?,
                        confidence_score = ?,
                        created_at = now()
                    WHERE id = ?
                """, [metric_value, source_concept, source_accession, 
                      confidence_score, existing_id])
                
                logger.debug(f"Updated metric {metric_id} for {company_ticker} "
                            f"FY{fiscal_year} (confidence: {existing_confidence:.2f} -> {confidence_score:.2f})")
            else:
                logger.debug(f"Skipped update for {metric_id} {company_ticker} "
                            f"FY{fiscal_year} (confidence {confidence_score:.2f} < {existing_confidence:.2f})")
            
            return existing_id
        else:
            # Insert new record
            norm_id = self.db.connection.execute(
                "SELECT nextval('normalized_financials_id_seq')"
            ).fetchone()[0]
            
            self.db.connection.execute("""
                INSERT INTO normalized_financials (
                    id, company_ticker, fiscal_year, fiscal_quarter, metric_id,
                    metric_value, source_concept, source_accession,
                    confidence_score, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            """, [norm_id, company_ticker, fiscal_year, fiscal_quarter, metric_id,
                  metric_value, source_concept, source_accession, confidence_score])
            
            logger.debug(f"Inserted new metric {metric_id} for {company_ticker} FY{fiscal_year}")
            
            return norm_id
    
    def get_normalized_metrics(
        self,
        ticker: Optional[str] = None,
        fiscal_year: Optional[int] = None,
        metric_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get normalized metrics with optional filters."""
        sql = "SELECT * FROM normalized_metrics_view WHERE 1=1"
        params = []
        
        if ticker:
            sql += " AND company_ticker = ?"
            params.append(ticker)
        if fiscal_year:
            sql += " AND fiscal_year = ?"
            params.append(fiscal_year)
        if metric_id:
            sql += " AND metric_id = ?"
            params.append(metric_id)
        
        sql += " ORDER BY company_ticker, fiscal_year DESC, metric_id"
        
        return self.db.connection.execute(sql, params).df()
    
    def detect_duplicates(
        self,
        table: str = "normalized_financials"
    ) -> list[dict]:
        """
        Detect duplicate records in specified table.
        
        For normalized_financials, duplicates are defined as multiple records
        with the same (company_ticker, fiscal_year, fiscal_quarter, metric_id).
        
        Args:
            table: Table name to check for duplicates
        
        Returns:
            List of duplicate group dictionaries with metadata
        
        Example:
            duplicates = repo.detect_duplicates("normalized_financials")
            for dup in duplicates:
                print(f"{dup['ticker']} {dup['year']} {dup['metric']}: {dup['count']} entries")
        """
        if table == "normalized_financials":
            # Find duplicate groups
            results = self.db.connection.execute("""
                SELECT 
                    company_ticker, 
                    fiscal_year, 
                    fiscal_quarter, 
                    metric_id,
                    COUNT(*) as count
                FROM normalized_financials
                GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
                HAVING COUNT(*) > 1
                ORDER BY count DESC, company_ticker, fiscal_year DESC
            """).fetchall()
            
            duplicates = []
            for ticker, year, quarter, metric, count in results:
                # Get details of all duplicate records in this group
                records = self.db.connection.execute("""
                    SELECT id, confidence_score, created_at, metric_value
                    FROM normalized_financials
                    WHERE company_ticker = ?
                      AND fiscal_year = ?
                      AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
                      AND metric_id = ?
                    ORDER BY confidence_score DESC, created_at DESC
                """, [ticker, year, quarter, metric]).fetchall()
                
                duplicates.append({
                    "table": table,
                    "ticker": ticker,
                    "year": year,
                    "quarter": quarter,
                    "metric": metric,
                    "count": count,
                    "records": [
                        {
                            "id": r[0],
                            "confidence": r[1],
                            "created_at": r[2],
                            "value": r[3],
                            "keep": i == 0  # First (best) record should be kept
                        }
                        for i, r in enumerate(records)
                    ]
                })
            
            return duplicates
        else:
            raise ValueError(f"Duplicate detection not implemented for table: {table}")
    
    def remove_duplicates(
        self,
        table: str = "normalized_financials",
        dry_run: bool = True
    ) -> dict:
        """
        Remove duplicate records, keeping the best one per group.
        
        For normalized_financials:
        - Keeps record with highest confidence_score
        - If tied, keeps most recent (created_at DESC)
        - Deletes all others
        
        Args:
            table: Table name to clean
            dry_run: If True, only reports what would be deleted (no actual deletion)
        
        Returns:
            Dict with statistics: duplicate_groups, records_removed, records_kept
        
        Example:
            # Preview what would be deleted
            stats = repo.remove_duplicates("normalized_financials", dry_run=True)
            print(f"Would remove {stats['records_removed']} duplicate records")
            
            # Actually delete
            stats = repo.remove_duplicates("normalized_financials", dry_run=False)
            print(f"Removed {stats['records_removed']} duplicates")
        """
        if table != "normalized_financials":
            raise ValueError(f"Duplicate removal not implemented for table: {table}")
        
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Detecting duplicates in {table}...")
        
        # Detect all duplicates
        duplicates = self.detect_duplicates(table)
        
        if not duplicates:
            logger.info("No duplicates found!")
            return {
                "duplicate_groups": 0,
                "records_removed": 0,
                "records_kept": 0
            }
        
        logger.info(f"Found {len(duplicates)} duplicate groups")
        
        total_removed = 0
        total_kept = len(duplicates)  # One kept per group
        
        if not dry_run:
            # Start transaction for safety
            self.db.connection.execute("BEGIN TRANSACTION")
        
        try:
            for dup in duplicates:
                ticker = dup["ticker"]
                year = dup["year"]
                quarter = dup["quarter"]
                metric = dup["metric"]
                count = dup["count"]
                
                # Get the best record to keep
                keeper = self.db.connection.execute("""
                    SELECT id
                    FROM normalized_financials
                    WHERE company_ticker = ?
                      AND fiscal_year = ?
                      AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
                      AND metric_id = ?
                    ORDER BY confidence_score DESC, created_at DESC
                    LIMIT 1
                """, [ticker, year, quarter, metric]).fetchone()
                
                keeper_id = keeper[0]
                
                # Delete all others
                if not dry_run:
                    self.db.connection.execute("""
                        DELETE FROM normalized_financials
                        WHERE company_ticker = ?
                          AND fiscal_year = ?
                          AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
                          AND metric_id = ?
                          AND id != ?
                    """, [ticker, year, quarter, metric, keeper_id])
                
                removed = count - 1
                total_removed += removed
                
                log_msg = f"{'Would remove' if dry_run else 'Removed'} {removed} duplicate(s) for {ticker} {year} Q{quarter or 'N/A'} {metric} (kept id={keeper_id})"
                logger.info(f"  {log_msg}")
            
            if not dry_run:
                # Commit transaction
                self.db.connection.execute("COMMIT")
                logger.info(f"Successfully removed {total_removed} duplicate records")
            else:
                logger.info(f"Would remove {total_removed} duplicate records (dry run)")
            
            return {
                "duplicate_groups": len(duplicates),
                "records_removed": total_removed,
                "records_kept": total_kept
            }
            
        except Exception as e:
            if not dry_run:
                self.db.connection.execute("ROLLBACK")
                logger.error(f"Failed to remove duplicates, transaction rolled back: {e}")
            raise
