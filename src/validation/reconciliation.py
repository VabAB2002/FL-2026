"""
Data reconciliation engine for detecting data drift and corruption.

Performs automated data quality checks and reconciliation.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from ..storage.database import Database
from ..utils.logger import get_logger

logger = get_logger("finloom.validation.reconciliation")


@dataclass
class ReconciliationIssue:
    """Represents a data reconciliation issue."""
    issue_type: str
    severity: str  # info, warning, error, critical
    description: str
    affected_records: int
    details: Optional[Dict] = None


class ReconciliationEngine:
    """
    Automated data reconciliation engine.
    
    Detects data drift, corruption, and inconsistencies across the system.
    """
    
    def __init__(self, db: Database):
        """
        Initialize reconciliation engine.
        
        Args:
            db: Database instance.
        """
        self.db = db
        self.issues: List[ReconciliationIssue] = []
    
    def run_all_checks(self) -> Dict:
        """
        Run all reconciliation checks.
        
        Returns:
            Dict with summary and issues.
        """
        logger.info("Starting data reconciliation")
        self.issues = []
        
        checks = [
            ("Filing Counts", self.reconcile_filing_counts),
            ("Facts to Filings", self.reconcile_facts_to_filings),
            ("Normalized to Raw", self.reconcile_normalized_to_raw),
            ("Duplicate Detection", self.check_duplicates),
            ("Data Completeness", self.check_data_completeness),
            ("Referential Integrity", self.check_referential_integrity),
        ]
        
        results = {}
        for check_name, check_func in checks:
            try:
                logger.info(f"Running check: {check_name}")
                result = check_func()
                results[check_name] = result
            except Exception as e:
                logger.error(f"Check '{check_name}' failed: {e}")
                results[check_name] = {"error": str(e)}
                self.issues.append(ReconciliationIssue(
                    issue_type="check_failed",
                    severity="error",
                    description=f"Reconciliation check '{check_name}' failed",
                    affected_records=0,
                    details={"error": str(e)}
                ))
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_checks": len(checks),
            "total_issues": len(self.issues),
            "critical_issues": sum(1 for i in self.issues if i.severity == "critical"),
            "error_issues": sum(1 for i in self.issues if i.severity == "error"),
            "warning_issues": sum(1 for i in self.issues if i.severity == "warning"),
            "results": results,
            "issues": [vars(i) for i in self.issues]
        }
        
        logger.info(f"Reconciliation complete: {len(self.issues)} issues found")
        return summary
    
    def reconcile_filing_counts(self) -> Dict:
        """Check that filing counts match expected."""
        logger.debug("Reconciling filing counts")
        results = {}
        
        # Expected: Each company should have ~10 years of 10-Ks
        companies = self.db.connection.execute("""
            SELECT cik, ticker, company_name
            FROM companies
            ORDER BY ticker
        """).fetchall()
        
        for cik, ticker, name in companies:
            filing_count = self.db.connection.execute("""
                SELECT COUNT(*) FROM filings
                WHERE cik = ? AND form_type IN ('10-K', '10-K/A')
                  AND xbrl_processed = TRUE
            """, [cik]).fetchone()[0]
            
            expected_count = 10
            results[ticker] = {
                "expected": expected_count,
                "actual": filing_count,
                "status": "ok" if filing_count >= expected_count else "missing_filings",
                "gap": max(0, expected_count - filing_count)
            }
            
            if filing_count < expected_count:
                self.issues.append(ReconciliationIssue(
                    issue_type="missing_filings",
                    severity="warning",
                    description=f"{ticker} has {filing_count}/{expected_count} expected filings",
                    affected_records=expected_count - filing_count,
                    details={"ticker": ticker, "cik": cik}
                ))
        
        return results
    
    def reconcile_facts_to_filings(self) -> Dict:
        """Ensure every processed filing has facts."""
        logger.debug("Reconciling facts to filings")
        
        orphaned = self.db.connection.execute("""
            SELECT f.accession_number, f.cik, c.ticker, f.filing_date
            FROM filings f
            JOIN companies c ON f.cik = c.cik
            LEFT JOIN facts fa ON f.accession_number = fa.accession_number
            WHERE f.xbrl_processed = TRUE
            GROUP BY f.accession_number, f.cik, c.ticker, f.filing_date
            HAVING COUNT(fa.id) = 0
            ORDER BY f.filing_date DESC
        """).fetchall()
        
        if orphaned:
            self.issues.append(ReconciliationIssue(
                issue_type="orphaned_filings",
                severity="error",
                description=f"Found {len(orphaned)} processed filings with no facts",
                affected_records=len(orphaned),
                details={
                    "filings": [
                        {"accession": r[0], "ticker": r[2], "date": str(r[3])}
                        for r in orphaned[:10]  # Limit to first 10
                    ]
                }
            ))
        
        return {
            "orphaned_filings": len(orphaned),
            "sample": [{"accession": r[0], "ticker": r[2]} for r in orphaned[:5]]
        }
    
    def reconcile_normalized_to_raw(self) -> Dict:
        """Check normalized metrics match raw facts."""
        logger.debug("Reconciling normalized to raw facts")
        issues = []
        
        # Sample normalized metrics
        normalized = self.db.connection.execute("""
            SELECT id, company_ticker, metric_id, source_accession, source_concept
            FROM normalized_financials
            WHERE source_accession IS NOT NULL
            LIMIT 1000
        """).fetchall()
        
        for norm_id, ticker, metric_id, source_acc, source_concept in normalized:
            # Check source fact exists
            fact_exists = self.db.connection.execute("""
                SELECT COUNT(*) FROM facts
                WHERE accession_number = ? AND concept_name = ?
            """, [source_acc, source_concept]).fetchone()[0]
            
            if fact_exists == 0:
                issues.append({
                    "normalized_id": norm_id,
                    "ticker": ticker,
                    "metric": metric_id,
                    "source_accession": source_acc,
                    "source_concept": source_concept
                })
        
        if issues:
            self.issues.append(ReconciliationIssue(
                issue_type="missing_source_facts",
                severity="error",
                description=f"Found {len(issues)} normalized metrics with missing source facts",
                affected_records=len(issues),
                details={"sample": issues[:5]}
            ))
        
        return {
            "checked_records": len(normalized),
            "missing_sources": len(issues),
            "sample_issues": issues[:5]
        }
    
    def check_duplicates(self) -> Dict:
        """Check for duplicate records."""
        logger.debug("Checking for duplicates")
        results = {}
        
        # Check facts table
        fact_dupes = self.db.connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT accession_number, concept_name, period_end, dimensions
                FROM facts
                GROUP BY accession_number, concept_name, period_end, dimensions
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        
        if fact_dupes > 0:
            self.issues.append(ReconciliationIssue(
                issue_type="duplicate_facts",
                severity="warning",
                description=f"Found {fact_dupes} duplicate fact groups",
                affected_records=fact_dupes
            ))
        
        results["facts"] = fact_dupes
        
        # Check normalized_financials table
        norm_dupes = self.db.connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT company_ticker, fiscal_year, fiscal_quarter, metric_id
                FROM normalized_financials
                GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        
        if norm_dupes > 0:
            self.issues.append(ReconciliationIssue(
                issue_type="duplicate_normalized_metrics",
                severity="critical",
                description=f"Found {norm_dupes} duplicate normalized metric groups",
                affected_records=norm_dupes
            ))
        
        results["normalized_financials"] = norm_dupes
        
        return results
    
    def check_data_completeness(self) -> Dict:
        """Check for data completeness issues."""
        logger.debug("Checking data completeness")
        results = {}
        
        # Check for filings without primary document
        missing_docs = self.db.connection.execute("""
            SELECT COUNT(*) FROM filings
            WHERE primary_document IS NULL OR primary_document = ''
        """).fetchone()[0]
        
        if missing_docs > 0:
            self.issues.append(ReconciliationIssue(
                issue_type="missing_primary_document",
                severity="warning",
                description=f"Found {missing_docs} filings without primary document",
                affected_records=missing_docs
            ))
        
        results["missing_primary_documents"] = missing_docs
        
        # Check for facts without values
        null_values = self.db.connection.execute("""
            SELECT COUNT(*) FROM facts
            WHERE value IS NULL AND value_text IS NULL
        """).fetchone()[0]
        
        if null_values > 0:
            self.issues.append(ReconciliationIssue(
                issue_type="null_fact_values",
                severity="info",
                description=f"Found {null_values} facts with null values",
                affected_records=null_values
            ))
        
        results["null_fact_values"] = null_values
        
        return results
    
    def check_referential_integrity(self) -> Dict:
        """Check referential integrity constraints."""
        logger.debug("Checking referential integrity")
        results = {}
        
        # Facts referencing non-existent filings
        orphaned_facts = self.db.connection.execute("""
            SELECT COUNT(*) FROM facts f
            LEFT JOIN filings fi ON f.accession_number = fi.accession_number
            WHERE fi.accession_number IS NULL
        """).fetchone()[0]
        
        if orphaned_facts > 0:
            self.issues.append(ReconciliationIssue(
                issue_type="orphaned_facts",
                severity="error",
                description=f"Found {orphaned_facts} facts referencing non-existent filings",
                affected_records=orphaned_facts
            ))
        
        results["orphaned_facts"] = orphaned_facts
        
        # Filings referencing non-existent companies
        orphaned_filings = self.db.connection.execute("""
            SELECT COUNT(*) FROM filings f
            LEFT JOIN companies c ON f.cik = c.cik
            WHERE c.cik IS NULL
        """).fetchone()[0]
        
        if orphaned_filings > 0:
            self.issues.append(ReconciliationIssue(
                issue_type="orphaned_filings_no_company",
                severity="critical",
                description=f"Found {orphaned_filings} filings referencing non-existent companies",
                affected_records=orphaned_filings
            ))
        
        results["orphaned_filings"] = orphaned_filings
        
        return results
