"""
Analytics repository for aggregations, stats, and processing logs.

Handles analytics queries, processing logs, and custom SQL execution
for reporting and monitoring.
"""

import json
from datetime import datetime
from typing import Optional

import pandas as pd

from ..infrastructure.logger import get_logger
from .connection import Database

logger = get_logger("finloom.storage.analytics")


class AnalyticsRepository:
    """Repository for analytics and monitoring operations."""
    
    def __init__(self, db: Database):
        """
        Initialize repository with database connection.
        
        Args:
            db: Database instance to use for queries
        """
        self.db = db
    
    def log_processing(
        self,
        pipeline_stage: str,
        status: str,
        accession_number: Optional[str] = None,
        cik: Optional[str] = None,
        operation: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        processing_time_ms: Optional[int] = None,
        records_processed: Optional[int] = None,
        records_failed: Optional[int] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> int:
        """Log a processing operation."""
        id_result = self.db.connection.execute("SELECT nextval('processing_logs_id_seq')").fetchone()
        log_id = id_result[0]
        
        sql = """
            INSERT INTO processing_logs (
                id, accession_number, cik, pipeline_stage, operation, status,
                started_at, completed_at, processing_time_ms,
                records_processed, records_failed, error_message, error_traceback, context
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.db.connection.execute(sql, [
            log_id, accession_number, cik, pipeline_stage, operation, status,
            started_at, completed_at, processing_time_ms,
            records_processed, records_failed, error_message, error_traceback,
            json.dumps(context) if context else None
        ])
        return log_id
    
    def get_processing_summary(self) -> pd.DataFrame:
        """Get processing status summary."""
        return self.db.connection.execute("SELECT * FROM processing_summary").df()
    
    def get_key_financials(self, cik: Optional[str] = None) -> pd.DataFrame:
        """Get key financial metrics."""
        sql = "SELECT * FROM key_financials"
        params = []
        
        if cik:
            sql = sql.replace("FROM key_financials", "FROM key_financials WHERE cik = ?")
            # Actually need to query the underlying tables
            sql = """
                SELECT 
                    c.ticker,
                    c.company_name,
                    f.accession_number,
                    f.period_of_report,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:Assets' THEN fa.value END) as total_assets,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:Liabilities' THEN fa.value END) as total_liabilities,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:StockholdersEquity' THEN fa.value END) as equity,
                    MAX(CASE WHEN fa.concept_name LIKE '%Revenue%' THEN fa.value END) as revenue,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:NetIncomeLoss' THEN fa.value END) as net_income
                FROM filings f
                JOIN companies c ON f.cik = c.cik
                LEFT JOIN facts fa ON f.accession_number = fa.accession_number
                WHERE c.cik = ?
                GROUP BY c.ticker, c.company_name, f.accession_number, f.period_of_report
                ORDER BY f.period_of_report DESC
            """
            params = [cik]
        
        return self.db.connection.execute(sql, params).df()
    
    def execute_query(self, sql: str, params: Optional[list] = None) -> pd.DataFrame:
        """Execute arbitrary SQL and return DataFrame."""
        return self.db.connection.execute(sql, params or []).df()
