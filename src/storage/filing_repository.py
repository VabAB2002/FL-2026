"""
Filing repository for CRUD operations on filing records.

Handles all filing-related database operations including
insertion, retrieval, status updates, and queries.
"""

import json
from datetime import date, datetime
from typing import Optional

from ..infrastructure.logger import get_logger
from .connection import Database

logger = get_logger("finloom.storage.filing_repository")


class FilingRepository:
    """Repository for filing data operations."""
    
    def __init__(self, db: Database):
        """
        Initialize repository with database connection.
        
        Args:
            db: Database instance to use for queries
        """
        self.db = db
    
    def upsert_filing(
        self,
        accession_number: str,
        cik: str,
        form_type: str,
        filing_date: date,
        period_of_report: Optional[date] = None,
        acceptance_datetime: Optional[datetime] = None,
        primary_document: Optional[str] = None,
        primary_doc_description: Optional[str] = None,
        is_xbrl: bool = False,
        is_inline_xbrl: bool = False,
        edgar_url: Optional[str] = None,
        local_path: Optional[str] = None,
        download_status: str = "pending",
    ) -> None:
        """Insert or update a filing record."""
        sql = """
            INSERT INTO filings (
                accession_number, cik, form_type, filing_date, period_of_report,
                acceptance_datetime, primary_document, primary_doc_description,
                is_xbrl, is_inline_xbrl, edgar_url, local_path, download_status,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (accession_number) DO UPDATE SET
                download_status = EXCLUDED.download_status,
                local_path = COALESCE(EXCLUDED.local_path, filings.local_path),
                updated_at = now()
        """
        self.db.connection.execute(sql, [
            accession_number, cik, form_type, filing_date, period_of_report,
            acceptance_datetime, primary_document, primary_doc_description,
            is_xbrl, is_inline_xbrl, edgar_url, local_path, download_status
        ])
        logger.debug(f"Upserted filing: {accession_number}")
    
    def get_filing(self, accession_number: str) -> Optional[dict]:
        """Get filing by accession number."""
        sql = "SELECT * FROM filings WHERE accession_number = ?"
        result = self.db.connection.execute(sql, [accession_number]).fetchone()
        
        if result:
            columns = [desc[0] for desc in self.db.connection.description]
            return dict(zip(columns, result))
        return None
    
    def get_company_filings(
        self,
        cik: str,
        form_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[dict]:
        """Get filings for a company with optional filters."""
        sql = "SELECT * FROM filings WHERE cik = ?"
        params = [cik]
        
        if form_type:
            sql += " AND form_type = ?"
            params.append(form_type)
        if start_date:
            sql += " AND filing_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND filing_date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY filing_date DESC"
        
        results = self.db.connection.execute(sql, params).fetchall()
        columns = [desc[0] for desc in self.db.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def update_filing_status(
        self,
        accession_number: str,
        download_status: Optional[str] = None,
        xbrl_processed: Optional[bool] = None,
        sections_processed: Optional[bool] = None,
        local_path: Optional[str] = None,
        processing_errors: Optional[dict] = None,
    ) -> None:
        """Update filing processing status."""
        updates = []
        params = []
        
        if download_status is not None:
            updates.append("download_status = ?")
            params.append(download_status)
        if xbrl_processed is not None:
            updates.append("xbrl_processed = ?")
            params.append(xbrl_processed)
        if sections_processed is not None:
            updates.append("sections_processed = ?")
            params.append(sections_processed)
        if local_path is not None:
            updates.append("local_path = ?")
            params.append(local_path)
        if processing_errors is not None:
            updates.append("processing_errors = ?")
            params.append(json.dumps(processing_errors))
        
        if not updates:
            return
        
        updates.append("updated_at = now()")
        params.append(accession_number)
        
        sql = f"UPDATE filings SET {', '.join(updates)} WHERE accession_number = ?"
        self.db.connection.execute(sql, params)
    
    def get_unprocessed_filings(self, processing_type: str = "xbrl") -> list[dict]:
        """Get filings that haven't been processed."""
        if processing_type == "xbrl":
            sql = "SELECT * FROM filings WHERE xbrl_processed = FALSE AND download_status = 'completed'"
        elif processing_type == "sections":
            sql = "SELECT * FROM filings WHERE sections_processed = FALSE AND download_status = 'completed'"
        else:
            sql = "SELECT * FROM filings WHERE download_status = 'completed'"
        
        results = self.db.connection.execute(sql).fetchall()
        columns = [desc[0] for desc in self.db.connection.description]
        return [dict(zip(columns, row)) for row in results]
