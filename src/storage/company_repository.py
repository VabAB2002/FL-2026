"""
Company repository for CRUD operations on company records.

Handles all company-related database operations including
insertion, retrieval, and queries.
"""

from typing import Optional

from ..infrastructure.logger import get_logger
from .connection import Database

logger = get_logger("finloom.storage.company_repository")


class CompanyRepository:
    """Repository for company data operations."""
    
    def __init__(self, db: Database):
        """
        Initialize repository with database connection.
        
        Args:
            db: Database instance to use for queries
        """
        self.db = db
    
    def upsert_company(
        self,
        cik: str,
        company_name: str,
        ticker: Optional[str] = None,
        sic_code: Optional[str] = None,
        sic_description: Optional[str] = None,
        state_of_incorporation: Optional[str] = None,
        fiscal_year_end: Optional[str] = None,
        category: Optional[str] = None,
        ein: Optional[str] = None,
    ) -> None:
        """Insert or update a company record."""
        sql = """
            INSERT INTO companies (
                cik, company_name, ticker, sic_code, sic_description,
                state_of_incorporation, fiscal_year_end, category, ein,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())
            ON CONFLICT (cik) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                ticker = COALESCE(EXCLUDED.ticker, companies.ticker),
                sic_code = COALESCE(EXCLUDED.sic_code, companies.sic_code),
                sic_description = COALESCE(EXCLUDED.sic_description, companies.sic_description),
                state_of_incorporation = COALESCE(EXCLUDED.state_of_incorporation, companies.state_of_incorporation),
                fiscal_year_end = COALESCE(EXCLUDED.fiscal_year_end, companies.fiscal_year_end),
                category = COALESCE(EXCLUDED.category, companies.category),
                ein = COALESCE(EXCLUDED.ein, companies.ein),
                updated_at = now()
        """
        self.db.connection.execute(sql, [
            cik, company_name, ticker, sic_code, sic_description,
            state_of_incorporation, fiscal_year_end, category, ein
        ])
        logger.debug(f"Upserted company: {cik} ({company_name})")
    
    def get_company(self, cik: str) -> Optional[dict]:
        """Get company by CIK."""
        sql = "SELECT * FROM companies WHERE cik = ?"
        result = self.db.connection.execute(sql, [cik]).fetchone()
        
        if result:
            columns = [desc[0] for desc in self.db.connection.description]
            return dict(zip(columns, result))
        return None
    
    def get_all_companies(self) -> list[dict]:
        """Get all companies."""
        sql = "SELECT * FROM companies ORDER BY ticker"
        results = self.db.connection.execute(sql).fetchall()
        columns = [desc[0] for desc in self.db.connection.description]
        return [dict(zip(columns, row)) for row in results]
