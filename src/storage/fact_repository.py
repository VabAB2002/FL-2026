"""
Fact repository for CRUD operations on XBRL facts and concept categories.

Handles all fact-related database operations including insertion,
retrieval, batch operations, and concept category management.
"""

import json
from datetime import date
from decimal import Decimal
from typing import Optional

import pandas as pd

from ..infrastructure.logger import get_logger
from .connection import Database

logger = get_logger("finloom.storage.fact_repository")


class FactRepository:
    """Repository for XBRL fact data operations."""
    
    def __init__(self, db: Database):
        """
        Initialize repository with database connection.
        
        Args:
            db: Database instance to use for queries
        """
        self.db = db
    
    def insert_fact(
        self,
        accession_number: str,
        concept_name: str,
        value: Optional[Decimal] = None,
        value_text: Optional[str] = None,
        unit: Optional[str] = None,
        decimals: Optional[int] = None,
        period_type: Optional[str] = None,
        period_start: Optional[date] = None,
        period_end: Optional[date] = None,
        dimensions: Optional[dict] = None,
        concept_namespace: Optional[str] = None,
        concept_local_name: Optional[str] = None,
        is_custom: bool = False,
        is_negated: bool = False,
        section: Optional[str] = None,
        parent_concept: Optional[str] = None,
        label: Optional[str] = None,
        depth: Optional[int] = None,
    ) -> int:
        """Insert a fact record and return its ID. Skips if duplicate already exists."""
        # Check if fact already exists (duplicate prevention)
        dimensions_json = json.dumps(dimensions) if dimensions else None
        check_sql = """
            SELECT id FROM facts 
            WHERE accession_number = ? 
              AND concept_name = ? 
              AND period_end IS NOT DISTINCT FROM ?
              AND dimensions IS NOT DISTINCT FROM ?
        """
        existing = self.db.connection.execute(check_sql, [
            accession_number, 
            concept_name, 
            period_end,
            dimensions_json
        ]).fetchone()
        
        # If fact already exists, return existing ID without inserting
        if existing:
            logger.debug(f"Fact already exists: {concept_name} for {accession_number}, skipping duplicate")
            return existing[0]
        
        # Get next ID from sequence
        id_result = self.db.connection.execute("SELECT nextval('facts_id_seq')").fetchone()
        fact_id = id_result[0]
        
        sql = """
            INSERT INTO facts (
                id, accession_number, concept_name, concept_namespace, concept_local_name,
                value, value_text, unit, decimals, period_type, period_start, period_end,
                dimensions, is_custom, is_negated, section, parent_concept, label, depth
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.db.connection.execute(sql, [
            fact_id, accession_number, concept_name, concept_namespace, concept_local_name,
            float(value) if value is not None else None, value_text, unit, decimals,
            period_type, period_start, period_end,
            dimensions_json, is_custom, is_negated,
            section, parent_concept, label, depth
        ])
        return fact_id
    
    def insert_facts_batch(self, facts: list[dict]) -> int:
        """Insert multiple facts in a batch."""
        if not facts:
            return 0
        
        count = 0
        for fact in facts:
            self.insert_fact(**fact)
            count += 1
        
        return count
    
    def get_facts(
        self,
        accession_number: str,
        concept_name: Optional[str] = None,
    ) -> list[dict]:
        """Get facts for a filing."""
        sql = "SELECT * FROM facts WHERE accession_number = ?"
        params = [accession_number]
        
        if concept_name:
            sql += " AND concept_name = ?"
            params.append(concept_name)
        
        results = self.db.connection.execute(sql, params).fetchall()
        columns = [desc[0] for desc in self.db.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def upsert_concept_category(
        self,
        concept_name: str,
        section: Optional[str] = None,
        subsection: Optional[str] = None,
        parent_concept: Optional[str] = None,
        depth: Optional[int] = None,
        label: Optional[str] = None,
        data_type: Optional[str] = None,
    ) -> None:
        """Insert or update a concept category record."""
        sql = """
            INSERT INTO concept_categories (
                concept_name, section, subsection, parent_concept, 
                depth, label, data_type, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, now(), now())
            ON CONFLICT (concept_name) DO UPDATE SET
                section = COALESCE(EXCLUDED.section, concept_categories.section),
                subsection = COALESCE(EXCLUDED.subsection, concept_categories.subsection),
                parent_concept = COALESCE(EXCLUDED.parent_concept, concept_categories.parent_concept),
                depth = COALESCE(EXCLUDED.depth, concept_categories.depth),
                label = COALESCE(EXCLUDED.label, concept_categories.label),
                data_type = COALESCE(EXCLUDED.data_type, concept_categories.data_type),
                updated_at = now()
        """
        self.db.connection.execute(sql, [
            concept_name, section, subsection, parent_concept, depth, label, data_type
        ])
    
    def get_concept_category(self, concept_name: str) -> Optional[dict]:
        """Get concept category by name."""
        sql = "SELECT * FROM concept_categories WHERE concept_name = ?"
        result = self.db.connection.execute(sql, [concept_name]).fetchone()
        
        if result:
            columns = [desc[0] for desc in self.db.connection.description]
            return dict(zip(columns, result))
        return None
    
    def get_concepts_by_section(self, section: str) -> list[dict]:
        """Get all concepts in a section."""
        sql = "SELECT * FROM concept_categories WHERE section = ? ORDER BY depth, concept_name"
        results = self.db.connection.execute(sql, [section]).fetchall()
        columns = [desc[0] for desc in self.db.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def get_all_sections(self) -> list[str]:
        """Get all unique sections."""
        sql = "SELECT DISTINCT section FROM concept_categories WHERE section IS NOT NULL ORDER BY section"
        results = self.db.connection.execute(sql).fetchall()
        return [row[0] for row in results]
    
    def get_fact_history(
        self,
        cik: str,
        concept_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """Get historical values for a concept across filings."""
        sql = """
            SELECT 
                f.accession_number,
                f.period_of_report,
                f.filing_date,
                fa.value,
                fa.unit,
                fa.period_start,
                fa.period_end
            FROM facts fa
            JOIN filings f ON fa.accession_number = f.accession_number
            WHERE f.cik = ? AND fa.concept_name = ?
            AND fa.dimensions IS NULL
        """
        params = [cik, concept_name]
        
        if start_date:
            sql += " AND f.period_of_report >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND f.period_of_report <= ?"
            params.append(end_date)
        
        sql += " ORDER BY f.period_of_report DESC"
        
        return self.db.connection.execute(sql, params).df()
