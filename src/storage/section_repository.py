"""
Section repository for filing section operations.

Note: Section extraction removed in markdown-only architecture.
All unstructured data is stored in filings.full_markdown column.
This module is kept for backward compatibility and future extension.
"""

from ..infrastructure.logger import get_logger
from .connection import Database

logger = get_logger("finloom.storage.section_repository")


class SectionRepository:
    """Repository for section data operations."""
    
    def __init__(self, db: Database):
        """
        Initialize repository with database connection.
        
        Args:
            db: Database instance to use for queries
        """
        self.db = db
    
    # Section operations intentionally removed
    # Future: Add document chunking/retrieval operations if needed
