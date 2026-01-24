"""
Filing metadata extraction and management.

Provides utilities for extracting and managing SEC filing metadata.
"""

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from ..utils.config import get_absolute_path, get_settings
from ..utils.logger import get_logger

logger = get_logger("finloom.ingestion.metadata")


@dataclass
class FilingMetadata:
    """
    Complete metadata for an SEC filing.
    
    Combines information from SEC API and downloaded files.
    """
    accession_number: str
    cik: str
    company_name: str
    ticker: Optional[str]
    form_type: str
    filing_date: date
    period_of_report: Optional[date]
    acceptance_datetime: Optional[datetime]
    
    # Document information
    primary_document: str
    primary_doc_description: str
    is_xbrl: bool = False
    is_inline_xbrl: bool = False
    
    # File locations
    local_path: Optional[str] = None
    edgar_url: Optional[str] = None
    
    # Processing status
    download_status: str = "pending"
    xbrl_processed: bool = False
    sections_processed: bool = False
    
    # Additional documents
    documents: list[dict] = field(default_factory=list)
    
    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "accession_number": self.accession_number,
            "cik": self.cik,
            "company_name": self.company_name,
            "ticker": self.ticker,
            "form_type": self.form_type,
            "filing_date": self.filing_date.isoformat() if self.filing_date else None,
            "period_of_report": self.period_of_report.isoformat() if self.period_of_report else None,
            "acceptance_datetime": self.acceptance_datetime.isoformat() if self.acceptance_datetime else None,
            "primary_document": self.primary_document,
            "primary_doc_description": self.primary_doc_description,
            "is_xbrl": self.is_xbrl,
            "is_inline_xbrl": self.is_inline_xbrl,
            "local_path": self.local_path,
            "edgar_url": self.edgar_url,
            "download_status": self.download_status,
            "xbrl_processed": self.xbrl_processed,
            "sections_processed": self.sections_processed,
            "documents": self.documents,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FilingMetadata":
        """Create from dictionary."""
        return cls(
            accession_number=data["accession_number"],
            cik=data["cik"],
            company_name=data.get("company_name", ""),
            ticker=data.get("ticker"),
            form_type=data["form_type"],
            filing_date=date.fromisoformat(data["filing_date"]) if data.get("filing_date") else None,
            period_of_report=date.fromisoformat(data["period_of_report"]) if data.get("period_of_report") else None,
            acceptance_datetime=datetime.fromisoformat(data["acceptance_datetime"]) if data.get("acceptance_datetime") else None,
            primary_document=data.get("primary_document", ""),
            primary_doc_description=data.get("primary_doc_description", ""),
            is_xbrl=data.get("is_xbrl", False),
            is_inline_xbrl=data.get("is_inline_xbrl", False),
            local_path=data.get("local_path"),
            edgar_url=data.get("edgar_url"),
            download_status=data.get("download_status", "pending"),
            xbrl_processed=data.get("xbrl_processed", False),
            sections_processed=data.get("sections_processed", False),
            documents=data.get("documents", []),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None,
        )
    
    @property
    def accession_number_raw(self) -> str:
        """Get accession number without dashes."""
        return self.accession_number.replace("-", "")
    
    @property
    def xbrl_files(self) -> list[str]:
        """Get list of XBRL-related files."""
        xbrl_extensions = [".xml", ".xsd", "_cal.xml", "_def.xml", "_lab.xml", "_pre.xml"]
        return [
            doc["name"] for doc in self.documents
            if any(doc.get("name", "").lower().endswith(ext) for ext in xbrl_extensions)
        ]
    
    @property
    def html_files(self) -> list[str]:
        """Get list of HTML files."""
        return [
            doc["name"] for doc in self.documents
            if doc.get("name", "").lower().endswith((".htm", ".html"))
        ]


class MetadataManager:
    """
    Manages filing metadata storage and retrieval.
    
    Provides utilities for loading metadata from downloaded filings
    and managing metadata across the pipeline.
    """
    
    def __init__(self, raw_data_path: Optional[str] = None) -> None:
        """
        Initialize metadata manager.
        
        Args:
            raw_data_path: Path to raw data directory.
        """
        settings = get_settings()
        self.raw_data_path = get_absolute_path(
            raw_data_path or settings.storage.raw_data_path
        )
        
        logger.debug(f"MetadataManager initialized: {self.raw_data_path}")
    
    def load_filing_metadata(
        self,
        cik: str,
        accession_number: str,
    ) -> Optional[FilingMetadata]:
        """
        Load metadata for a specific filing from disk.
        
        Args:
            cik: Company CIK.
            accession_number: Filing accession number.
        
        Returns:
            FilingMetadata or None if not found.
        """
        # Find the filing directory
        acc_raw = accession_number.replace("-", "")
        cik_padded = cik.zfill(10)
        
        # Search in year directories
        for year_dir in self.raw_data_path.iterdir():
            if not year_dir.is_dir():
                continue
            
            filing_path = year_dir / cik_padded / acc_raw
            metadata_file = filing_path / "metadata.json"
            
            if metadata_file.exists():
                return self._load_metadata_file(metadata_file)
        
        return None
    
    def _load_metadata_file(self, metadata_file: Path) -> Optional[FilingMetadata]:
        """Load metadata from a JSON file."""
        try:
            with open(metadata_file) as f:
                data = json.load(f)
            
            # Map from download metadata format to FilingMetadata
            return FilingMetadata(
                accession_number=data["accession_number"],
                cik=data["cik"],
                company_name=data.get("company_name", ""),
                ticker=data.get("ticker"),
                form_type=data["form_type"],
                filing_date=date.fromisoformat(data["filing_date"]),
                period_of_report=date.fromisoformat(data["period_of_report"]) if data.get("period_of_report") else None,
                acceptance_datetime=datetime.fromisoformat(data["acceptance_datetime"].replace("Z", "+00:00")) if data.get("acceptance_datetime") else None,
                primary_document=data.get("primary_document", ""),
                primary_doc_description=data.get("primary_doc_description", ""),
                is_xbrl=data.get("is_xbrl", False),
                is_inline_xbrl=data.get("is_inline_xbrl", False),
                local_path=str(metadata_file.parent),
                documents=data.get("documents", []),
            )
        except Exception as e:
            logger.error(f"Failed to load metadata from {metadata_file}: {e}")
            return None
    
    def list_company_filings(self, cik: str) -> list[FilingMetadata]:
        """
        List all downloaded filings for a company.
        
        Args:
            cik: Company CIK.
        
        Returns:
            List of FilingMetadata objects.
        """
        cik_padded = cik.zfill(10)
        filings = []
        
        for year_dir in self.raw_data_path.iterdir():
            if not year_dir.is_dir():
                continue
            
            cik_dir = year_dir / cik_padded
            if not cik_dir.exists():
                continue
            
            for filing_dir in cik_dir.iterdir():
                if not filing_dir.is_dir():
                    continue
                
                metadata_file = filing_dir / "metadata.json"
                if metadata_file.exists():
                    metadata = self._load_metadata_file(metadata_file)
                    if metadata:
                        filings.append(metadata)
        
        # Sort by filing date
        filings.sort(key=lambda f: f.filing_date or date.min, reverse=True)
        
        return filings
    
    def list_all_filings(self) -> list[FilingMetadata]:
        """
        List all downloaded filings across all companies.
        
        Returns:
            List of FilingMetadata objects.
        """
        filings = []
        
        for year_dir in self.raw_data_path.iterdir():
            if not year_dir.is_dir():
                continue
            
            for cik_dir in year_dir.iterdir():
                if not cik_dir.is_dir():
                    continue
                
                for filing_dir in cik_dir.iterdir():
                    if not filing_dir.is_dir():
                        continue
                    
                    metadata_file = filing_dir / "metadata.json"
                    if metadata_file.exists():
                        metadata = self._load_metadata_file(metadata_file)
                        if metadata:
                            filings.append(metadata)
        
        # Sort by filing date
        filings.sort(key=lambda f: f.filing_date or date.min, reverse=True)
        
        return filings
    
    def get_unprocessed_filings(
        self,
        processing_type: str = "xbrl",
    ) -> list[FilingMetadata]:
        """
        Get filings that haven't been processed yet.
        
        Args:
            processing_type: Type of processing ("xbrl" or "sections").
        
        Returns:
            List of unprocessed FilingMetadata objects.
        """
        all_filings = self.list_all_filings()
        
        if processing_type == "xbrl":
            return [f for f in all_filings if not f.xbrl_processed]
        elif processing_type == "sections":
            return [f for f in all_filings if not f.sections_processed]
        else:
            return all_filings
    
    def update_processing_status(
        self,
        cik: str,
        accession_number: str,
        xbrl_processed: Optional[bool] = None,
        sections_processed: Optional[bool] = None,
    ) -> bool:
        """
        Update processing status for a filing.
        
        Args:
            cik: Company CIK.
            accession_number: Filing accession number.
            xbrl_processed: XBRL processing status.
            sections_processed: Section extraction status.
        
        Returns:
            True if update succeeded.
        """
        # Find the filing
        acc_raw = accession_number.replace("-", "")
        cik_padded = cik.zfill(10)
        
        for year_dir in self.raw_data_path.iterdir():
            if not year_dir.is_dir():
                continue
            
            filing_path = year_dir / cik_padded / acc_raw
            metadata_file = filing_path / "metadata.json"
            
            if metadata_file.exists():
                try:
                    with open(metadata_file) as f:
                        data = json.load(f)
                    
                    if xbrl_processed is not None:
                        data["xbrl_processed"] = xbrl_processed
                    if sections_processed is not None:
                        data["sections_processed"] = sections_processed
                    
                    data["updated_at"] = datetime.utcnow().isoformat() + "Z"
                    
                    with open(metadata_file, "w") as f:
                        json.dump(data, f, indent=2)
                    
                    return True
                except Exception as e:
                    logger.error(f"Failed to update metadata: {e}")
                    return False
        
        return False
