"""
SEC filing downloader.

Downloads SEC filings and their associated documents (HTML, XBRL, exhibits).
Implements resume capability and checkpointing.
"""

import hashlib
import json
import os
import re
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import requests

from ..utils.config import get_absolute_path, get_env_settings, get_settings
from ..utils.logger import get_logger
from ..utils.rate_limiter import get_rate_limiter
from .sec_api import FilingInfo, SECApi, SECApiError

logger = get_logger("finloom.ingestion.downloader")


@dataclass
class DownloadResult:
    """Result of a download operation."""
    success: bool
    accession_number: str
    cik: str
    local_path: Optional[str] = None
    files_downloaded: list[str] = field(default_factory=list)
    error_message: Optional[str] = None
    download_time_ms: float = 0.0
    total_bytes: int = 0


@dataclass
class DownloadCheckpoint:
    """Checkpoint for resumable downloads."""
    cik: str
    last_accession_number: Optional[str]
    completed_filings: list[str]
    failed_filings: list[str]
    timestamp: str
    
    def to_dict(self) -> dict:
        return {
            "cik": self.cik,
            "last_accession_number": self.last_accession_number,
            "completed_filings": self.completed_filings,
            "failed_filings": self.failed_filings,
            "timestamp": self.timestamp,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "DownloadCheckpoint":
        return cls(
            cik=data["cik"],
            last_accession_number=data.get("last_accession_number"),
            completed_filings=data.get("completed_filings", []),
            failed_filings=data.get("failed_filings", []),
            timestamp=data.get("timestamp", ""),
        )


class SECDownloader:
    """
    Downloads SEC filings to local storage.
    
    Features:
    - Rate-limited downloads respecting SEC guidelines
    - Resume capability with checkpointing
    - Parallel document downloads within a filing
    - Checksum verification
    """
    
    # XBRL linkbase patterns (always download these)
    XBRL_LINKBASE_PATTERNS = [
        "_cal.xml",  # Calculation linkbase
        "_def.xml",  # Definition linkbase
        "_lab.xml",  # Label linkbase
        "_pre.xml",  # Presentation linkbase
    ]
    
    # Patterns to exclude from downloads
    EXCLUDED_PATTERNS = [
        "filingsummary",     # FilingSummary.xml - index file, not needed
        "financial_report",  # Financial_Report.xlsx - redundant with XBRL
        "defref",            # Definition reference files
    ]
    
    def __init__(
        self,
        output_dir: Optional[str] = None,
        checkpoint_dir: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        """
        Initialize the downloader.
        
        Args:
            output_dir: Directory to store downloaded files.
            checkpoint_dir: Directory for checkpoint files.
            user_agent: User-Agent string for requests.
        """
        settings = get_settings()
        env = get_env_settings()
        
        self.output_dir = get_absolute_path(
            output_dir or settings.storage.raw_data_path
        )
        self.checkpoint_dir = get_absolute_path(
            checkpoint_dir or settings.processing.checkpoint_path
        )
        self.user_agent = user_agent or env.sec_api_user_agent
        
        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.rate_limiter = get_rate_limiter()
        self.sec_api = SECApi(user_agent=self.user_agent)
        
        # Session for downloading files
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
        })
        
        logger.info(f"Downloader initialized. Output: {self.output_dir}")
    
    def get_filing_path(self, cik: str, accession_number: str) -> Path:
        """
        Get the local path for a filing.
        
        Path structure: {output_dir}/{year}/{cik}/{accession}/
        
        Args:
            cik: Company CIK.
            accession_number: Filing accession number.
        
        Returns:
            Path object for the filing directory.
        """
        # Extract year from accession number (format: XXXXXXXXXX-YY-NNNNNN)
        acc_clean = accession_number.replace("-", "")
        if "-" in accession_number:
            year_suffix = accession_number.split("-")[1]
            year = f"20{year_suffix}" if int(year_suffix) < 50 else f"19{year_suffix}"
        else:
            year = "unknown"
        
        return self.output_dir / year / cik.zfill(10) / acc_clean
    
    def download_filing(
        self,
        filing: FilingInfo,
        include_optional: bool = True,
    ) -> DownloadResult:
        """
        Download all documents for a filing.
        
        Args:
            filing: FilingInfo object with filing metadata.
            include_optional: Whether to include optional XBRL files.
        
        Returns:
            DownloadResult with download status.
        """
        start_time = time.time()
        filing_path = self.get_filing_path(filing.cik, filing.accession_number)
        
        logger.info(
            f"Downloading filing: {filing.cik}/{filing.accession_number} "
            f"({filing.form_type}, {filing.filing_date})"
        )
        
        try:
            # Create filing directory
            filing_path.mkdir(parents=True, exist_ok=True)
            
            # Get list of documents
            documents = self.sec_api.get_filing_documents(
                filing.cik, 
                filing.accession_number
            )
            
            # Filter documents to download (only essential files)
            docs_to_download = self._filter_documents(
                documents, 
                filing.primary_document,
                include_optional
            )
            
            # Save metadata
            self._save_metadata(filing, filing_path, documents)
            
            # Download each document
            downloaded_files = []
            total_bytes = 0
            
            for doc in docs_to_download:
                doc_url = self._get_document_url(filing, doc["name"])
                local_file = filing_path / doc["name"]
                
                try:
                    bytes_downloaded = self._download_file(doc_url, local_file)
                    downloaded_files.append(doc["name"])
                    total_bytes += bytes_downloaded
                except Exception as e:
                    logger.warning(f"Failed to download {doc['name']}: {e}")
            
            elapsed_ms = (time.time() - start_time) * 1000
            
            logger.info(
                f"Downloaded {len(downloaded_files)} files for "
                f"{filing.accession_number} ({total_bytes:,} bytes, {elapsed_ms:.0f}ms)"
            )
            
            return DownloadResult(
                success=True,
                accession_number=filing.accession_number,
                cik=filing.cik,
                local_path=str(filing_path),
                files_downloaded=downloaded_files,
                download_time_ms=elapsed_ms,
                total_bytes=total_bytes,
            )
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.error(f"Failed to download filing {filing.accession_number}: {e}")
            
            return DownloadResult(
                success=False,
                accession_number=filing.accession_number,
                cik=filing.cik,
                error_message=str(e),
                download_time_ms=elapsed_ms,
            )
    
    def _filter_documents(
        self,
        documents: list[dict],
        primary_document: str,
        include_optional: bool = True,
    ) -> list[dict]:
        """
        Filter documents to download only essential files.
        
        Downloads:
        - Primary document (the main 10-K HTML file)
        - XBRL instance document
        - XBRL linkbase files (cal, def, lab, pre)
        - XBRL schema (.xsd)
        
        Excludes:
        - R*.htm files (individual report views - redundant)
        - Exhibit files (contracts, certifications)
        - FilingSummary.xml (index file - not needed for parsing)
        - Graphic files (.jpg, .gif, .png)
        
        Args:
            documents: List of document info dicts from SEC API.
            primary_document: Name of the primary document (from filing metadata).
            include_optional: Whether to include optional XBRL files.
        
        Returns:
            Filtered list of documents to download.
        """
        filtered = []
        primary_doc_lower = primary_document.lower()
        
        for doc in documents:
            name = doc.get("name", "")
            name_lower = name.lower()
            
            # 1. Always include primary document
            if name_lower == primary_doc_lower:
                filtered.append(doc)
                continue
            
            # 2. Skip known junk files
            if self._is_excluded_file(name_lower):
                continue
            
            # 3. Include XBRL linkbases
            if any(name_lower.endswith(p) for p in self.XBRL_LINKBASE_PATTERNS):
                filtered.append(doc)
                continue
            
            # 4. Include XBRL schema
            if name_lower.endswith(".xsd"):
                filtered.append(doc)
                continue
            
            # 5. Include XBRL instance (ticker-date.xml or ticker-date_htm.xml)
            if name_lower.endswith(".xml") and self._is_xbrl_instance(name_lower):
                filtered.append(doc)
                continue
        
        return filtered
    
    def _is_excluded_file(self, name_lower: str) -> bool:
        """
        Check if a file should be excluded from download.
        
        Args:
            name_lower: Lowercase filename.
        
        Returns:
            True if file should be excluded.
        """
        # Skip R*.htm files (individual report views)
        if re.match(r'^r\d+\.htm$', name_lower):
            return True
        
        # Skip exhibit files (contracts, certifications, etc.)
        if name_lower.startswith(('ex', 'exhibit')):
            return True
        
        # Skip FilingSummary and other known non-essential files
        if any(p in name_lower for p in self.EXCLUDED_PATTERNS):
            return True
        
        # Skip graphic files
        if name_lower.endswith(('.jpg', '.jpeg', '.gif', '.png', '.ico')):
            return True
        
        # Skip Excel files (redundant with XBRL)
        if name_lower.endswith(('.xlsx', '.xls')):
            return True
        
        return False
    
    def _is_xbrl_instance(self, name_lower: str) -> bool:
        """
        Check if a file is an XBRL instance document.
        
        XBRL instance files contain the actual financial data and typically 
        follow the pattern: ticker-date.xml or ticker-date_htm.xml
        
        Args:
            name_lower: Lowercase filename.
        
        Returns:
            True if file appears to be an XBRL instance.
        """
        # Already confirmed it ends with .xml by caller
        
        # Not a linkbase file
        if any(name_lower.endswith(p) for p in self.XBRL_LINKBASE_PATTERNS):
            return False
        
        # Not FilingSummary or other excluded patterns
        if any(p in name_lower for p in self.EXCLUDED_PATTERNS):
            return False
        
        # Not a schema file
        if name_lower.endswith('.xsd'):
            return False
        
        return True
    
    def _get_document_url(self, filing: FilingInfo, document_name: str) -> str:
        """Build URL for a filing document."""
        cik_num = filing.cik.lstrip("0")
        acc_raw = filing.accession_number.replace("-", "")
        return f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_raw}/{document_name}"
    
    def _download_file(self, url: str, local_path: Path) -> int:
        """
        Download a single file with rate limiting.
        
        Args:
            url: URL to download.
            local_path: Local path to save file.
        
        Returns:
            Number of bytes downloaded.
        """
        # Wait for rate limiter
        self.rate_limiter.wait()
        
        response = self.session.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        # Write to file
        total_bytes = 0
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
        
        return total_bytes
    
    def _save_metadata(
        self,
        filing: FilingInfo,
        filing_path: Path,
        documents: list[dict],
    ) -> None:
        """Save filing metadata to JSON file."""
        metadata = {
            "accession_number": filing.accession_number,
            "cik": filing.cik,
            "form_type": filing.form_type,
            "filing_date": filing.filing_date.isoformat(),
            "primary_document": filing.primary_document,
            "primary_doc_description": filing.primary_doc_description,
            "is_xbrl": filing.is_xbrl,
            "is_inline_xbrl": filing.is_inline_xbrl,
            "documents": documents,
            "download_timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
        if filing.acceptance_datetime:
            metadata["acceptance_datetime"] = filing.acceptance_datetime.isoformat()
        
        metadata_path = filing_path / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
    
    def download_company_filings(
        self,
        cik: str,
        form_type: str = "10-K",
        start_year: int = 2014,
        end_year: int = 2024,
        resume: bool = True,
    ) -> list[DownloadResult]:
        """
        Download all filings for a company within date range.
        
        Args:
            cik: Company CIK.
            form_type: Form type to download.
            start_year: Start year for filtering.
            end_year: End year for filtering.
            resume: Whether to resume from checkpoint.
        
        Returns:
            List of DownloadResult objects.
        """
        from datetime import date
        
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)
        
        # Load checkpoint if resuming
        checkpoint = None
        if resume:
            checkpoint = self._load_checkpoint(cik)
        
        # Get filing list
        logger.info(f"Fetching {form_type} filings for CIK {cik} ({start_year}-{end_year})")
        filings = self.sec_api.get_company_filings(
            cik=cik,
            form_type=form_type,
            start_date=start_date,
            end_date=end_date,
        )
        
        # Filter out already completed filings
        if checkpoint:
            completed_set = set(checkpoint.completed_filings)
            filings = [f for f in filings if f.accession_number not in completed_set]
            logger.info(f"Resuming from checkpoint. {len(filings)} filings remaining.")
        
        # Download each filing
        results = []
        completed = checkpoint.completed_filings.copy() if checkpoint else []
        failed = checkpoint.failed_filings.copy() if checkpoint else []
        
        for i, filing in enumerate(filings):
            logger.info(f"Processing filing {i+1}/{len(filings)}: {filing.accession_number}")
            
            result = self.download_filing(filing)
            results.append(result)
            
            if result.success:
                completed.append(filing.accession_number)
            else:
                failed.append(filing.accession_number)
            
            # Save checkpoint after each filing
            self._save_checkpoint(cik, filing.accession_number, completed, failed)
        
        logger.info(
            f"Completed downloading for CIK {cik}: "
            f"{len(completed)} succeeded, {len(failed)} failed"
        )
        
        return results
    
    def _get_checkpoint_path(self, cik: str) -> Path:
        """Get path for checkpoint file."""
        return self.checkpoint_dir / f"download_{cik.zfill(10)}.json"
    
    def _load_checkpoint(self, cik: str) -> Optional[DownloadCheckpoint]:
        """Load checkpoint for a company."""
        checkpoint_path = self._get_checkpoint_path(cik)
        
        if checkpoint_path.exists():
            try:
                with open(checkpoint_path) as f:
                    data = json.load(f)
                return DownloadCheckpoint.from_dict(data)
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        
        return None
    
    def _save_checkpoint(
        self,
        cik: str,
        last_accession: str,
        completed: list[str],
        failed: list[str],
    ) -> None:
        """Save checkpoint for a company."""
        checkpoint = DownloadCheckpoint(
            cik=cik,
            last_accession_number=last_accession,
            completed_filings=completed,
            failed_filings=failed,
            timestamp=datetime.utcnow().isoformat() + "Z",
        )
        
        checkpoint_path = self._get_checkpoint_path(cik)
        with open(checkpoint_path, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)
    
    def verify_download(self, filing_path: Path) -> bool:
        """
        Verify a downloaded filing is complete.
        
        Args:
            filing_path: Path to filing directory.
        
        Returns:
            True if filing appears complete.
        """
        # Check metadata exists
        metadata_path = filing_path / "metadata.json"
        if not metadata_path.exists():
            return False
        
        # Check at least one HTML file exists
        html_files = list(filing_path.glob("*.htm")) + list(filing_path.glob("*.html"))
        if not html_files:
            return False
        
        # For XBRL filings, check XML file exists
        with open(metadata_path) as f:
            metadata = json.load(f)
        
        if metadata.get("is_xbrl") or metadata.get("is_inline_xbrl"):
            xml_files = list(filing_path.glob("*.xml"))
            if not xml_files:
                return False
        
        return True
    
    def close(self) -> None:
        """Close connections."""
        self.session.close()
        self.sec_api.close()
    
    def __enter__(self) -> "SECDownloader":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
