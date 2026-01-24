"""
SEC Edgar API wrapper.

Provides methods to query SEC Edgar for company filings and metadata.
Respects SEC rate limits and follows their guidelines.
"""

import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..utils.config import get_env_settings, get_settings
from ..utils.logger import get_logger
from ..utils.rate_limiter import get_rate_limiter

logger = get_logger("finloom.ingestion.sec_api")


@dataclass
class FilingInfo:
    """Information about a single SEC filing."""
    accession_number: str
    cik: str
    form_type: str
    filing_date: date
    primary_document: str
    primary_doc_description: str
    acceptance_datetime: Optional[datetime] = None
    file_number: Optional[str] = None
    film_number: Optional[str] = None
    items: Optional[str] = None
    size: Optional[int] = None
    is_xbrl: bool = False
    is_inline_xbrl: bool = False
    
    @property
    def accession_number_formatted(self) -> str:
        """Get accession number with dashes (for URLs)."""
        # Convert 0001234567-12-123456 format if needed
        if "-" in self.accession_number:
            return self.accession_number
        # Insert dashes: first 10, next 2, last 6
        return f"{self.accession_number[:10]}-{self.accession_number[10:12]}-{self.accession_number[12:]}"
    
    @property
    def accession_number_raw(self) -> str:
        """Get accession number without dashes (for file paths)."""
        return self.accession_number.replace("-", "")
    
    @property
    def filing_url(self) -> str:
        """Get URL to the filing index page."""
        cik_num = self.cik.lstrip("0")
        acc_raw = self.accession_number_raw
        return f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_raw}/{self.primary_document}"
    
    @property
    def index_url(self) -> str:
        """Get URL to the filing index."""
        cik_num = self.cik.lstrip("0")
        acc_raw = self.accession_number_raw
        return f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_raw}/index.json"


class SECApiError(Exception):
    """Exception raised for SEC API errors."""
    pass


class SECRateLimitError(SECApiError):
    """Exception raised when rate limited by SEC."""
    def __init__(self, retry_after: Optional[float] = None):
        self.retry_after = retry_after
        super().__init__(f"Rate limited by SEC. Retry after: {retry_after}s")


class SECApi:
    """
    SEC Edgar API wrapper.
    
    Provides methods to query SEC for company filings, submission history,
    and filing documents.
    
    Attributes:
        user_agent: User-Agent string required by SEC.
        base_url: Base URL for SEC website.
    """
    
    # SEC API endpoints
    SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
    COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    FULL_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx"
    
    def __init__(self, user_agent: Optional[str] = None) -> None:
        """
        Initialize SEC API client.
        
        Args:
            user_agent: User-Agent string. Defaults to environment variable.
        """
        env = get_env_settings()
        settings = get_settings()
        
        self.user_agent = user_agent or env.sec_api_user_agent
        self.base_url = settings.sec_api.base_url
        self.rate_limiter = get_rate_limiter()
        
        # Configure session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=settings.sec_api.max_retries,
            backoff_factor=settings.sec_api.retry_delay_base,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Set headers
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        })
        
        logger.info(f"SEC API initialized with User-Agent: {self.user_agent}")
    
    def _make_request(
        self,
        url: str,
        params: Optional[dict] = None,
        timeout: float = 30.0,
    ) -> requests.Response:
        """
        Make a rate-limited request to SEC.
        
        Args:
            url: URL to request.
            params: Query parameters.
            timeout: Request timeout in seconds.
        
        Returns:
            Response object.
        
        Raises:
            SECRateLimitError: If rate limited by SEC.
            SECApiError: For other API errors.
        """
        # Wait for rate limiter
        self.rate_limiter.wait()
        
        try:
            response = self.session.get(url, params=params, timeout=timeout)
            
            # Check for rate limiting
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                retry_seconds = float(retry_after) if retry_after else 60.0
                raise SECRateLimitError(retry_seconds)
            
            response.raise_for_status()
            return response
            
        except requests.RequestException as e:
            logger.error(f"SEC API request failed: {url} - {e}")
            raise SECApiError(f"Request failed: {e}") from e
    
    def get_company_submissions(self, cik: str) -> dict[str, Any]:
        """
        Get all submissions for a company.
        
        Args:
            cik: Company CIK (will be zero-padded to 10 digits).
        
        Returns:
            JSON response with company submissions.
        """
        cik_padded = cik.zfill(10)
        url = self.SUBMISSIONS_URL.format(cik=cik_padded)
        
        logger.debug(f"Fetching submissions for CIK {cik_padded}")
        response = self._make_request(url)
        
        return response.json()
    
    def get_company_filings(
        self,
        cik: str,
        form_type: str = "10-K",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[FilingInfo]:
        """
        Get filings for a company filtered by form type and date range.
        
        Args:
            cik: Company CIK.
            form_type: Form type to filter (e.g., "10-K", "10-Q").
            start_date: Start date for filtering.
            end_date: End date for filtering.
        
        Returns:
            List of FilingInfo objects.
        """
        submissions = self.get_company_submissions(cik)
        
        # Get recent filings from main response
        recent_filings = submissions.get("filings", {}).get("recent", {})
        
        # Parse filings
        filings = self._parse_filings(cik, recent_filings, form_type, start_date, end_date)
        
        # Check for additional filing files (for companies with many filings)
        filing_files = submissions.get("filings", {}).get("files", [])
        for file_info in filing_files:
            file_url = f"https://data.sec.gov/submissions/{file_info['name']}"
            try:
                response = self._make_request(file_url)
                additional_filings = response.json()
                filings.extend(
                    self._parse_filings(cik, additional_filings, form_type, start_date, end_date)
                )
            except SECApiError as e:
                logger.warning(f"Failed to fetch additional filings file: {e}")
        
        # Sort by filing date descending
        filings.sort(key=lambda f: f.filing_date, reverse=True)
        
        logger.info(f"Found {len(filings)} {form_type} filings for CIK {cik}")
        return filings
    
    def _parse_filings(
        self,
        cik: str,
        filings_data: dict,
        form_type: str,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> list[FilingInfo]:
        """Parse filings data into FilingInfo objects."""
        filings = []
        
        # Get arrays from response
        accession_numbers = filings_data.get("accessionNumber", [])
        forms = filings_data.get("form", [])
        filing_dates = filings_data.get("filingDate", [])
        primary_documents = filings_data.get("primaryDocument", [])
        primary_doc_descriptions = filings_data.get("primaryDocDescription", [])
        acceptance_datetimes = filings_data.get("acceptanceDateTime", [])
        is_xbrl_list = filings_data.get("isXBRL", [])
        is_inline_xbrl_list = filings_data.get("isInlineXBRL", [])
        
        for i in range(len(accession_numbers)):
            # Filter by form type
            if forms[i] != form_type:
                continue
            
            # Parse filing date
            try:
                filing_dt = datetime.strptime(filing_dates[i], "%Y-%m-%d").date()
            except (ValueError, IndexError):
                continue
            
            # Filter by date range
            if start_date and filing_dt < start_date:
                continue
            if end_date and filing_dt > end_date:
                continue
            
            # Parse acceptance datetime
            acceptance_dt = None
            if i < len(acceptance_datetimes) and acceptance_datetimes[i]:
                try:
                    acceptance_dt = datetime.fromisoformat(
                        acceptance_datetimes[i].replace("Z", "+00:00")
                    )
                except ValueError:
                    pass
            
            filing = FilingInfo(
                accession_number=accession_numbers[i],
                cik=cik.zfill(10),
                form_type=forms[i],
                filing_date=filing_dt,
                primary_document=primary_documents[i] if i < len(primary_documents) else "",
                primary_doc_description=primary_doc_descriptions[i] if i < len(primary_doc_descriptions) else "",
                acceptance_datetime=acceptance_dt,
                is_xbrl=is_xbrl_list[i] if i < len(is_xbrl_list) else False,
                is_inline_xbrl=is_inline_xbrl_list[i] if i < len(is_inline_xbrl_list) else False,
            )
            filings.append(filing)
        
        return filings
    
    def get_filing_index(self, cik: str, accession_number: str) -> dict[str, Any]:
        """
        Get the index of files for a specific filing.
        
        Args:
            cik: Company CIK.
            accession_number: Filing accession number.
        
        Returns:
            JSON index of filing documents.
        """
        cik_num = cik.lstrip("0")
        acc_raw = accession_number.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_raw}/index.json"
        
        response = self._make_request(url)
        return response.json()
    
    def get_filing_documents(self, cik: str, accession_number: str) -> list[dict]:
        """
        Get list of documents in a filing.
        
        Args:
            cik: Company CIK.
            accession_number: Filing accession number.
        
        Returns:
            List of document info dicts.
        """
        index = self.get_filing_index(cik, accession_number)
        
        documents = []
        for item in index.get("directory", {}).get("item", []):
            doc = {
                "name": item.get("name", ""),
                "type": item.get("type", ""),
                "size": item.get("size", 0),
                "last_modified": item.get("last-modified", ""),
            }
            documents.append(doc)
        
        return documents
    
    def get_company_info(self, cik: str) -> dict[str, Any]:
        """
        Get company information from submissions endpoint.
        
        Args:
            cik: Company CIK.
        
        Returns:
            Dictionary with company info (name, ticker, SIC, etc).
        """
        submissions = self.get_company_submissions(cik)
        
        return {
            "cik": submissions.get("cik", cik),
            "name": submissions.get("name", ""),
            "tickers": submissions.get("tickers", []),
            "sic_code": submissions.get("sic", ""),
            "sic_description": submissions.get("sicDescription", ""),
            "category": submissions.get("category", ""),
            "fiscal_year_end": submissions.get("fiscalYearEnd", ""),
            "state_of_incorporation": submissions.get("stateOfIncorporation", ""),
            "ein": submissions.get("ein", ""),
        }
    
    def close(self) -> None:
        """Close the session."""
        self.session.close()
    
    def __enter__(self) -> "SECApi":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
