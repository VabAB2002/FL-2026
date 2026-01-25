"""
Async download pipeline for SEC filings.

Significantly faster than synchronous downloads using aiohttp.
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import aiohttp

from ..utils.logger import get_logger, set_correlation_id
from ..utils.rate_limiter import RateLimiter

logger = get_logger("finloom.ingestion.async_downloader")


@dataclass
class AsyncDownloadResult:
    """Result of async download operation."""
    accession_number: str
    success: bool
    local_path: Optional[Path] = None
    error: Optional[str] = None
    size_bytes: Optional[int] = None
    duration_seconds: Optional[float] = None


class AsyncRateLimiter:
    """Async-compatible rate limiter."""
    
    def __init__(self, rate: float = 8.0):
        """
        Initialize async rate limiter.
        
        Args:
            rate: Requests per second.
        """
        self.rate = rate
        self._semaphore = asyncio.Semaphore(int(rate * 2))
        self._last_call = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire rate limit token."""
        async with self._semaphore:
            async with self._lock:
                now = asyncio.get_event_loop().time()
                time_since_last = now - self._last_call
                min_interval = 1.0 / self.rate
                
                if time_since_last < min_interval:
                    await asyncio.sleep(min_interval - time_since_last)
                
                self._last_call = asyncio.get_event_loop().time()


class AsyncSECDownloader:
    """
    Async SEC filing downloader.
    
    Downloads multiple filings concurrently while respecting rate limits.
    Significantly faster than synchronous approach.
    """
    
    def __init__(
        self,
        max_concurrent: int = 10,
        rate_limit: float = 8.0,
        timeout: int = 30,
        user_agent: Optional[str] = None
    ):
        """
        Initialize async downloader.
        
        Args:
            max_concurrent: Maximum concurrent downloads.
            rate_limit: Requests per second.
            timeout: Request timeout in seconds.
            user_agent: SEC User-Agent string.
        """
        self.max_concurrent = max_concurrent
        self.rate_limiter = AsyncRateLimiter(rate=rate_limit)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.user_agent = user_agent or "FinLoom AsyncDownloader"
        self.session: Optional[aiohttp.ClientSession] = None
        
        logger.info(
            f"Async downloader initialized: "
            f"max_concurrent={max_concurrent}, rate={rate_limit}/sec"
        )
    
    async def __aenter__(self):
        """Async context manager entry."""
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "*/*"
        }
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers=headers
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def download_filing(
        self,
        filing,
        output_dir: Path
    ) -> AsyncDownloadResult:
        """
        Download a single filing.
        
        Args:
            filing: FilingInfo object.
            output_dir: Output directory.
        
        Returns:
            AsyncDownloadResult.
        """
        await self.rate_limiter.acquire()
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Get filing index to find all documents
            index_url = filing.index_url
            
            async with self.session.get(index_url) as response:
                if response.status != 200:
                    return AsyncDownloadResult(
                        accession_number=filing.accession_number,
                        success=False,
                        error=f"HTTP {response.status}"
                    )
                
                index_data = await response.json()
            
            # Create output directory
            acc_dir = output_dir / filing.accession_number_raw
            acc_dir.mkdir(parents=True, exist_ok=True)
            
            # Download required files (XBRL only)
            required_extensions = ['.xml', '.xsd']
            excluded_patterns = ['_htm.xml', 'FilingSummary.xml']
            
            files = index_data['directory']['item']
            download_tasks = []
            
            for file_info in files:
                filename = file_info['name']
                
                # Check if file should be downloaded
                should_download = any(filename.endswith(ext) for ext in required_extensions)
                should_exclude = any(pattern in filename for pattern in excluded_patterns)
                
                if should_download and not should_exclude:
                    file_url = f"https://www.sec.gov/Archives/edgar/data/{filing.cik.lstrip('0')}/{filing.accession_number_raw}/{filename}"
                    local_path = acc_dir / filename
                    download_tasks.append(
                        self._download_file(file_url, local_path)
                    )
            
            # Download all files concurrently
            results = await asyncio.gather(*download_tasks, return_exceptions=True)
            
            # Check for errors
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                logger.warning(f"Some files failed to download: {len(errors)}/{len(results)}")
            
            duration = asyncio.get_event_loop().time() - start_time
            
            return AsyncDownloadResult(
                accession_number=filing.accession_number,
                success=len(errors) < len(results) / 2,  # At least 50% success
                local_path=acc_dir,
                size_bytes=sum(r for r in results if isinstance(r, int)),
                duration_seconds=duration
            )
            
        except Exception as e:
            logger.error(f"Download failed for {filing.accession_number}: {e}")
            duration = asyncio.get_event_loop().time() - start_time
            return AsyncDownloadResult(
                accession_number=filing.accession_number,
                success=False,
                error=str(e),
                duration_seconds=duration
            )
    
    async def _download_file(self, url: str, local_path: Path) -> int:
        """
        Download a single file.
        
        Args:
            url: File URL.
            local_path: Local path to save.
        
        Returns:
            File size in bytes.
        """
        await self.rate_limiter.acquire()
        
        async with self.session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                local_path.write_bytes(content)
                return len(content)
            else:
                raise Exception(f"HTTP {response.status} for {url}")
    
    async def download_batch(
        self,
        filings: List,
        output_dir: Path
    ) -> List[AsyncDownloadResult]:
        """
        Download multiple filings concurrently.
        
        Args:
            filings: List of FilingInfo objects.
            output_dir: Output directory.
        
        Returns:
            List of AsyncDownloadResult.
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def download_with_semaphore(filing):
            async with semaphore:
                # Set correlation ID for this filing
                set_correlation_id(filing.accession_number)
                return await self.download_filing(filing, output_dir)
        
        logger.info(f"Starting batch download of {len(filings)} filings")
        results = await asyncio.gather(
            *[download_with_semaphore(f) for f in filings],
            return_exceptions=True
        )
        
        # Handle exceptions
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Download {i} failed with exception: {result}")
                valid_results.append(AsyncDownloadResult(
                    accession_number=filings[i].accession_number,
                    success=False,
                    error=str(result)
                ))
            else:
                valid_results.append(result)
        
        # Log summary
        success_count = sum(1 for r in valid_results if r.success)
        logger.info(f"Batch download complete: {success_count}/{len(filings)} successful")
        
        return valid_results


# =============================================================================
# Convenience Functions
# =============================================================================

async def download_filings_async(
    filings: List,
    output_dir: Path,
    max_concurrent: int = 10
) -> List[AsyncDownloadResult]:
    """
    Convenience function to download filings asynchronously.
    
    Args:
        filings: List of FilingInfo objects.
        output_dir: Output directory.
        max_concurrent: Max concurrent downloads.
    
    Returns:
        List of download results.
    """
    async with AsyncSECDownloader(max_concurrent=max_concurrent) as downloader:
        return await downloader.download_batch(filings, output_dir)


def download_filings_sync_wrapper(
    filings: List,
    output_dir: Path,
    max_concurrent: int = 10
) -> List[AsyncDownloadResult]:
    """
    Synchronous wrapper for async download function.
    
    Use this in synchronous code that needs async downloads.
    """
    return asyncio.run(download_filings_async(filings, output_dir, max_concurrent))
