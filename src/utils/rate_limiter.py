"""
Rate limiter for SEC API requests.

Implements token bucket algorithm to respect SEC's rate limits.
SEC allows max 10 requests per second; we use 8 for safety margin.
"""

import asyncio
import threading
import time
from typing import Optional

from .config import get_settings
from .logger import get_logger

logger = get_logger("finloom.utils.rate_limiter")


class RateLimiter:
    """
    Token bucket rate limiter for API requests.
    
    Thread-safe implementation that can be used for both sync and async code.
    
    Attributes:
        rate: Maximum requests per second.
        burst: Maximum burst size (tokens).
    """
    
    def __init__(
        self,
        rate: Optional[float] = None,
        burst: Optional[int] = None,
    ) -> None:
        """
        Initialize rate limiter.
        
        Args:
            rate: Requests per second. Defaults to config value.
            burst: Maximum burst size. Defaults to rate * 2.
        """
        settings = get_settings()
        
        self.rate = rate or settings.sec_api.rate_limit_per_second
        self.burst = burst or int(self.rate * 2)
        
        # Token bucket state
        self._tokens = float(self.burst)
        self._last_update = time.monotonic()
        
        # Thread safety
        self._lock = threading.Lock()
        self._async_lock: Optional[asyncio.Lock] = None
        
        logger.debug(
            f"Rate limiter initialized: rate={self.rate}/sec, burst={self.burst}"
        )
    
    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_update = now
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a token (blocking).
        
        Args:
            timeout: Maximum time to wait for a token (seconds).
                    None means wait indefinitely.
        
        Returns:
            True if token acquired, False if timeout exceeded.
        """
        start_time = time.monotonic()
        
        while True:
            with self._lock:
                self._refill_tokens()
                
                if self._tokens >= 1:
                    self._tokens -= 1
                    return True
                
                # Calculate wait time
                wait_time = (1 - self._tokens) / self.rate
            
            # Check timeout
            if timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed + wait_time > timeout:
                    return False
            
            # Wait for tokens to refill
            time.sleep(min(wait_time, 0.1))
    
    async def acquire_async(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a token asynchronously.
        
        Args:
            timeout: Maximum time to wait for a token (seconds).
        
        Returns:
            True if token acquired, False if timeout exceeded.
        """
        # Initialize async lock if needed
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        
        start_time = time.monotonic()
        
        while True:
            async with self._async_lock:
                with self._lock:
                    self._refill_tokens()
                    
                    if self._tokens >= 1:
                        self._tokens -= 1
                        return True
                    
                    # Calculate wait time
                    wait_time = (1 - self._tokens) / self.rate
            
            # Check timeout
            if timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed + wait_time > timeout:
                    return False
            
            # Wait for tokens to refill
            await asyncio.sleep(min(wait_time, 0.1))
    
    def wait(self) -> None:
        """Wait until a token is available (blocking)."""
        self.acquire(timeout=None)
    
    async def wait_async(self) -> None:
        """Wait until a token is available (async)."""
        await self.acquire_async(timeout=None)
    
    @property
    def available_tokens(self) -> float:
        """Get current number of available tokens."""
        with self._lock:
            self._refill_tokens()
            return self._tokens
    
    def reset(self) -> None:
        """Reset the rate limiter to full capacity."""
        with self._lock:
            self._tokens = float(self.burst)
            self._last_update = time.monotonic()


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on response headers.
    
    SEC may return rate limit headers that we can use to adjust our limits.
    """
    
    def __init__(
        self,
        rate: Optional[float] = None,
        burst: Optional[int] = None,
        min_rate: float = 1.0,
    ) -> None:
        """
        Initialize adaptive rate limiter.
        
        Args:
            rate: Initial requests per second.
            burst: Maximum burst size.
            min_rate: Minimum rate to use when backing off.
        """
        super().__init__(rate, burst)
        self.min_rate = min_rate
        self._original_rate = self.rate
        self._backoff_until: Optional[float] = None
    
    def report_rate_limit(self, retry_after: Optional[float] = None) -> None:
        """
        Report that we received a rate limit response.
        
        Args:
            retry_after: Seconds to wait before retrying (from header).
        """
        with self._lock:
            if retry_after:
                self._backoff_until = time.monotonic() + retry_after
                logger.warning(f"Rate limit hit, backing off for {retry_after}s")
            else:
                # Reduce rate by 50%
                self.rate = max(self.min_rate, self.rate * 0.5)
                logger.warning(f"Rate limit hit, reducing rate to {self.rate}/sec")
    
    def report_success(self) -> None:
        """Report a successful request, potentially increasing rate."""
        with self._lock:
            if self.rate < self._original_rate:
                # Slowly increase rate back to original
                self.rate = min(self._original_rate, self.rate * 1.1)
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire with backoff check."""
        # Check if in backoff period
        if self._backoff_until:
            now = time.monotonic()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                if timeout is not None and wait_time > timeout:
                    return False
                time.sleep(wait_time)
            self._backoff_until = None
        
        return super().acquire(timeout)


# Global rate limiter instance
_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """Get the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = AdaptiveRateLimiter()
    return _rate_limiter
