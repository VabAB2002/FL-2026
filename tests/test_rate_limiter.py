"""Tests for rate limiter."""

import time
import pytest

from src.utils.rate_limiter import RateLimiter, AdaptiveRateLimiter


class TestRateLimiter:
    """Tests for RateLimiter class."""
    
    def test_init(self):
        """Test rate limiter initialization."""
        limiter = RateLimiter(rate=10, burst=20)
        assert limiter.rate == 10
        assert limiter.burst == 20
    
    def test_acquire_immediate(self):
        """Test immediate token acquisition."""
        limiter = RateLimiter(rate=10, burst=10)
        
        # Should acquire immediately when tokens available
        start = time.monotonic()
        assert limiter.acquire(timeout=1.0) is True
        elapsed = time.monotonic() - start
        
        assert elapsed < 0.1  # Should be near-instant
    
    def test_acquire_waits(self):
        """Test that acquire waits when no tokens."""
        limiter = RateLimiter(rate=10, burst=1)
        
        # Use up the burst
        limiter.acquire()
        
        # Next acquire should wait
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        
        assert elapsed >= 0.05  # Should wait at least some time
    
    def test_acquire_timeout(self):
        """Test acquire with timeout."""
        limiter = RateLimiter(rate=1, burst=1)
        
        # Use up the burst
        limiter.acquire()
        
        # Try to acquire with short timeout
        result = limiter.acquire(timeout=0.01)
        assert result is False
    
    def test_available_tokens(self):
        """Test available tokens property."""
        limiter = RateLimiter(rate=10, burst=10)
        
        initial = limiter.available_tokens
        assert initial <= 10
        
        limiter.acquire()
        after = limiter.available_tokens
        assert after < initial
    
    def test_reset(self):
        """Test reset method."""
        limiter = RateLimiter(rate=10, burst=10)
        
        # Use some tokens
        for _ in range(5):
            limiter.acquire()
        
        # Reset
        limiter.reset()
        
        # Should be back at full
        assert limiter.available_tokens == 10


class TestAdaptiveRateLimiter:
    """Tests for AdaptiveRateLimiter class."""
    
    def test_report_rate_limit(self):
        """Test rate reduction on limit hit."""
        limiter = AdaptiveRateLimiter(rate=10, min_rate=1)
        original_rate = limiter.rate
        
        limiter.report_rate_limit()
        
        assert limiter.rate < original_rate
        assert limiter.rate >= limiter.min_rate
    
    def test_report_success_increases_rate(self):
        """Test rate increase on success."""
        limiter = AdaptiveRateLimiter(rate=10, min_rate=1)
        
        # Reduce rate first
        limiter.report_rate_limit()
        reduced_rate = limiter.rate
        
        # Report success
        limiter.report_success()
        
        assert limiter.rate >= reduced_rate
