"""
Retry logic with exponential backoff and jitter.

Prevents thundering herd problem and improves reliability.
"""

import random
import time
from functools import wraps
from typing import Callable, Optional, Tuple, Type

from .logger import get_logger

logger = get_logger("finloom.utils.retry")


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay in seconds.
        max_delay: Maximum delay in seconds.
        exponential_base: Base for exponential calculation.
        jitter: Add random jitter to prevent thundering herd.
        exceptions: Tuple of exceptions to catch and retry.
    
    Usage:
        @retry_with_backoff(max_retries=3, base_delay=1.0)
        def fetch_data():
            return requests.get(url)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} retries: {e}"
                        )
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        base_delay * (exponential_base ** attempt),
                        max_delay
                    )
                    
                    # Add jitter to prevent thundering herd
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    time.sleep(delay)
            
            # This should never be reached, but satisfy type checker
            raise RuntimeError(f"{func.__name__} failed after all retries")
        
        return wrapper
    return decorator


class RetryStrategy:
    """
    Configurable retry strategy for programmatic use.
    
    Usage:
        strategy = RetryStrategy(max_retries=5, base_delay=2.0)
        result = strategy.execute(risky_function, arg1, arg2)
    """
    
    def __init__(
        self,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
    ):
        """Initialize retry strategy."""
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.exceptions = exceptions
    
    def execute(self, func: Callable, *args, **kwargs):
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        
        Returns:
            Function result.
        
        Raises:
            Last exception if all retries fail.
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except self.exceptions as e:
                last_exception = e
                
                if attempt == self.max_retries:
                    logger.error(
                        f"{func.__name__} failed after {self.max_retries} retries"
                    )
                    raise
                
                delay = self._calculate_delay(attempt)
                
                logger.warning(
                    f"{func.__name__} attempt {attempt + 1}/{self.max_retries} "
                    f"failed: {e}. Retrying in {delay:.2f}s"
                )
                time.sleep(delay)
        
        # Should never reach here
        if last_exception:
            raise last_exception
        raise RuntimeError("Retry logic failed unexpectedly")
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for current attempt."""
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        
        if self.jitter:
            # Add jitter: delay * (0.5 to 1.5)
            delay = delay * (0.5 + random.random())
        
        return delay


def retry_on_condition(
    condition: Callable[[Exception], bool],
    max_retries: int = 3,
    base_delay: float = 1.0,
):
    """
    Retry decorator that checks a condition before retrying.
    
    Args:
        condition: Function that takes exception and returns True if should retry.
        max_retries: Maximum retry attempts.
        base_delay: Base delay between retries.
    
    Usage:
        @retry_on_condition(lambda e: isinstance(e, TimeoutError), max_retries=3)
        def fetch_data():
            return requests.get(url, timeout=5)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not condition(e) or attempt == max_retries:
                        raise
                    
                    delay = base_delay * (2 ** attempt)
                    logger.warning(
                        f"{func.__name__} retry {attempt + 1}/{max_retries} "
                        f"due to {type(e).__name__}. Waiting {delay}s"
                    )
                    time.sleep(delay)
            
            raise RuntimeError("Retry logic failed")
        
        return wrapper
    return decorator
