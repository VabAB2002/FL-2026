"""
Circuit breaker pattern implementation for fault tolerance.

Prevents cascading failures by failing fast when a service is unavailable.
"""

import threading
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional

from .logger import get_logger

logger = get_logger("finloom.utils.circuit_breaker")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.
    
    Tracks failures and opens circuit when threshold is reached.
    Automatically attempts recovery after timeout.
    
    Usage:
        breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        result = breaker.call(risky_function, arg1, arg2)
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception,
        success_threshold: int = 2,
    ) -> None:
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit.
            recovery_timeout: Seconds to wait before attempting recovery.
            expected_exception: Exception type to catch.
            success_threshold: Consecutive successes needed in half-open to close.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.success_threshold = success_threshold
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitState.CLOSED
        self._lock = threading.Lock()
        
        logger.info(
            f"Circuit breaker initialized: "
            f"failure_threshold={failure_threshold}, "
            f"recovery_timeout={recovery_timeout}s"
        )
    
    def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute.
            *args: Positional arguments for function.
            **kwargs: Keyword arguments for function.
        
        Returns:
            Function result.
        
        Raises:
            CircuitBreakerOpenError: If circuit is open.
        """
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info("Circuit breaker entering HALF_OPEN state")
                else:
                    time_remaining = self._time_until_retry()
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker OPEN. Retry after {time_remaining:.0f}s"
                    )
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        if self.last_failure_time is None:
            return False
        
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return elapsed >= self.recovery_timeout
    
    def _time_until_retry(self) -> float:
        """Calculate seconds until retry attempt."""
        if self.last_failure_time is None:
            return 0
        
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return max(0, self.recovery_timeout - elapsed)
    
    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                logger.debug(
                    f"Circuit breaker success in HALF_OPEN: "
                    f"{self.success_count}/{self.success_threshold}"
                )
                
                if self.success_count >= self.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info("Circuit breaker CLOSED - service recovered")
            
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = 0
    
    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            
            if self.state == CircuitState.HALF_OPEN:
                # Any failure in half-open returns to open
                self.state = CircuitState.OPEN
                logger.warning(
                    "Circuit breaker returned to OPEN - recovery failed"
                )
            
            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                    logger.error(
                        f"Circuit breaker OPENED after {self.failure_count} failures"
                    )
                else:
                    logger.warning(
                        f"Circuit breaker failure {self.failure_count}/"
                        f"{self.failure_threshold}"
                    )
    
    def reset(self) -> None:
        """Manually reset circuit breaker to closed state."""
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset to CLOSED")
    
    @property
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN
    
    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self.state == CircuitState.CLOSED


# Global circuit breakers for different services
_circuit_breakers = {}
_breakers_lock = threading.Lock()


def get_circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    recovery_timeout: int = 60,
) -> CircuitBreaker:
    """
    Get or create a named circuit breaker.
    
    Args:
        name: Circuit breaker name (e.g., 'sec_api', 'database').
        failure_threshold: Number of failures before opening.
        recovery_timeout: Seconds before retry.
    
    Returns:
        CircuitBreaker instance.
    """
    with _breakers_lock:
        if name not in _circuit_breakers:
            _circuit_breakers[name] = CircuitBreaker(
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
            )
        return _circuit_breakers[name]
