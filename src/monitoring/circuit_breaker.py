"""Circuit breaker stub (no-op implementation)."""

class CircuitBreaker:
    """No-op circuit breaker for backward compatibility."""
    
    def __init__(self, *args, **kwargs):
        """Initialize (no-op)."""
        pass
    
    def call(self, func, *args, **kwargs):
        """Execute function without circuit breaker logic."""
        return func(*args, **kwargs)
    
    def __enter__(self):
        """Context manager enter (no-op)."""
        return self
    
    def __exit__(self, *args):
        """Context manager exit (no-op)."""
        pass
