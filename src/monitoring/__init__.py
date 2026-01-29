"""Monitoring stubs (no-op implementations)."""

# Stub metrics (no-op)
class _StubMetric:
    def labels(self, **kwargs):
        return self
    def inc(self, *args):
        pass
    def set(self, *args):
        pass
    def observe(self, *args):
        pass

# Export stub metrics
unstructured_quality_score = _StubMetric()
unstructured_extraction_errors = _StubMetric()
unstructured_processing_time = _StubMetric()

__all__ = [
    'unstructured_quality_score',
    'unstructured_extraction_errors',
    'unstructured_processing_time',
]
