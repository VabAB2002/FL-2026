"""
OpenTelemetry distributed tracing integration.

Provides end-to-end request tracing across the system.
"""

import os
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

from ..utils.logger import get_correlation_id, get_logger, set_correlation_id

logger = get_logger("finloom.monitoring.tracing")


class TracingConfig:
    """OpenTelemetry tracing configuration."""
    
    def __init__(
        self,
        service_name: str = "finloom",
        otlp_endpoint: str = "http://localhost:4317",
        enabled: bool = True
    ):
        """
        Initialize tracing config.
        
        Args:
            service_name: Service name for traces.
            otlp_endpoint: OTLP gRPC endpoint (e.g., 'http://localhost:4317' for Jaeger).
            enabled: Enable tracing.
        """
        self.service_name = service_name
        self.otlp_endpoint = otlp_endpoint
        self.enabled = enabled


def init_tracing(config: Optional[TracingConfig] = None) -> None:
    """
    Initialize OpenTelemetry tracing.
    
    Args:
        config: Tracing configuration.
    """
    if config is None:
        config = TracingConfig(
            enabled=os.getenv('FINLOOM_TRACING_ENABLED', 'false').lower() == 'true',
            otlp_endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://localhost:4317')
        )
    
    if not config.enabled:
        logger.info("Tracing disabled")
        return
    
    try:
        # Create resource
        resource = Resource(attributes={
            SERVICE_NAME: config.service_name
        })
        
        # Create tracer provider
        provider = TracerProvider(resource=resource)
        
        # Create OTLP exporter (compatible with Jaeger, Tempo, etc.)
        otlp_exporter = OTLPSpanExporter(
            endpoint=config.otlp_endpoint,
            insecure=True  # Use insecure for local development
        )
        
        # Add span processor
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        
        # Set global tracer provider
        trace.set_tracer_provider(provider)
        
        logger.info(
            f"Tracing initialized: {config.service_name} -> "
            f"{config.otlp_endpoint}"
        )
        
    except Exception as e:
        logger.error(f"Failed to initialize tracing: {e}")


def get_tracer(name: str = "finloom") -> trace.Tracer:
    """
    Get OpenTelemetry tracer.
    
    Args:
        name: Tracer name.
    
    Returns:
        Tracer instance.
    """
    return trace.get_tracer(name)


@contextmanager
def trace_operation(
    operation_name: str,
    attributes: Optional[Dict[str, Any]] = None,
    set_correlation: bool = True
):
    """
    Context manager for tracing an operation.
    
    Args:
        operation_name: Name of the operation.
        attributes: Additional span attributes.
        set_correlation: Set correlation ID from span.
    
    Usage:
        with trace_operation('download_filing', {'cik': '0000320193'}):
            download_filing()
    """
    tracer = get_tracer()
    
    with tracer.start_as_current_span(operation_name) as span:
        try:
            # Add attributes
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, str(value))
            
            # Set correlation ID from trace
            if set_correlation:
                ctx = span.get_span_context()
                if ctx.is_valid:
                    trace_id = format(ctx.trace_id, '032x')
                    set_correlation_id(trace_id[:16])  # Use first 16 chars
            
            # Add existing correlation ID as attribute
            if corr_id := get_correlation_id():
                span.set_attribute('correlation_id', corr_id)
            
            yield span
            
            # Mark success
            span.set_status(Status(StatusCode.OK))
            
        except Exception as e:
            # Record exception
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            raise


def trace_decorator(operation_name: Optional[str] = None, attributes: Optional[Dict] = None):
    """
    Decorator for automatic tracing.
    
    Usage:
        @trace_decorator('process_filing', {'stage': 'xbrl_parse'})
        def process_filing(filing):
            # Your code
            pass
    """
    from functools import wraps
    
    def decorator(func: Callable) -> Callable:
        span_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            with trace_operation(span_name, attributes):
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


class TracedPipeline:
    """
    Pipeline executor with distributed tracing.
    
    Traces each stage and links them in a single trace.
    """
    
    def __init__(self, pipeline_name: str):
        """
        Initialize traced pipeline.
        
        Args:
            pipeline_name: Pipeline name.
        """
        self.pipeline_name = pipeline_name
        self.tracer = get_tracer()
    
    def execute(self, stages: list, context: Dict[str, Any]) -> Dict:
        """
        Execute pipeline with tracing.
        
        Args:
            stages: List of pipeline stages.
            context: Execution context.
        
        Returns:
            Execution results.
        """
        with self.tracer.start_as_current_span(
            self.pipeline_name,
            attributes={"pipeline.stage_count": len(stages)}
        ) as parent_span:
            results = {
                "success": [],
                "failed": [],
                "context": context
            }
            
            for i, stage in enumerate(stages):
                stage_name = stage.get('name', f'stage_{i}')
                stage_func = stage['function']
                
                with self.tracer.start_as_current_span(
                    stage_name,
                    attributes={
                        "stage.index": i,
                        "stage.name": stage_name
                    }
                ) as span:
                    try:
                        logger.info(f"Executing stage: {stage_name}")
                        result = stage_func(context)
                        
                        span.set_status(Status(StatusCode.OK))
                        span.set_attribute("stage.result", "success")
                        
                        results['success'].append({
                            "stage": stage_name,
                            "result": result
                        })
                        
                        if result:
                            context.update(result)
                        
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        span.set_attribute("stage.result", "failed")
                        
                        results['failed'].append({
                            "stage": stage_name,
                            "error": str(e)
                        })
                        
                        logger.error(f"Stage failed: {stage_name} - {e}")
            
            # Set final pipeline status
            parent_span.set_attribute("pipeline.success_count", len(results['success']))
            parent_span.set_attribute("pipeline.failed_count", len(results['failed']))
            
            return results


# Example traced functions
@trace_decorator('sec_api.get_filings')
def get_filings_traced(cik: str):
    """Example traced function."""
    pass


@trace_decorator('xbrl.parse_filing')
def parse_xbrl_traced(filing_path: str):
    """Example traced function."""
    pass
