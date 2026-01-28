"""
Data processing and transformation layer.

This module provides:
- UnstructuredDataPipeline: Orchestrates HTML-to-Markdown extraction for SEC filings
- ProcessingResult: Result container for pipeline operations
"""

from .unstructured_pipeline import ProcessingResult, UnstructuredDataPipeline

__all__ = [
    "UnstructuredDataPipeline",
    "ProcessingResult",
]
