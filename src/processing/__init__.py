"""
Data processing and transformation layer.

This module provides:
- UnstructuredDataPipeline: Orchestrates section, table, and footnote extraction
- SemanticChunker: Creates RAG-ready chunks from documents
- ProcessingResult: Result container for pipeline operations
"""

from .chunker import Chunk, SemanticChunker
from .unstructured_pipeline import ProcessingResult, UnstructuredDataPipeline

__all__ = [
    "UnstructuredDataPipeline",
    "ProcessingResult",
    "SemanticChunker",
    "Chunk",
]
