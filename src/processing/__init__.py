"""
Data processing and transformation layer.

This module provides:
- UnstructuredDataPipeline: Orchestrates section, table, and footnote extraction
- ProcessingResult: Result container for pipeline operations

Note: Chunking functionality is currently disabled and will be implemented later.
"""

# from .chunker import Chunk, SemanticChunker  # DISABLED: Will implement later
from .unstructured_pipeline import ProcessingResult, UnstructuredDataPipeline

__all__ = [
    "UnstructuredDataPipeline",
    "ProcessingResult",
    # "SemanticChunker",  # DISABLED: Chunking not implemented yet
    # "Chunk",            # DISABLED: Chunking not implemented yet
]
