"""Pydantic models for semantic chunking of SEC filings."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ChunkConfig(BaseModel):
    """Configuration for the semantic chunker."""

    min_tokens: int = Field(default=100, description="Minimum chunk size in tokens")
    max_tokens: int = Field(default=512, description="Maximum chunk size in tokens")
    overlap_tokens: int = Field(default=50, description="Token overlap between chunks")
    tokens_per_word: float = Field(
        default=1.33, description="Approximate tokens per word for English text"
    )


class Chunk(BaseModel):
    """A single semantic chunk from a filing section."""

    chunk_id: str  # "{accession}_{item}_{index:04d}"
    accession_number: str
    section_item: str  # "1A", "7", etc.
    section_title: str | None = None  # "Risk Factors"
    chunk_index: int  # 0-based within section
    context_prefix: str  # "Company: AAPL | Filing: 10-K 2024-01-01 | Section: Item 1A"
    text: str
    token_count: int
    contains_table: bool = False


class FilingChunks(BaseModel):
    """All chunks for a single filing."""

    accession_number: str
    ticker: str
    company_name: str
    filing_date: str
    form_type: str = "10-K"
    total_chunks: int
    total_tokens: int
    chunks: list[Chunk]
