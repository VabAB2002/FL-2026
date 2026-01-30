"""
Semantic chunker for SEC 10-K filing sections.

Splits section markdown into chunks that:
- Preserve table boundaries (never split mid-table)
- Respect paragraph boundaries
- Stay within configurable token limits
- Include paragraph-aware overlap between chunks
"""

from __future__ import annotations

import re

from src.infrastructure.logger import get_logger

from .split_models import Chunk, ChunkConfig

logger = get_logger("finloom.chunking.text_splitter")

# Matches <table>...</table> blocks including nested content
_TABLE_RE = re.compile(r"<table[^>]*>.*?</table>", re.DOTALL | re.IGNORECASE)


class SemanticChunker:
    """Structure-aware chunker for financial documents."""

    def __init__(self, config: ChunkConfig | None = None):
        self.config = config or ChunkConfig()

    def estimate_tokens(self, text: str) -> int:
        """Estimate token count from word count."""
        return int(len(text.split()) * self.config.tokens_per_word)

    def chunk_section(
        self,
        markdown: str,
        accession_number: str,
        section_item: str,
        section_title: str | None,
        context_prefix: str,
    ) -> list[Chunk]:
        """
        Chunk a single section's markdown.

        Args:
            markdown: Section markdown text (may contain HTML tables)
            accession_number: Filing accession number
            section_item: Section identifier (e.g. "1A")
            section_title: Section title (e.g. "Risk Factors")
            context_prefix: Metadata prefix for each chunk

        Returns:
            List of Chunk objects
        """
        blocks = self._split_into_blocks(markdown)
        if not blocks:
            return []

        return self._merge_blocks(
            blocks, accession_number, section_item, section_title, context_prefix
        )

    # ------------------------------------------------------------------
    # Block splitting
    # ------------------------------------------------------------------

    def _split_into_blocks(self, markdown: str) -> list[dict]:
        """
        Split markdown into atomic blocks (paragraphs and tables).

        Tables are detected via <table>...</table> regex and kept intact.
        Remaining text is split on double-newlines into paragraphs.
        """
        blocks: list[dict] = []
        last_end = 0

        for match in _TABLE_RE.finditer(markdown):
            # Text before this table → paragraphs
            before = markdown[last_end : match.start()].strip()
            if before:
                self._add_text_blocks(before, blocks)

            # Table as atomic block
            blocks.append({"text": match.group(), "is_table": True})
            last_end = match.end()

        # Text after last table
        after = markdown[last_end:].strip()
        if after:
            self._add_text_blocks(after, blocks)

        return blocks

    @staticmethod
    def _add_text_blocks(text: str, blocks: list[dict]) -> None:
        """Split text on double-newlines and append non-empty paragraphs."""
        for para in text.split("\n\n"):
            para = para.strip()
            if para:
                blocks.append({"text": para, "is_table": False})

    # ------------------------------------------------------------------
    # Block merging
    # ------------------------------------------------------------------

    def _merge_blocks(
        self,
        blocks: list[dict],
        accession_number: str,
        section_item: str,
        section_title: str | None,
        context_prefix: str,
    ) -> list[Chunk]:
        """Greedy merge of blocks into chunks respecting token limits."""
        chunks: list[Chunk] = []
        current_parts: list[dict] = []
        current_tokens = 0
        chunk_index = 0

        for block in blocks:
            block_tokens = self.estimate_tokens(block["text"])

            # Oversized table → emit solo
            if block["is_table"] and block_tokens > self.config.max_tokens:
                if current_parts:
                    chunks.append(
                        self._make_chunk(
                            current_parts, chunk_index, accession_number,
                            section_item, section_title, context_prefix,
                        )
                    )
                    chunk_index += 1
                    current_parts = []
                    current_tokens = 0

                chunks.append(
                    self._make_chunk(
                        [block], chunk_index, accession_number,
                        section_item, section_title, context_prefix,
                    )
                )
                chunk_index += 1
                continue

            # Would exceed max → flush current, start new with overlap
            if current_tokens + block_tokens > self.config.max_tokens and current_parts:
                chunks.append(
                    self._make_chunk(
                        current_parts, chunk_index, accession_number,
                        section_item, section_title, context_prefix,
                    )
                )
                chunk_index += 1

                overlap = self._get_overlap(current_parts)
                current_parts = list(overlap)
                current_tokens = sum(self.estimate_tokens(p["text"]) for p in current_parts)

            current_parts.append(block)
            current_tokens += block_tokens

        # Flush remaining
        if current_parts:
            tokens = sum(self.estimate_tokens(p["text"]) for p in current_parts)
            has_table = any(p["is_table"] for p in current_parts)
            if tokens >= self.config.min_tokens or has_table:
                chunks.append(
                    self._make_chunk(
                        current_parts, chunk_index, accession_number,
                        section_item, section_title, context_prefix,
                    )
                )

        return chunks

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_chunk(
        self,
        parts: list[dict],
        chunk_index: int,
        accession_number: str,
        section_item: str,
        section_title: str | None,
        context_prefix: str,
    ) -> Chunk:
        """Assemble a Chunk from a list of block dicts."""
        text = "\n\n".join(p["text"] for p in parts)
        # Normalize the section item for the chunk_id (remove spaces, lowercase)
        item_key = section_item.replace(" ", "").upper()
        return Chunk(
            chunk_id=f"{accession_number}_{item_key}_{chunk_index:04d}",
            accession_number=accession_number,
            section_item=section_item,
            section_title=section_title,
            chunk_index=chunk_index,
            context_prefix=context_prefix,
            text=text,
            token_count=self.estimate_tokens(text),
            contains_table=any(p["is_table"] for p in parts),
        )

    def _get_overlap(self, parts: list[dict]) -> list[dict]:
        """Get trailing non-table paragraphs up to overlap_tokens."""
        overlap: list[dict] = []
        tokens = 0
        for part in reversed(parts):
            if part["is_table"]:
                break
            part_tokens = self.estimate_tokens(part["text"])
            if tokens + part_tokens > self.config.overlap_tokens:
                break
            overlap.insert(0, part)
            tokens += part_tokens
        return overlap
