"""
Semantic chunking engine for RAG-ready text extraction.

Features:
- 3-level hierarchical chunking (section, topic, paragraph)
- Smart boundary detection (preserve sentences, tables, lists)
- Token counting and overlap management
- Chunk metadata extraction
"""

import re
import uuid
from dataclasses import dataclass, field
from typing import Optional

from ..utils.logger import get_logger

logger = get_logger("finloom.processing.chunker")


@dataclass
class Chunk:
    """Represents a semantic chunk with metadata."""
    chunk_id: str
    filing_accession: str
    section_id: Optional[int]
    parent_chunk_id: Optional[str]
    
    # CONTENT
    chunk_text: str
    chunk_markdown: Optional[str] = None
    
    # HIERARCHY
    chunk_level: int = 2  # 1=section, 2=topic, 3=paragraph
    chunk_index: int = 0  # Position within parent
    
    # SIZE
    token_count: int = 0
    char_start: int = 0
    char_end: int = 0
    
    # COMPOSITION
    contains_tables: bool = False
    contains_lists: bool = False
    contains_numbers: bool = False
    
    # CONTEXT
    heading: Optional[str] = None
    section_type: Optional[str] = None
    
    # CROSS REFS
    cross_references: list = field(default_factory=list)
    
    # FOR FUTURE
    s3_path: Optional[str] = None
    embedding_vector: Optional[list] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        import json
        return {
            "chunk_id": self.chunk_id,
            "accession_number": self.filing_accession,
            "section_id": self.section_id,
            "parent_chunk_id": self.parent_chunk_id,
            "chunk_level": self.chunk_level,
            "chunk_index": self.chunk_index,
            "chunk_text": self.chunk_text,
            "chunk_markdown": self.chunk_markdown,
            "token_count": self.token_count,
            "char_start": self.char_start,
            "char_end": self.char_end,
            "heading": self.heading,
            "section_type": self.section_type,
            "contains_tables": self.contains_tables,
            "contains_lists": self.contains_lists,
            "contains_numbers": self.contains_numbers,
            "cross_references": json.dumps(self.cross_references) if self.cross_references else None,
            "s3_path": self.s3_path,
        }


class SemanticChunker:
    """
    Semantic chunker for RAG-ready text extraction.
    
    Implements 3-level hierarchical chunking:
    - Level 1: Section-level (metadata only)
    - Level 2: Semantic topic chunks (500-1000 tokens) - PRIMARY for RAG
    - Level 3: Paragraph-level (fine-grained retrieval)
    
    Smart rules:
    - Preserve sentence boundaries
    - Keep tables with context
    - Keep lists together
    - Add overlap for context
    """
    
    # Configuration
    TARGET_CHUNK_SIZE = 750  # tokens
    MIN_CHUNK_SIZE = 500
    MAX_CHUNK_SIZE = 1000
    OVERLAP_SIZE = 100  # tokens
    
    # Patterns
    SENTENCE_ENDINGS = r'[.!?]+[\s"\']+'
    PARAGRAPH_BREAK = r'\n\n+'
    HEADING_PATTERN = r'^[A-Z][A-Za-z\s,:\-]{5,100}$'
    
    def __init__(self) -> None:
        """Initialize semantic chunker."""
        logger.info("Semantic chunker initialized")
    
    def create_chunks(
        self,
        sections: list,
        accession_number: str,
    ) -> list[Chunk]:
        """
        Create hierarchical semantic chunks from sections.
        
        Args:
            sections: List of ExtractedSection objects
            accession_number: Filing accession number
        
        Returns:
            List of Chunk objects (all 3 levels)
        """
        all_chunks = []
        
        for section in sections:
            # Level 1: Section-level chunk (metadata only)
            section_chunk = self._create_section_chunk(section, accession_number)
            all_chunks.append(section_chunk)
            
            # Level 2: Topic-level chunks (PRIMARY for RAG)
            topic_chunks = self._create_topic_chunks(
                section, accession_number, section_chunk.chunk_id
            )
            all_chunks.extend(topic_chunks)
            
            # Level 3: Paragraph-level chunks (for key sections only)
            if section.section_type in ['item_1', 'item_1a', 'item_7', 'item_8']:
                para_chunks = self._create_paragraph_chunks(
                    section, accession_number, topic_chunks
                )
                all_chunks.extend(para_chunks)
        
        logger.info(f"Created {len(all_chunks)} total chunks from {len(sections)} sections")
        return all_chunks
    
    def _create_section_chunk(
        self,
        section,
        accession_number: str,
    ) -> Chunk:
        """Create Level 1 section chunk (metadata only)."""
        
        chunk = Chunk(
            chunk_id=str(uuid.uuid4()),
            filing_accession=accession_number,
            section_id=getattr(section, 'id', None),
            parent_chunk_id=None,
            chunk_level=1,
            chunk_index=0,
            chunk_text=section.content_text,
            chunk_markdown=None,
            token_count=section.word_count,  # Approximate
            char_start=0,
            char_end=len(section.content_text),
            heading=section.section_title,
            section_type=section.section_type,
            contains_tables=section.contains_tables > 0 if hasattr(section, 'contains_tables') else False,
            contains_lists=section.contains_lists > 0 if hasattr(section, 'contains_lists') else False,
            contains_numbers=self._contains_numbers(section.content_text),
            cross_references=getattr(section, 'cross_references', []),
        )
        
        return chunk
    
    def _create_topic_chunks(
        self,
        section,
        accession_number: str,
        parent_id: str,
    ) -> list[Chunk]:
        """Create Level 2 topic chunks (500-1000 tokens)."""
        
        text = section.content_text
        chunks = []
        
        # Split by potential topic boundaries
        topic_boundaries = self._detect_topic_boundaries(text)
        
        # Create chunks with overlap
        current_pos = 0
        chunk_index = 0
        
        for boundary_start, boundary_end in topic_boundaries:
            chunk_text = text[boundary_start:boundary_end]
            token_count = self._count_tokens(chunk_text)
            
            # Skip if too small (unless it's the last chunk)
            if token_count < self.MIN_CHUNK_SIZE and boundary_end < len(text) - 100:
                continue
            
            # Truncate if too large
            if token_count > self.MAX_CHUNK_SIZE:
                chunk_text = self._truncate_to_token_limit(chunk_text, self.MAX_CHUNK_SIZE)
                token_count = self.MAX_CHUNK_SIZE
            
            # Extract heading
            heading = self._extract_chunk_heading(chunk_text)
            
            chunk = Chunk(
                chunk_id=str(uuid.uuid4()),
                filing_accession=accession_number,
                section_id=getattr(section, 'id', None),
                parent_chunk_id=parent_id,
                chunk_level=2,
                chunk_index=chunk_index,
                chunk_text=chunk_text,
                token_count=token_count,
                char_start=boundary_start,
                char_end=boundary_end,
                heading=heading,
                section_type=section.section_type,
                contains_tables=self._text_contains_tables(chunk_text),
                contains_lists=self._text_contains_lists(chunk_text),
                contains_numbers=self._contains_numbers(chunk_text),
            )
            
            chunks.append(chunk)
            chunk_index += 1
        
        return chunks
    
    def _create_paragraph_chunks(
        self,
        section,
        accession_number: str,
        parent_chunks: list[Chunk],
    ) -> list[Chunk]:
        """Create Level 3 paragraph chunks (fine-grained)."""
        
        chunks = []
        
        # Split text into paragraphs
        text = section.content_text
        paragraphs = re.split(self.PARAGRAPH_BREAK, text)
        
        chunk_index = 0
        
        for para in paragraphs:
            para = para.strip()
            if not para or len(para) < 100:
                continue
            
            token_count = self._count_tokens(para)
            
            # Skip very short paragraphs
            if token_count < 50:
                continue
            
            # Find parent topic chunk
            parent_id = self._find_parent_chunk(para, parent_chunks)
            
            chunk = Chunk(
                chunk_id=str(uuid.uuid4()),
                filing_accession=accession_number,
                section_id=getattr(section, 'id', None),
                parent_chunk_id=parent_id,
                chunk_level=3,
                chunk_index=chunk_index,
                chunk_text=para,
                token_count=token_count,
                section_type=section.section_type,
                contains_numbers=self._contains_numbers(para),
            )
            
            chunks.append(chunk)
            chunk_index += 1
        
        return chunks
    
    def _detect_topic_boundaries(self, text: str) -> list[tuple[int, int]]:
        """Detect topic boundaries using headings and paragraph breaks."""
        
        boundaries = []
        lines = text.split('\n')
        
        current_start = 0
        current_pos = 0
        current_size = 0
        
        for line in lines:
            line_len = len(line) + 1  # +1 for newline
            
            # Check if line looks like a heading
            if re.match(self.HEADING_PATTERN, line.strip()) and len(line.strip()) > 10:
                # Save previous chunk if it's big enough
                if current_size >= self.MIN_CHUNK_SIZE:
                    boundaries.append((current_start, current_pos))
                    current_start = current_pos
                    current_size = 0
            
            # Check if we've reached target size
            elif current_size >= self.TARGET_CHUNK_SIZE:
                # Find next sentence boundary
                remaining = text[current_pos:]
                sentence_match = re.search(self.SENTENCE_ENDINGS, remaining)
                
                if sentence_match:
                    end_pos = current_pos + sentence_match.end()
                    boundaries.append((current_start, end_pos))
                    current_start = end_pos - self.OVERLAP_SIZE * 4  # Approx chars per token
                    current_size = 0
            
            current_pos += line_len
            current_size = self._count_tokens(text[current_start:current_pos])
        
        # Add final chunk
        if current_size > self.MIN_CHUNK_SIZE // 2:
            boundaries.append((current_start, len(text)))
        
        return boundaries
    
    def _count_tokens(self, text: str) -> int:
        """Approximate token count (simple whitespace-based)."""
        # Rough approximation: 1 token ≈ 4 characters
        # More accurate would be using tiktoken, but this is sufficient
        return len(text.split())
    
    def _truncate_to_token_limit(self, text: str, max_tokens: int) -> str:
        """Truncate text to token limit at sentence boundary."""
        words = text.split()
        
        if len(words) <= max_tokens:
            return text
        
        # Truncate to max tokens
        truncated = ' '.join(words[:max_tokens])
        
        # Find last sentence boundary
        sentences = re.split(self.SENTENCE_ENDINGS, truncated)
        if len(sentences) > 1:
            # Return all complete sentences
            return ' '.join(sentences[:-1]) + '.'
        
        return truncated
    
    def _extract_chunk_heading(self, text: str) -> Optional[str]:
        """Extract heading from chunk (first line if it looks like a heading)."""
        lines = text.split('\n')
        
        if lines:
            first_line = lines[0].strip()
            # Check if first line looks like a heading
            if re.match(self.HEADING_PATTERN, first_line) and len(first_line) < 100:
                return first_line
        
        return None
    
    def _text_contains_tables(self, text: str) -> bool:
        """Check if text likely contains table content."""
        # Look for patterns like multiple columns aligned with spaces
        lines = text.split('\n')
        
        # Count lines with multiple tab-separated or heavily-spaced columns
        table_like_lines = 0
        for line in lines[:20]:  # Check first 20 lines
            if '\t' in line or re.search(r'\s{3,}', line):
                table_like_lines += 1
        
        return table_like_lines > 3
    
    def _text_contains_lists(self, text: str) -> bool:
        """Check if text contains lists."""
        list_patterns = [
            r'^\s*[\•\-\*]\s+',
            r'^\s*\d+\.\s+',
            r'^\s*\([a-z]\)\s+',
            r'^\s*[ivxIVX]+\.\s+',
        ]
        
        lines = text.split('\n')
        list_lines = 0
        
        for line in lines:
            for pattern in list_patterns:
                if re.match(pattern, line):
                    list_lines += 1
                    break
        
        return list_lines > 2
    
    def _contains_numbers(self, text: str) -> bool:
        """Check if text contains significant numeric content."""
        # Look for numbers with formatting (currency, percentages, large numbers)
        number_patterns = [
            r'\$[\d,]+',
            r'\d+\.\d+%',
            r'\d{1,3}(,\d{3})+',
        ]
        
        count = 0
        for pattern in number_patterns:
            count += len(re.findall(pattern, text))
        
        return count > 5
    
    def _find_parent_chunk(self, text: str, parent_chunks: list[Chunk]) -> Optional[str]:
        """Find which parent chunk this text belongs to."""
        # Simple approach: find parent chunk whose text contains this paragraph
        for chunk in parent_chunks:
            if text in chunk.chunk_text:
                return chunk.chunk_id
        
        # If not found, return first parent chunk
        return parent_chunks[0].chunk_id if parent_chunks else None
