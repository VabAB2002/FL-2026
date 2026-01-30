"""
LLM-based section finder for non-standard SEC filings.

Used as Tier 3 fallback for companies like INTC that use completely custom formats.
"""

from __future__ import annotations

import json
import logging
import os

from openai import OpenAI

logger = logging.getLogger(__name__)


class LLMSectionFinder:
    """
    Find sections in non-standard filings using LLM analysis.
    
    This is a last-resort fallback for edge cases where regex fails.
    Cost: ~$0.01 per filing.
    """

    def __init__(self, model: str = "gpt-4o-mini"):
        """
        Initialize LLM section finder.
        
        Args:
            model: OpenAI model to use (default: gpt-4o-mini)
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        self.client = OpenAI(api_key=api_key)
        self.model = model
        logger.info(f"LLMSectionFinder initialized with model: {model}")

    def find_section(self, full_markdown: str, item: str) -> str | None:
        """
        Find a specific section using LLM analysis.
        
        Strategy:
        1. Extract table of contents
        2. Ask LLM to map requested item to section title
        3. Find and extract that section from the markdown
        
        Args:
            full_markdown: Complete filing markdown
            item: Item number (e.g., "ITEM 1", "ITEM 10")
        
        Returns:
            Section text or None if not found
        """
        try:
            # Extract first ~20KB which usually contains ToC
            toc_text = full_markdown[:20000]
            
            # Ask LLM to find the section title
            prompt = self._build_section_mapping_prompt(toc_text, item)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a SEC filing analysis expert. Extract section titles from table of contents. Output valid JSON only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=200,
                temperature=0,
                response_format={"type": "json_object"},
            )
            
            content = response.choices[0].message.content
            if not content:
                logger.warning(f"Empty LLM response for {item}")
                return None
            
            data = json.loads(content)
            section_title = data.get("section_title")
            page_number = data.get("page_number")
            
            if not section_title:
                logger.warning(f"LLM could not find section title for {item}")
                return None
            
            logger.info(f"LLM mapped {item} -> '{section_title}' (page {page_number})")
            
            # Now find this section in the full markdown
            section_text = self._extract_by_title(full_markdown, section_title)
            
            if section_text:
                logger.info(f"LLM found {item} ({len(section_text)} chars)")
                return section_text
            
            logger.warning(f"Could not find section '{section_title}' in markdown")
            return None
        
        except Exception as e:
            logger.error(f"LLM section finding failed for {item}: {e}")
            return None

    def _build_section_mapping_prompt(self, toc_text: str, item: str) -> str:
        """Build prompt to find section title from ToC."""
        item_descriptions = {
            "ITEM 1": "Business",
            "ITEM 1A": "Risk Factors",
            "ITEM 7": "Management's Discussion and Analysis",
            "ITEM 10": "Directors, Executive Officers and Corporate Governance",
            "ITEM 11": "Executive Compensation",
        }
        
        description = item_descriptions.get(item, item)
        
        return f"""Analyze this SEC filing table of contents and find the section that corresponds to {item} ({description}).

The filing may use non-standard section names. Look for the section that covers {description.lower()}.

Table of Contents:
{toc_text}

Output JSON format:
{{
  "section_title": "The exact section title from the ToC",
  "page_number": The page number if shown (or null)
}}

If you cannot find a matching section, output:
{{
  "section_title": null,
  "page_number": null
}}"""

    def _extract_by_title(self, markdown: str, title: str) -> str | None:
        """
        Extract section by searching for the title.
        
        Args:
            markdown: Full markdown text
            title: Section title to search for
        
        Returns:
            Section text or None
        """
        import re
        
        # Search for the title as a heading
        patterns = [
            # Markdown heading
            re.compile(rf"(?:^|\n)#+\s*{re.escape(title)}\s*\n", re.IGNORECASE | re.MULTILINE),
            # Bold text
            re.compile(rf"(?:^|\n)\*\*{re.escape(title)}\*\*\s*\n", re.IGNORECASE | re.MULTILINE),
            # Plain text heading
            re.compile(rf"(?:^|\n){re.escape(title)}\s*\n", re.IGNORECASE | re.MULTILINE),
        ]
        
        for pattern in patterns:
            match = pattern.search(markdown)
            if match:
                start = match.start()
                
                # Find next heading/section
                next_heading = re.compile(r"(?:^|\n)#+\s+[A-Z]", re.MULTILINE)
                next_match = next_heading.search(markdown, start + len(match.group(0)))
                
                if next_match:
                    end = next_match.start()
                    return markdown[start:end].strip()
                else:
                    # Take next 50KB
                    return markdown[start:start + 50000].strip()
        
        return None
