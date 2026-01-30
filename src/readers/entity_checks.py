"""
Entity validation and filtering for improved accuracy.

Provides validation rules to filter out noisy entities:
- CARDINAL: Remove phone numbers, zip codes, page numbers
- DATE: Remove frequency words like "quarterly", "annual"
"""

from __future__ import annotations

import re
from typing import Any


def is_valid_cardinal(text: str) -> bool:
    """
    Filter out noise from CARDINAL entities.
    
    Args:
        text: Entity text to validate
    
    Returns:
        True if valid cardinal, False if noise
    """
    # Remove phone numbers
    if re.match(r'\(\d{3}\)\s*\d{3}-\d{4}', text):
        return False
    if re.match(r'\d{3}-\d{3}-\d{4}', text):
        return False
    
    # Remove zip codes
    if re.match(r'^\d{5}(-\d{4})?$', text):
        return False
    
    # Remove Roman numerals (often part of names: John Doe III)
    if text.strip() in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']:
        return False
    
    # Remove likely page numbers (single/double digits under 500)
    if re.match(r'^\d{1,2}$', text):
        try:
            if int(text) < 500:
                return False
        except ValueError:
            pass
    
    return True


def is_valid_date(text: str) -> bool:
    """
    Filter out frequency words from DATE entities.
    
    Args:
        text: Entity text to validate
    
    Returns:
        True if valid date, False if frequency word
    """
    # Remove frequency/period words
    frequency_words = {
        'quarterly', 'annual', 'monthly', 'weekly', 'daily',
        'first', 'second', 'third', 'fourth', 'fifth',
        'prior', 'current', 'subsequent', 'future',
        'initial', 'final', 'interim'
    }
    
    if text.lower() in frequency_words:
        return False
    
    # Try parsing as actual date
    try:
        from dateutil.parser import parse
        parse(text, fuzzy=False)
        return True
    except (ValueError, OverflowError):
        pass
    
    # Accept year patterns (1900-2100)
    if re.match(r'^\d{4}$', text):
        try:
            year = int(text)
            return 1900 <= year <= 2100
        except ValueError:
            return False
    
    # Accept quarter patterns (Q1 2021, Q4 2020, etc.)
    if re.match(r'^Q[1-4]\s*\d{4}$', text, re.IGNORECASE):
        return True
    
    # Accept month-day-year patterns
    months = '|'.join([
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ])
    if re.match(rf'({months})\s+\d{{1,2}},\s*\d{{4}}', text):
        return True
    
    # Accept ISO date patterns (2021-01-28)
    if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
        return True
    
    return False


def filter_entities(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Filter entity list to remove noise.
    
    Args:
        entities: List of entity dicts with 'type' and 'text' keys
    
    Returns:
        Filtered list of entities
    """
    filtered = []
    
    for entity in entities:
        ent_type = entity['type']
        text = entity['text']
        
        # Apply type-specific filters
        if ent_type == 'CARDINAL':
            if not is_valid_cardinal(text):
                continue
        
        if ent_type == 'DATE':
            if not is_valid_date(text):
                continue
        
        filtered.append(entity)
    
    return filtered
