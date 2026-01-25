"""
Shared type aliases and type definitions for FinLoom.

Centralizes commonly used types for consistency across modules.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, TypeAlias

# Form types supported
FormType: TypeAlias = Literal["10-K", "10-Q", "8-K", "10-K/A", "10-Q/A"]

# Processing status
ProcessingStatus: TypeAlias = Literal["pending", "processing", "completed", "failed", "skipped"]

# Period types for XBRL facts
PeriodType: TypeAlias = Literal["instant", "duration", "unknown"]

# Severity levels
Severity: TypeAlias = Literal["error", "warning", "info"]

# Section types for 10-K filings
SectionType: TypeAlias = Literal[
    "item_1", "item_1a", "item_1b", "item_2", "item_3",
    "item_4", "item_5", "item_6", "item_7", "item_7a",
    "item_8", "item_9", "item_9a", "item_9b", "item_10",
    "item_11", "item_12", "item_13", "item_14", "item_15",
]

# CIK is always a 10-digit zero-padded string
CIK: TypeAlias = str

# Accession number format: XXXXXXXXXX-YY-NNNNNN
AccessionNumber: TypeAlias = str

# Common data containers
FactValue: TypeAlias = Decimal | str | None
Dimensions: TypeAlias = dict[str, str]

# Date types
DateLike: TypeAlias = date | datetime | str
