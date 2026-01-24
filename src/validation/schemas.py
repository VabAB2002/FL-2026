"""
Pydantic schemas for data validation.

Defines models for validating SEC filing data at all pipeline stages.
"""

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class Company(BaseModel):
    """Schema for company data."""
    cik: str = Field(..., min_length=1, max_length=10, description="SEC Central Index Key")
    company_name: str = Field(..., min_length=1, max_length=255)
    ticker: Optional[str] = Field(None, max_length=10)
    sic_code: Optional[str] = Field(None, max_length=4)
    sic_description: Optional[str] = None
    state_of_incorporation: Optional[str] = Field(None, max_length=2)
    fiscal_year_end: Optional[str] = Field(None, max_length=4)  # MMDD format
    category: Optional[str] = None
    ein: Optional[str] = None
    
    @field_validator("cik")
    @classmethod
    def validate_cik(cls, v: str) -> str:
        """Validate and normalize CIK to 10 digits."""
        # Remove leading zeros for validation
        cik_clean = v.lstrip("0")
        if not cik_clean.isdigit():
            raise ValueError("CIK must contain only digits")
        # Return zero-padded
        return v.zfill(10)
    
    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: Optional[str]) -> Optional[str]:
        """Validate ticker symbol."""
        if v is None:
            return None
        v = v.upper().strip()
        if not re.match(r"^[A-Z]{1,5}(-[A-Z])?$", v):
            # Allow some variations
            if not re.match(r"^[A-Z0-9\.\-]{1,10}$", v):
                raise ValueError("Invalid ticker format")
        return v


class Filing(BaseModel):
    """Schema for SEC filing metadata."""
    accession_number: str = Field(..., description="Unique filing identifier")
    cik: str = Field(..., min_length=1, max_length=10)
    form_type: Literal["10-K", "10-Q", "8-K", "10-K/A", "10-Q/A"] = Field(...)
    filing_date: date = Field(...)
    period_of_report: Optional[date] = None
    acceptance_datetime: Optional[datetime] = None
    primary_document: Optional[str] = None
    primary_doc_description: Optional[str] = None
    is_xbrl: bool = False
    is_inline_xbrl: bool = False
    edgar_url: Optional[str] = None
    local_path: Optional[str] = None
    download_status: str = "pending"
    
    @field_validator("accession_number")
    @classmethod
    def validate_accession_number(cls, v: str) -> str:
        """Validate accession number format."""
        # Format: 0000320193-24-000001 or 0000320193240000001
        v = v.strip()
        pattern_dash = r"^\d{10}-\d{2}-\d{6}$"
        pattern_nodash = r"^\d{18}$"
        
        if re.match(pattern_dash, v):
            return v
        elif re.match(pattern_nodash, v):
            # Convert to dash format
            return f"{v[:10]}-{v[10:12]}-{v[12:]}"
        else:
            raise ValueError(
                f"Invalid accession number format: {v}. "
                "Expected: XXXXXXXXXX-YY-NNNNNN"
            )
    
    @field_validator("cik")
    @classmethod
    def validate_cik(cls, v: str) -> str:
        """Validate CIK."""
        return v.zfill(10)
    
    @model_validator(mode="after")
    def validate_dates(self) -> "Filing":
        """Validate date relationships."""
        if self.period_of_report and self.filing_date:
            if self.period_of_report > self.filing_date:
                raise ValueError(
                    "period_of_report cannot be after filing_date"
                )
        return self


class Fact(BaseModel):
    """Schema for XBRL fact data."""
    accession_number: str = Field(...)
    concept_name: str = Field(..., min_length=1)
    concept_namespace: Optional[str] = None
    concept_local_name: Optional[str] = None
    value: Optional[Decimal] = None
    value_text: Optional[str] = None
    unit: Optional[str] = None
    decimals: Optional[int] = None
    period_type: Literal["instant", "duration", "unknown"] = "instant"
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    dimensions: Optional[dict[str, str]] = None
    is_custom: bool = False
    is_negated: bool = False
    
    @field_validator("value")
    @classmethod
    def validate_value(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Validate numeric value."""
        if v is None:
            return None
        
        # Flag unrealistic values (> 1 quadrillion)
        if abs(v) > Decimal("1e15"):
            raise ValueError(f"Unrealistic value: {v}")
        
        return v
    
    @field_validator("concept_name")
    @classmethod
    def validate_concept_name(cls, v: str) -> str:
        """Validate concept name format."""
        v = v.strip()
        if not v:
            raise ValueError("Concept name cannot be empty")
        return v
    
    @model_validator(mode="after")
    def validate_value_or_text(self) -> "Fact":
        """Ensure at least one of value or value_text is present."""
        if self.value is None and self.value_text is None:
            raise ValueError("Either value or value_text must be provided")
        return self
    
    @model_validator(mode="after")
    def validate_period(self) -> "Fact":
        """Validate period dates."""
        if self.period_type == "duration":
            if self.period_start and self.period_end:
                if self.period_start > self.period_end:
                    raise ValueError("period_start cannot be after period_end")
        return self


class Section(BaseModel):
    """Schema for extracted document sections."""
    accession_number: str = Field(...)
    section_type: str = Field(..., min_length=1)
    section_number: Optional[str] = None
    section_title: Optional[str] = None
    content_text: str = Field(..., min_length=1)
    content_html: Optional[str] = None
    word_count: Optional[int] = Field(None, ge=0)
    character_count: Optional[int] = Field(None, ge=0)
    paragraph_count: Optional[int] = Field(None, ge=0)
    extraction_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    extraction_method: Optional[str] = None
    
    @field_validator("section_type")
    @classmethod
    def validate_section_type(cls, v: str) -> str:
        """Validate section type."""
        valid_types = [
            "item_1", "item_1a", "item_1b", "item_2", "item_3",
            "item_4", "item_5", "item_6", "item_7", "item_7a",
            "item_8", "item_9", "item_9a", "item_9b", "item_10",
            "item_11", "item_12", "item_13", "item_14", "item_15",
        ]
        v = v.lower().strip()
        if v not in valid_types:
            raise ValueError(f"Invalid section type: {v}")
        return v
    
    @model_validator(mode="after")
    def calculate_counts(self) -> "Section":
        """Calculate counts if not provided."""
        if self.word_count is None and self.content_text:
            object.__setattr__(self, "word_count", len(self.content_text.split()))
        if self.character_count is None and self.content_text:
            object.__setattr__(self, "character_count", len(self.content_text))
        return self


class ProcessingLog(BaseModel):
    """Schema for processing log entries."""
    accession_number: Optional[str] = None
    cik: Optional[str] = None
    pipeline_stage: str = Field(...)
    operation: Optional[str] = None
    status: Literal["started", "completed", "failed", "skipped"] = Field(...)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    processing_time_ms: Optional[int] = Field(None, ge=0)
    records_processed: Optional[int] = Field(None, ge=0)
    records_failed: Optional[int] = Field(None, ge=0)
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    context: Optional[dict[str, Any]] = None


class DataQualityIssue(BaseModel):
    """Schema for data quality issues."""
    accession_number: Optional[str] = None
    issue_type: str = Field(...)
    severity: Literal["error", "warning", "info"] = Field(...)
    field_name: Optional[str] = None
    message: str = Field(..., min_length=1)
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None
