"""
Data quality validation for SEC filing data.

Implements business rules and validation checks for financial data.
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Callable, Optional

from ..utils.logger import get_logger
from .schemas import Company, DataQualityIssue, Fact, Filing, Section

logger = get_logger("finloom.validation.quality")


@dataclass
class ValidationResult:
    """Result of a validation check."""
    valid: bool
    issues: list[DataQualityIssue] = field(default_factory=list)
    
    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")
    
    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")
    
    def add_issue(
        self,
        issue_type: str,
        severity: str,
        message: str,
        field_name: Optional[str] = None,
        expected_value: Optional[str] = None,
        actual_value: Optional[str] = None,
        accession_number: Optional[str] = None,
    ) -> None:
        """Add a validation issue."""
        self.issues.append(DataQualityIssue(
            accession_number=accession_number,
            issue_type=issue_type,
            severity=severity,
            field_name=field_name,
            message=message,
            expected_value=expected_value,
            actual_value=actual_value,
        ))
        if severity == "error":
            self.valid = False


class DataQualityChecker:
    """
    Validates data quality for SEC filing data.
    
    Implements business rules including:
    - Balance sheet equation validation
    - Required field checks
    - Value range validation
    - Consistency checks
    """
    
    # Required XBRL concepts for 10-K filings
    REQUIRED_CONCEPTS = [
        "us-gaap:Assets",
        "us-gaap:Liabilities",
        "us-gaap:StockholdersEquity",
    ]
    
    # Balance sheet concepts for equation validation
    BALANCE_SHEET_ASSETS = [
        "us-gaap:Assets",
    ]
    
    BALANCE_SHEET_LIABILITIES = [
        "us-gaap:Liabilities",
        "us-gaap:LiabilitiesAndStockholdersEquity",
    ]
    
    BALANCE_SHEET_EQUITY = [
        "us-gaap:StockholdersEquity",
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ]
    
    def __init__(
        self,
        tolerance_percent: float = 1.0,
        strict_mode: bool = False,
    ) -> None:
        """
        Initialize data quality checker.
        
        Args:
            tolerance_percent: Tolerance for balance sheet equation (default 1%).
            strict_mode: If True, treat warnings as errors.
        """
        self.tolerance_percent = tolerance_percent
        self.strict_mode = strict_mode
        
        logger.info(
            f"Data quality checker initialized. "
            f"Tolerance: {tolerance_percent}%, Strict: {strict_mode}"
        )
    
    def validate_company(self, company: dict | Company) -> ValidationResult:
        """
        Validate company data.
        
        Args:
            company: Company data as dict or Company model.
        
        Returns:
            ValidationResult with any issues found.
        """
        result = ValidationResult(valid=True)
        
        try:
            if isinstance(company, dict):
                company = Company(**company)
        except Exception as e:
            result.add_issue(
                issue_type="validation_error",
                severity="error",
                message=f"Company validation failed: {e}",
            )
            return result
        
        # Check required fields
        if not company.company_name:
            result.add_issue(
                issue_type="missing_field",
                severity="error",
                field_name="company_name",
                message="Company name is required",
            )
        
        return result
    
    def validate_filing(
        self,
        filing: dict | Filing,
        accession_number: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate filing metadata.
        
        Args:
            filing: Filing data as dict or Filing model.
            accession_number: Accession number for issue tracking.
        
        Returns:
            ValidationResult with any issues found.
        """
        result = ValidationResult(valid=True)
        
        try:
            if isinstance(filing, dict):
                filing = Filing(**filing)
            accession_number = accession_number or filing.accession_number
        except Exception as e:
            result.add_issue(
                issue_type="validation_error",
                severity="error",
                message=f"Filing validation failed: {e}",
                accession_number=accession_number,
            )
            return result
        
        # Check date consistency
        if filing.period_of_report and filing.filing_date:
            days_diff = (filing.filing_date - filing.period_of_report).days
            
            # 10-K should be filed within 60-90 days of period end
            if filing.form_type == "10-K":
                if days_diff < 0:
                    result.add_issue(
                        issue_type="date_inconsistency",
                        severity="error",
                        field_name="period_of_report",
                        message="Period of report is after filing date",
                        expected_value=f"<= {filing.filing_date}",
                        actual_value=str(filing.period_of_report),
                        accession_number=accession_number,
                    )
                elif days_diff > 120:
                    result.add_issue(
                        issue_type="date_inconsistency",
                        severity="warning",
                        field_name="period_of_report",
                        message="Filing date is more than 120 days after period end",
                        accession_number=accession_number,
                    )
        
        return result
    
    def validate_facts(
        self,
        facts: list[dict | Fact],
        accession_number: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate a set of XBRL facts.
        
        Performs:
        - Required concept checks
        - Balance sheet equation validation
        - Value range checks
        - Duplicate detection
        
        Args:
            facts: List of facts as dicts or Fact models.
            accession_number: Accession number for issue tracking.
        
        Returns:
            ValidationResult with any issues found.
        """
        result = ValidationResult(valid=True)
        
        # Convert to Fact objects
        validated_facts = []
        for fact in facts:
            try:
                if isinstance(fact, dict):
                    fact = Fact(**fact)
                validated_facts.append(fact)
            except Exception as e:
                result.add_issue(
                    issue_type="validation_error",
                    severity="warning",
                    message=f"Fact validation failed: {e}",
                    accession_number=accession_number,
                )
        
        if not validated_facts:
            result.add_issue(
                issue_type="no_facts",
                severity="error",
                message="No valid facts found in filing",
                accession_number=accession_number,
            )
            return result
        
        # Group facts by concept for analysis
        facts_by_concept = {}
        for fact in validated_facts:
            if fact.concept_name not in facts_by_concept:
                facts_by_concept[fact.concept_name] = []
            facts_by_concept[fact.concept_name].append(fact)
        
        # Check required concepts
        self._check_required_concepts(facts_by_concept, result, accession_number)
        
        # Validate balance sheet equation
        self._validate_balance_sheet(facts_by_concept, result, accession_number)
        
        # Check for negative values in unexpected places
        self._check_value_signs(validated_facts, result, accession_number)
        
        # Check for duplicates
        self._check_duplicates(validated_facts, result, accession_number)
        
        logger.debug(f"Validated {len(validated_facts)} facts, found {len(result.issues)} issues")
        return result
    
    def validate_fact_completeness(
        self,
        facts: list,
        accession_number: str,
        extract_all_mode: bool
    ) -> list[dict]:
        """
        Validate fact count is within expected ranges.
        
        Args:
            facts: List of extracted facts
            accession_number: Filing identifier
            extract_all_mode: Whether full extraction was used
        
        Returns:
            List of validation issues as dicts
        """
        issues = []
        fact_count = len(facts)
        
        # Expected ranges based on mode
        if extract_all_mode:
            min_expected = 500  # Minimum for full extraction
            max_expected = 3000  # Maximum reasonable
            mode = "full extraction"
        else:
            min_expected = 20  # Minimum core facts
            max_expected = 150  # Maximum core facts
            mode = "core concepts only"
        
        if fact_count < min_expected:
            issues.append({
                "type": "fact_count_low",
                "severity": "warning",
                "message": f"Low fact count for {mode}: {fact_count} facts (expected {min_expected}+)",
                "accession_number": accession_number,
                "field_name": "fact_count",
                "actual_value": str(fact_count),
                "expected_value": f"{min_expected}+",
            })
        
        if fact_count > max_expected:
            issues.append({
                "type": "fact_count_high",
                "severity": "warning",
                "message": f"Unusually high fact count: {fact_count} facts (max expected {max_expected})",
                "accession_number": accession_number,
                "field_name": "fact_count",
                "actual_value": str(fact_count),
                "expected_value": f"<{max_expected}",
            })
        
        logger.debug(f"Fact completeness check: {fact_count} facts in {mode} mode, {len(issues)} issues")
        return issues
    
    def _check_required_concepts(
        self,
        facts_by_concept: dict[str, list[Fact]],
        result: ValidationResult,
        accession_number: Optional[str],
    ) -> None:
        """Check for required XBRL concepts."""
        for concept in self.REQUIRED_CONCEPTS:
            if concept not in facts_by_concept:
                result.add_issue(
                    issue_type="missing_concept",
                    severity="warning",
                    field_name=concept,
                    message=f"Required concept '{concept}' not found",
                    accession_number=accession_number,
                )
    
    def _validate_balance_sheet(
        self,
        facts_by_concept: dict[str, list[Fact]],
        result: ValidationResult,
        accession_number: Optional[str],
    ) -> None:
        """
        Validate balance sheet equation: Assets = Liabilities + Equity.
        """
        # Get most recent values (no dimensions)
        assets = self._get_primary_value(facts_by_concept, self.BALANCE_SHEET_ASSETS)
        liabilities = self._get_primary_value(facts_by_concept, self.BALANCE_SHEET_LIABILITIES)
        equity = self._get_primary_value(facts_by_concept, self.BALANCE_SHEET_EQUITY)
        
        if assets is None or (liabilities is None and equity is None):
            # Can't validate without values
            return
        
        # Handle case where LiabilitiesAndStockholdersEquity is used
        if liabilities is not None and liabilities == assets:
            # This means they reported L&SE as a single line
            return
        
        if liabilities is not None and equity is not None:
            calculated_assets = liabilities + equity
            
            if assets != Decimal(0):
                diff_percent = abs((assets - calculated_assets) / assets) * 100
                
                if diff_percent > self.tolerance_percent:
                    result.add_issue(
                        issue_type="balance_sheet_imbalance",
                        severity="warning" if diff_percent < 5 else "error",
                        message=(
                            f"Balance sheet equation failed. "
                            f"Assets ({assets:,.0f}) != Liabilities ({liabilities:,.0f}) + "
                            f"Equity ({equity:,.0f}). Diff: {diff_percent:.2f}%"
                        ),
                        expected_value=str(assets),
                        actual_value=str(calculated_assets),
                        accession_number=accession_number,
                    )
    
    def _get_primary_value(
        self,
        facts_by_concept: dict[str, list[Fact]],
        concept_names: list[str],
    ) -> Optional[Decimal]:
        """Get the primary (non-dimensional) value for concepts."""
        for concept in concept_names:
            facts = facts_by_concept.get(concept, [])
            
            # Filter to non-dimensional facts
            primary_facts = [f for f in facts if not f.dimensions]
            
            if primary_facts:
                # Get most recent by period_end
                primary_facts.sort(
                    key=lambda f: f.period_end or date.min,
                    reverse=True
                )
                if primary_facts[0].value is not None:
                    return primary_facts[0].value
        
        return None
    
    def _check_value_signs(
        self,
        facts: list[Fact],
        result: ValidationResult,
        accession_number: Optional[str],
    ) -> None:
        """Check for unexpected negative values."""
        # Concepts that should typically be positive
        positive_concepts = [
            "us-gaap:Assets",
            "us-gaap:AssetsCurrent",
            "us-gaap:Revenues",
            "us-gaap:CommonStockSharesOutstanding",
        ]
        
        for fact in facts:
            if fact.value is not None and fact.value < 0:
                if any(fact.concept_name == c for c in positive_concepts):
                    if not fact.is_negated:
                        result.add_issue(
                            issue_type="unexpected_negative",
                            severity="warning",
                            field_name=fact.concept_name,
                            message=f"Unexpected negative value for {fact.concept_name}",
                            actual_value=str(fact.value),
                            accession_number=accession_number,
                        )
    
    def _check_duplicates(
        self,
        facts: list[Fact],
        result: ValidationResult,
        accession_number: Optional[str],
    ) -> None:
        """Check for duplicate facts."""
        seen = set()
        duplicates = []
        
        for fact in facts:
            # Create a key for comparison
            key = (
                fact.concept_name,
                fact.period_end,
                fact.period_start,
                str(fact.dimensions) if fact.dimensions else None,
            )
            
            if key in seen:
                duplicates.append(fact.concept_name)
            else:
                seen.add(key)
        
        if duplicates:
            result.add_issue(
                issue_type="duplicate_facts",
                severity="warning",
                message=f"Found {len(duplicates)} duplicate facts",
                accession_number=accession_number,
            )
    
    def validate_sections(
        self,
        sections: list[dict | Section],
        accession_number: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate extracted sections.
        
        Args:
            sections: List of sections as dicts or Section models.
            accession_number: Accession number for issue tracking.
        
        Returns:
            ValidationResult with any issues found.
        """
        result = ValidationResult(valid=True)
        
        # Required sections for 10-K
        required_sections = {"item_1", "item_1a", "item_7"}
        found_sections = set()
        
        for section in sections:
            try:
                if isinstance(section, dict):
                    section = Section(**section)
                found_sections.add(section.section_type)
                
                # Check section length
                if section.word_count and section.word_count < 100:
                    result.add_issue(
                        issue_type="short_section",
                        severity="warning",
                        field_name=section.section_type,
                        message=f"Section {section.section_type} is unusually short ({section.word_count} words)",
                        accession_number=accession_number,
                    )
                    
            except Exception as e:
                result.add_issue(
                    issue_type="validation_error",
                    severity="warning",
                    message=f"Section validation failed: {e}",
                    accession_number=accession_number,
                )
        
        # Check for missing required sections
        missing = required_sections - found_sections
        for section_type in missing:
            result.add_issue(
                issue_type="missing_section",
                severity="warning",
                field_name=section_type,
                message=f"Required section {section_type} not found",
                accession_number=accession_number,
            )
        
        return result
    
    def validate_filing_complete(
        self,
        filing: dict | Filing,
        facts: list[dict | Fact],
        sections: list[dict | Section],
    ) -> ValidationResult:
        """
        Perform complete validation of a filing.
        
        Args:
            filing: Filing metadata.
            facts: Extracted XBRL facts.
            sections: Extracted text sections.
        
        Returns:
            Combined ValidationResult.
        """
        result = ValidationResult(valid=True)
        
        # Get accession number
        if isinstance(filing, dict):
            accession_number = filing.get("accession_number")
        else:
            accession_number = filing.accession_number
        
        # Validate filing metadata
        filing_result = self.validate_filing(filing, accession_number)
        result.issues.extend(filing_result.issues)
        result.valid = result.valid and filing_result.valid
        
        # Validate facts
        facts_result = self.validate_facts(facts, accession_number)
        result.issues.extend(facts_result.issues)
        result.valid = result.valid and facts_result.valid
        
        # Validate sections
        sections_result = self.validate_sections(sections, accession_number)
        result.issues.extend(sections_result.issues)
        result.valid = result.valid and sections_result.valid
        
        # Log summary
        logger.info(
            f"Validation complete for {accession_number}: "
            f"valid={result.valid}, errors={result.error_count}, "
            f"warnings={result.warning_count}"
        )
        
        return result
