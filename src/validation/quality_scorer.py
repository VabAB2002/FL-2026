"""
Data quality scoring system for SEC filings.

Assigns quality scores to filings and facts based on completeness and accuracy.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional

from ..config.extraction_config import get_quality_scoring_config
from ..storage.database import Database
from ..utils.logger import get_logger

logger = get_logger("finloom.validation.quality_scorer")


@dataclass
class QualityScore:
    """Quality score for a filing or dataset."""
    score: float  # 0-100
    grade: str  # A, B, C, D, F
    issues: List[str]
    metrics: Dict[str, float]


class DataQualityScorer:
    """
    Data quality scoring system.
    
    Evaluates filings and assigns quality scores based on various criteria.
    """
    
    # Required XBRL concepts for a complete 10-K
    REQUIRED_CONCEPTS = [
        'us-gaap:Assets',
        'us-gaap:Revenues',
        'us-gaap:NetIncomeLoss',
        'us-gaap:Liabilities',
        'us-gaap:StockholdersEquity',
        'us-gaap:CashAndCashEquivalentsAtCarryingValue',
    ]
    
    def __init__(self, db: Database, config: Optional['QualityScoringConfig'] = None):
        """
        Initialize quality scorer.
        
        Args:
            db: Database instance.
            config: Quality scoring configuration (uses default if not provided).
        """
        self.db = db
        self.config = config or get_quality_scoring_config()
    
    def score_filing(self, accession_number: str) -> QualityScore:
        """
        Score a single filing.
        
        Args:
            accession_number: Filing accession number.
        
        Returns:
            QualityScore object.
        """
        score = self.config.base_score
        issues = []
        metrics = {}
        
        # Get facts for this filing
        facts = self.db.connection.execute("""
            SELECT concept_name, value, unit, dimensions, period_end
            FROM facts
            WHERE accession_number = ?
        """, [accession_number]).fetchall()
        
        if not facts:
            return QualityScore(
                score=0.0,
                grade=self.config.score_to_grade(0.0),
                issues=['No facts extracted'],
                metrics={'fact_count': 0}
            )
        
        metrics['fact_count'] = len(facts)
        
        # Check for required concepts (use config helper)
        concepts = {f[0] for f in facts}
        missing_concepts = set(self.REQUIRED_CONCEPTS) - concepts
        if missing_concepts:
            penalty = self.config.calculate_missing_concepts_penalty(len(missing_concepts))
            score -= penalty
            issues.append(f"Missing {len(missing_concepts)} required concepts")
        metrics['concept_coverage'] = (len(concepts) / len(self.REQUIRED_CONCEPTS)) * 100
        
        # Check balance sheet equation (use config helper)
        assets = self._get_concept_value(facts, 'us-gaap:Assets')
        liabilities = self._get_concept_value(facts, 'us-gaap:Liabilities')
        equity = self._get_concept_value(facts, 'us-gaap:StockholdersEquity')
        
        if assets and liabilities and equity:
            diff_pct = abs((assets - (liabilities + equity)) / assets) * 100
            metrics['balance_sheet_accuracy'] = max(0, 100 - diff_pct)
            
            penalty = self.config.calculate_balance_sheet_penalty(diff_pct)
            if penalty > 0:
                score -= penalty
                issues.append(f"Balance sheet imbalance: {diff_pct:.2f}%")
        else:
            score -= self.config.incomplete_balance_penalty
            issues.append("Incomplete balance sheet data")
            metrics['balance_sheet_accuracy'] = 0
        
        # Check for duplicate facts (use config helper)
        duplicates = self._count_duplicates(facts)
        if duplicates > 0:
            penalty = self.config.calculate_duplicate_penalty(duplicates)
            score -= penalty
            issues.append(f"Found {duplicates} duplicate facts")
        metrics['duplicate_count'] = duplicates
        
        # Check for null values (use config helper)
        null_values = sum(1 for f in facts if f[1] is None)
        if null_values > 0:
            null_pct = (null_values / len(facts)) * 100
            penalty = self.config.calculate_null_value_penalty(null_pct)
            if penalty > 0:
                score -= penalty
                issues.append(f"{null_pct:.1f}% of facts have null values")
        metrics['null_value_percent'] = (null_values / len(facts)) * 100 if facts else 0
        
        # Check for dimensional complexity (too many dimensions can indicate issues)
        dimensional_facts = sum(1 for f in facts if f[3] is not None)
        dimensional_pct = (dimensional_facts / len(facts)) * 100 if facts else 0
        metrics['dimensional_fact_percent'] = dimensional_pct
        
        if dimensional_pct > 50:
            issues.append(f"High dimensional complexity: {dimensional_pct:.1f}%")
        
        # Final score (clamp to 0-100)
        score = max(0.0, min(100.0, score))
        grade = self.config.score_to_grade(score)
        
        return QualityScore(
            score=round(score, 2),
            grade=grade,
            issues=issues,
            metrics=metrics
        )
    
    def score_company(self, ticker: str) -> Dict:
        """
        Score all filings for a company.
        
        Args:
            ticker: Company ticker symbol.
        
        Returns:
            Dict with company-level quality metrics.
        """
        # Get all filings for company
        filings = self.db.connection.execute("""
            SELECT f.accession_number, f.filing_date
            FROM filings f
            JOIN companies c ON f.cik = c.cik
            WHERE c.ticker = ?
              AND f.xbrl_processed = TRUE
            ORDER BY f.filing_date DESC
        """, [ticker]).fetchall()
        
        if not filings:
            return {
                "ticker": ticker,
                "filing_count": 0,
                "average_score": 0.0,
                "grade_distribution": {}
            }
        
        scores = []
        grade_dist = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'F': 0}
        
        for accession, _ in filings:
            quality = self.score_filing(accession)
            scores.append(quality.score)
            grade_dist[quality.grade] += 1
        
        return {
            "ticker": ticker,
            "filing_count": len(filings),
            "average_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
            "min_score": min(scores) if scores else 0.0,
            "max_score": max(scores) if scores else 0.0,
            "grade_distribution": grade_dist
        }
    
    def score_all_companies(self) -> List[Dict]:
        """
        Score all companies in the database.
        
        Returns:
            List of company quality scores.
        """
        companies = self.db.connection.execute("""
            SELECT ticker FROM companies
            ORDER BY ticker
        """).fetchall()
        
        results = []
        for (ticker,) in companies:
            score = self.score_company(ticker)
            results.append(score)
        
        return results
    
    def _get_concept_value(
        self,
        facts: List,
        concept_name: str
    ) -> Optional[Decimal]:
        """Get value for a specific concept (non-dimensional)."""
        for fact in facts:
            if fact[0] == concept_name and fact[3] is None:  # No dimensions
                return fact[1]
        return None
    
    def _count_duplicates(self, facts: List) -> int:
        """Count duplicate facts."""
        seen = set()
        duplicates = 0
        
        for fact in facts:
            # Create key: concept + period_end + dimensions
            key = (fact[0], fact[4], str(fact[3]))
            if key in seen:
                duplicates += 1
            else:
                seen.add(key)
        
        return duplicates
    
    # NOTE: _score_to_grade() method removed - now using config.score_to_grade()
    # This allows for configurable grade thresholds
