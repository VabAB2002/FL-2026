#!/usr/bin/env python3
"""
Data Quality Assessment Tool

Runs reconciliation and quality scoring on the FinLoom database.

Usage:
    python scripts/assess_quality.py --reconcile
    python scripts/assess_quality.py --score AAPL
    python scripts/assess_quality.py --score-all
    python scripts/assess_quality.py --full
"""

import argparse
import json
import sys
from pathlib import Path
from tabulate import tabulate

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.validation.reconciliation import ReconciliationEngine
from src.validation.quality_scorer import DataQualityScorer
from src.utils.logger import get_logger, setup_logging

logger = get_logger("finloom.assess_quality")


def run_reconciliation(db: Database) -> None:
    """Run data reconciliation checks."""
    print("\n" + "="*80)
    print("  DATA RECONCILIATION")
    print("="*80 + "\n")
    
    engine = ReconciliationEngine(db)
    results = engine.run_all_checks()
    
    # Print summary
    print(f"Total Checks: {results['total_checks']}")
    print(f"Total Issues: {results['total_issues']}")
    print(f"  Critical: {results['critical_issues']}")
    print(f"  Errors: {results['error_issues']}")
    print(f"  Warnings: {results['warning_issues']}")
    
    # Print issues
    if results['total_issues'] > 0:
        print("\n" + "="*80)
        print("  ISSUES FOUND")
        print("="*80 + "\n")
        
        for issue in results['issues']:
            severity_icon = {
                'critical': '🔴',
                'error': '🟠',
                'warning': '🟡',
                'info': '🔵'
            }.get(issue['severity'], '⚪')
            
            print(f"{severity_icon} [{issue['severity'].upper()}] {issue['description']}")
            print(f"   Affected records: {issue['affected_records']}")
            if issue.get('details'):
                print(f"   Details: {json.dumps(issue['details'], indent=2)}")
            print()
    else:
        print("\n✅ No issues found - data quality is excellent!\n")


def score_company(db: Database, ticker: str) -> None:
    """Score a specific company."""
    print("\n" + "="*80)
    print(f"  DATA QUALITY SCORE: {ticker}")
    print("="*80 + "\n")
    
    scorer = DataQualityScorer(db)
    result = scorer.score_company(ticker)
    
    if result['filing_count'] == 0:
        print(f"No filings found for {ticker}\n")
        return
    
    print(f"Company: {result['ticker']}")
    print(f"Filings Analyzed: {result['filing_count']}")
    print(f"\nAverage Quality Score: {result['average_score']:.2f}/100")
    print(f"Score Range: {result['min_score']:.2f} - {result['max_score']:.2f}")
    
    print("\nGrade Distribution:")
    for grade in ['A', 'B', 'C', 'D', 'F']:
        count = result['grade_distribution'][grade]
        if count > 0:
            bar = '█' * count
            print(f"  {grade}: {bar} ({count})")
    print()


def score_all_companies(db: Database) -> None:
    """Score all companies."""
    print("\n" + "="*80)
    print("  DATA QUALITY SCORES - ALL COMPANIES")
    print("="*80 + "\n")
    
    scorer = DataQualityScorer(db)
    results = scorer.score_all_companies()
    
    # Prepare table data
    table_data = []
    for result in results:
        if result['filing_count'] > 0:
            # Determine primary grade
            grade_dist = result['grade_distribution']
            primary_grade = max(grade_dist, key=grade_dist.get)
            
            table_data.append([
                result['ticker'],
                result['filing_count'],
                f"{result['average_score']:.2f}",
                primary_grade,
                f"{result['min_score']:.2f} - {result['max_score']:.2f}"
            ])
    
    # Sort by average score descending
    table_data.sort(key=lambda x: float(x[2]), reverse=True)
    
    # Print table
    headers = ["Ticker", "Filings", "Avg Score", "Grade", "Range"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Summary statistics
    if table_data:
        avg_scores = [float(row[2]) for row in table_data]
        overall_avg = sum(avg_scores) / len(avg_scores)
        print(f"\nOverall Average Score: {overall_avg:.2f}/100")
        
        grade_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'F': 0}
        for row in table_data:
            grade_counts[row[3]] += 1
        
        print("\nOverall Grade Distribution:")
        for grade in ['A', 'B', 'C', 'D', 'F']:
            count = grade_counts[grade]
            pct = (count / len(table_data)) * 100 if table_data else 0
            print(f"  {grade}: {count:2d} ({pct:5.1f}%)")
    print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Assess data quality in FinLoom database"
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Run data reconciliation checks"
    )
    parser.add_argument(
        "--score",
        type=str,
        metavar="TICKER",
        help="Score a specific company"
    )
    parser.add_argument(
        "--score-all",
        action="store_true",
        help="Score all companies"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run all assessments (reconciliation + scoring)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Save results to JSON file"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Initialize database
    db = Database()
    
    try:
        # Run requested assessments
        if args.full:
            run_reconciliation(db)
            score_all_companies(db)
        else:
            if args.reconcile:
                run_reconciliation(db)
            if args.score:
                score_company(db, args.score.upper())
            if args.score_all:
                score_all_companies(db)
            
            # Show help if no options specified
            if not (args.reconcile or args.score or args.score_all):
                parser.print_help()
                return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Quality assessment failed: {e}", exc_info=True)
        print(f"\n❌ Error: {e}\n")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
