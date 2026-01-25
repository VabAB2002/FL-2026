#!/usr/bin/env python3
"""
View normalized financial data for companies.
Displays Bloomberg-style standardized metrics.

Usage:
    python view_normalized.py NVDA           # All years for NVIDIA
    python view_normalized.py NVDA 2023      # Only 2023 for NVIDIA
    python view_normalized.py AAPL 2024      # Only 2024 for Apple
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database


def view_company_metrics(ticker='NVDA', year=None):
    """Display all normalized metrics for a company."""
    db = Database()
    
    # Get company name
    company = db.connection.execute(
        "SELECT company_name FROM companies WHERE ticker = ?", [ticker]
    ).fetchone()
    
    if not company:
        print(f"\nError: Company '{ticker}' not found in database\n")
        db.close()
        return
    
    company_name = company[0]
    
    print("\n" + "=" * 80)
    print(f"  {company_name.upper()} ({ticker}) - Normalized Financial Metrics")
    if year:
        print(f"  Fiscal Year: {year}")
    print("=" * 80 + "\n")
    
    # Build query
    sql = '''
        SELECT 
            n.fiscal_year,
            s.category,
            s.display_label,
            n.metric_value,
            s.data_type,
            n.confidence_score
        FROM normalized_financials n
        JOIN standardized_metrics s ON n.metric_id = s.metric_id
        WHERE n.company_ticker = ?
    '''
    params = [ticker]
    
    if year:
        sql += ' AND n.fiscal_year = ?'
        params.append(year)
    
    sql += ' ORDER BY n.fiscal_year DESC, s.category, s.display_label'
    
    # Get all metrics
    results = db.connection.execute(sql, params).fetchall()
    
    if not results:
        year_msg = f" for year {year}" if year else ""
        print(f"No normalized data found for {ticker}{year_msg}\n")
        db.close()
        return
    
    current_year = None
    current_category = None
    
    for year, category, label, value, dtype, conf in results:
        # Print year header
        if year != current_year:
            if current_year is not None:
                print()
            current_year = year
            print(f"\n{'=' * 80}")
            print(f"  FISCAL YEAR {int(year)}")
            print("=" * 80 + "\n")
            current_category = None
        
        # Print category header
        if category != current_category:
            current_category = category
            cat_title = category.upper().replace('_', ' ')
            print(f"\n{cat_title}:")
            print("-" * 80)
        
        # Format value
        value_f = float(value)
        if dtype == 'monetary':
            if abs(value_f) >= 1e9:
                value_str = f"${value_f/1e9:>10.2f}B"
            elif abs(value_f) >= 1e6:
                value_str = f"${value_f/1e6:>10.2f}M"
            else:
                value_str = f"${value_f:>10,.2f}"
        elif dtype == 'shares':
            if value_f >= 1e9:
                value_str = f"{value_f/1e9:>10.2f}B shares"
            elif value_f >= 1e6:
                value_str = f"{value_f/1e6:>10.2f}M shares"
            else:
                value_str = f"{value_f:>10,.0f} shares"
        elif dtype == 'per_share':
            value_str = f"${value_f:>10.2f}"
        else:
            value_str = f"{value_f:>10,.2f}"
        
        print(f"  {label:50} {value_str:>20} (conf: {conf:.2f})")
    
    # Summary
    print(f"\n\n{'=' * 80}")
    print("SUMMARY")
    print("=" * 80)
    
    # Get summary
    summary_sql = '''
        SELECT 
            COUNT(DISTINCT fiscal_year) as years,
            COUNT(DISTINCT metric_id) as unique_metrics,
            COUNT(*) as total_data_points
        FROM normalized_financials
        WHERE company_ticker = ?
    '''
    summary_params = [ticker]
    
    if year:
        summary_sql += ' AND fiscal_year = ?'
        summary_params.append(year)
    
    summary = db.connection.execute(summary_sql, summary_params).fetchone()
    
    print(f"  Fiscal Years: {summary[0]}")
    print(f"  Unique Metrics: {summary[1]}")
    print(f"  Total Data Points: {summary[2]}")
    print()
    
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="View normalized financial metrics for a company",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python view_normalized.py NVDA           # All years for NVIDIA
  python view_normalized.py NVDA 2023      # Only 2023 for NVIDIA
  python view_normalized.py AAPL 2024      # Only 2024 for Apple
  python view_normalized.py MSFT           # All years for Microsoft
        """
    )
    
    parser.add_argument(
        "ticker",
        help="Company ticker symbol (e.g., NVDA, AAPL, MSFT)"
    )
    
    parser.add_argument(
        "year",
        nargs="?",
        type=int,
        help="Fiscal year (optional, shows all years if not specified)"
    )
    
    args = parser.parse_args()
    
    view_company_metrics(ticker=args.ticker, year=args.year)
