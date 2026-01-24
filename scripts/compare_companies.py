#!/usr/bin/env python3
"""
Compare normalized financial metrics across companies.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database


def compare_companies(tickers, year=2024, metrics=None):
    """Compare companies side-by-side."""
    db = Database()
    
    if metrics is None:
        # Default key metrics
        metrics = [
            'revenue', 'net_income', 'gross_profit', 'operating_income',
            'total_assets', 'stockholders_equity', 'operating_cash_flow',
            'rd_expense', 'eps_diluted'
        ]
    
    print("\n" + "=" * 120)
    print(f"  FINANCIAL COMPARISON - Fiscal Year {year}")
    print("=" * 120 + "\n")
    
    # Get company names
    companies = {}
    for ticker in tickers:
        result = db.connection.execute(
            "SELECT company_name FROM companies WHERE ticker = ?", [ticker]
        ).fetchone()
        if result:
            companies[ticker] = result[0]
    
    # Get normalized data for each metric
    for metric in metrics:
        # Get metric info
        metric_info = db.connection.execute(
            "SELECT display_label, data_type FROM standardized_metrics WHERE metric_id = ?",
            [metric]
        ).fetchone()
        
        if not metric_info:
            continue
        
        label, dtype = metric_info
        
        print(f"\n{label.upper()}")
        print("-" * 120)
        
        # Get values for each company
        values = {}
        for ticker in tickers:
            result = db.connection.execute('''
                SELECT metric_value, confidence_score
                FROM normalized_financials
                WHERE company_ticker = ? AND metric_id = ? AND fiscal_year = ?
                ORDER BY confidence_score DESC
                LIMIT 1
            ''', [ticker, metric, year]).fetchone()
            
            if result:
                values[ticker] = result
        
        # Print comparison
        row_parts = []
        for ticker in tickers:
            if ticker in values:
                value, conf = values[ticker]
                value_f = float(value)
                
                if dtype == 'monetary':
                    if abs(value_f) >= 1e9:
                        value_str = f"${value_f/1e9:.2f}B"
                    elif abs(value_f) >= 1e6:
                        value_str = f"${value_f/1e6:.2f}M"
                    else:
                        value_str = f"${value_f:,.0f}"
                elif dtype == 'per_share':
                    value_str = f"${value_f:.2f}"
                else:
                    value_str = f"{value_f:,.2f}"
                
                row_parts.append(f"{ticker:6} {value_str:>15}")
            else:
                row_parts.append(f"{ticker:6} {'N/A':>15}")
        
        print("  " + " | ".join(row_parts))
        
        # Calculate and show growth rates or ratios where relevant
        if metric in ['revenue', 'net_income', 'operating_income']:
            # Show margins
            if metric != 'revenue' and 'revenue' in [m for m in metrics]:
                print("  " + "-" * 110)
                margin_parts = []
                for ticker in tickers:
                    if ticker in values:
                        # Get revenue
                        rev_result = db.connection.execute('''
                            SELECT metric_value
                            FROM normalized_financials
                            WHERE company_ticker = ? AND metric_id = 'revenue' AND fiscal_year = ?
                            LIMIT 1
                        ''', [ticker, year]).fetchone()
                        
                        if rev_result:
                            revenue = float(rev_result[0])
                            value_f = float(values[ticker][0])
                            margin = (value_f / revenue) * 100
                            margin_str = f"{margin:.1f}%"
                            margin_parts.append(f"{'':6} {margin_str:>15}")
                        else:
                            margin_parts.append(f"{'':6} {'N/A':>15}")
                    else:
                        margin_parts.append(f"{'':6} {'N/A':>15}")
                
                if any('N/A' not in p for p in margin_parts):
                    print("  " + " | ".join(margin_parts) + "  (% of Revenue)")
    
    print("\n" + "=" * 120 + "\n")
    db.close()


if __name__ == "__main__":
    # Compare NVIDIA vs competitors
    tech_semis = ['NVDA', 'AMD', 'INTC']  # Would need AMD and Intel data
    
    # For now, compare what we have
    available = ['NVDA', 'AAPL', 'MSFT', 'GOOGL']
    
    print("\nComparing: NVIDIA (semiconductors) vs Big Tech Giants")
    compare_companies(available, year=2024)
