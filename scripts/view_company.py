#!/usr/bin/env python3
"""
CLI to display structured financial analysis data for SEC-filing companies.

Usage:
    python scripts/view_company.py AAPL          # Full structured report
    python scripts/view_company.py MSFT          # Show all data for Microsoft
    python scripts/view_company.py --list        # List all available companies
    python scripts/view_company.py AAPL --raw    # Show raw XBRL data
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import duckdb
from tabulate import tabulate


# =============================================================================
# TEMPLATE CONFIGURATION
# =============================================================================

# Revenue concepts in priority order (first available is used)
REVENUE_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",  # ASC 606 (2017+)
    "SalesRevenueNet",  # Legacy (pre-2017)
    "Revenues",  # Fallback
]

# Cost of revenue concepts
COST_CONCEPTS = [
    "CostOfGoodsAndServicesSold",
    "CostOfRevenue",
]

# Template sections with concept mappings
TEMPLATE_SECTIONS = {
    "key_metrics": {
        "title": "KEY METRICS SUMMARY",
        "show_growth": True,
        "metrics": [
            ("Revenue", REVENUE_CONCEPTS),
            ("Net Income", ["NetIncomeLoss"]),
            ("EPS (Diluted)", ["EarningsPerShareDiluted"]),
        ]
    },
    "profitability": {
        "title": "PROFITABILITY RATIOS",
        "calculated": True,
        "metrics": [
            "gross_margin",
            "operating_margin", 
            "net_margin",
            "roe",
            "roa",
        ]
    },
    "income_statement": {
        "title": "INCOME STATEMENT",
        "metrics": [
            ("Revenue", REVENUE_CONCEPTS),
            ("Cost of Revenue", COST_CONCEPTS),
            ("Gross Profit", ["GrossProfit"]),
            ("Operating Expenses", ["OperatingExpenses"]),
            ("  R&D Expense", ["ResearchAndDevelopmentExpense"]),
            ("  SG&A Expense", ["SellingGeneralAndAdministrativeExpense"]),
            ("Operating Income", ["OperatingIncomeLoss"]),
            ("Interest Expense", ["InterestExpense"]),
            ("Pre-Tax Income", ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"]),
            ("Income Tax", ["IncomeTaxExpenseBenefit"]),
            ("Net Income", ["NetIncomeLoss"]),
        ]
    },
    "balance_sheet": {
        "title": "BALANCE SHEET",
        "metrics": [
            ("Total Assets", ["Assets"]),
            ("  Current Assets", ["AssetsCurrent"]),
            ("  Cash & Equivalents", ["CashAndCashEquivalentsAtCarryingValue"]),
            ("  Inventory", ["InventoryNet"]),
            ("  Receivables", ["AccountsReceivableNetCurrent"]),
            ("  Non-Current Assets", ["AssetsNoncurrent"]),
            ("  Property & Equipment", ["PropertyPlantAndEquipmentNet"]),
            ("Total Liabilities", ["Liabilities"]),
            ("  Current Liabilities", ["LiabilitiesCurrent"]),
            ("  Accounts Payable", ["AccountsPayableCurrent"]),
            ("  Non-Current Liabilities", ["LiabilitiesNoncurrent"]),
            ("  Long-Term Debt", ["LongTermDebtNoncurrent", "LongTermDebt"]),
            ("Stockholders' Equity", ["StockholdersEquity"]),
            ("  Retained Earnings", ["RetainedEarningsAccumulatedDeficit"]),
        ]
    },
    "cash_flow": {
        "title": "CASH FLOW STATEMENT",
        "metrics": [
            ("Operating Cash Flow", ["NetCashProvidedByUsedInOperatingActivities"]),
            ("Investing Cash Flow", ["NetCashProvidedByUsedInInvestingActivities"]),
            ("Financing Cash Flow", ["NetCashProvidedByUsedInFinancingActivities"]),
            ("CapEx", ["PaymentsToAcquirePropertyPlantAndEquipment"]),
            ("Depreciation", ["DepreciationDepletionAndAmortization"]),
        ],
        "calculated_rows": [
            ("Free Cash Flow", "free_cash_flow"),
        ]
    },
    "liquidity": {
        "title": "LIQUIDITY & LEVERAGE",
        "calculated": True,
        "metrics": [
            "current_ratio",
            "debt_to_equity",
            "debt_to_assets",
        ]
    },
    "per_share": {
        "title": "PER SHARE DATA",
        "metrics": [
            ("EPS (Basic)", ["EarningsPerShareBasic"]),
            ("EPS (Diluted)", ["EarningsPerShareDiluted"]),
            ("Dividends Per Share", ["CommonStockDividendsPerShareDeclared"]),
            ("Shares Outstanding", ["CommonStockSharesOutstanding"]),
            ("Share Buybacks", ["PaymentsForRepurchaseOfCommonStock"]),
        ]
    },
}

# Friendly names for calculated ratios
RATIO_NAMES = {
    "gross_margin": "Gross Margin",
    "operating_margin": "Operating Margin",
    "net_margin": "Net Profit Margin",
    "roe": "Return on Equity (ROE)",
    "roa": "Return on Assets (ROA)",
    "current_ratio": "Current Ratio",
    "debt_to_equity": "Debt-to-Equity",
    "debt_to_assets": "Debt-to-Assets",
    "free_cash_flow": "Free Cash Flow",
}


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_database_path() -> Path:
    """Get path to the DuckDB database."""
    script_dir = Path(__file__).parent.parent
    db_path = script_dir / "data" / "database" / "finloom.duckdb"
    
    if not db_path.exists():
        db_path = Path("data/database/finloom.duckdb")
    
    return db_path


def safe_divide(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """Safely divide two numbers, returning None if invalid."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def format_currency(value: Optional[float]) -> str:
    """Format a currency value with abbreviations."""
    if value is None:
        return "-"
    
    try:
        val = float(value)
    except (ValueError, TypeError):
        return "-"
    
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    
    if abs_val >= 1e12:
        return f"{sign}${abs_val/1e12:.1f}T"
    elif abs_val >= 1e9:
        return f"{sign}${abs_val/1e9:.1f}B"
    elif abs_val >= 1e6:
        return f"{sign}${abs_val/1e6:.1f}M"
    elif abs_val >= 1e3:
        return f"{sign}${abs_val/1e3:.1f}K"
    else:
        return f"{sign}${abs_val:,.0f}"


def format_ratio(value: Optional[float], is_percentage: bool = False) -> str:
    """Format a ratio value."""
    if value is None:
        return "-"
    
    try:
        val = float(value)
    except (ValueError, TypeError):
        return "-"
    
    if is_percentage:
        return f"{val:.1f}%"
    else:
        return f"{val:.2f}"


def format_shares(value: Optional[float]) -> str:
    """Format share counts."""
    if value is None:
        return "-"
    
    try:
        val = float(value)
    except (ValueError, TypeError):
        return "-"
    
    abs_val = abs(val)
    if abs_val >= 1e9:
        return f"{val/1e9:.2f}B"
    elif abs_val >= 1e6:
        return f"{val/1e6:.1f}M"
    else:
        return f"{val:,.0f}"


def format_growth(current: Optional[float], previous: Optional[float]) -> str:
    """Calculate and format YoY growth percentage."""
    if current is None or previous is None or previous == 0:
        return "-"
    
    try:
        growth = ((float(current) - float(previous)) / abs(float(previous))) * 100
        sign = "+" if growth >= 0 else ""
        return f"{sign}{growth:.1f}%"
    except (ValueError, TypeError):
        return "-"


# =============================================================================
# DATA RETRIEVAL
# =============================================================================

def list_companies(db: duckdb.DuckDBPyConnection) -> None:
    """List all available companies."""
    result = db.execute("""
        SELECT 
            c.ticker,
            c.company_name,
            COUNT(DISTINCT fl.accession_number) as filings,
            COUNT(f.id) as facts
        FROM companies c
        LEFT JOIN filings fl ON c.cik = fl.cik
        LEFT JOIN facts f ON fl.accession_number = f.accession_number
        GROUP BY c.ticker, c.company_name
        ORDER BY c.ticker
    """).fetchall()
    
    print("\n=== Available Companies ===\n")
    headers = ["Ticker", "Company Name", "Filings", "Facts"]
    print(tabulate(result, headers=headers, tablefmt="simple"))
    print()


def get_company_info(db: duckdb.DuckDBPyConnection, ticker: str) -> Optional[Tuple]:
    """Get company information."""
    result = db.execute("""
        SELECT cik, company_name, ticker
        FROM companies
        WHERE UPPER(ticker) = UPPER(?)
    """, [ticker]).fetchone()
    return result


def get_raw_facts(db: duckdb.DuckDBPyConnection, cik: str) -> List[Tuple]:
    """Get all facts for a company."""
    return db.execute("""
        SELECT 
            REPLACE(f.concept_name, 'us-gaap:', '') as concept,
            EXTRACT(YEAR FROM f.period_end) as year,
            f.value,
            f.unit,
            f.period_type
        FROM facts f
        JOIN filings fl ON f.accession_number = fl.accession_number
        WHERE fl.cik = ?
          AND f.value IS NOT NULL
          AND f.period_end IS NOT NULL
        ORDER BY concept, year
    """, [cik]).fetchall()


def get_filing_stats(db: duckdb.DuckDBPyConnection, cik: str) -> Tuple[int, str, str]:
    """Get filing statistics for a company."""
    result = db.execute("""
        SELECT 
            COUNT(DISTINCT accession_number) as cnt,
            MIN(EXTRACT(YEAR FROM filing_date)) as min_year,
            MAX(EXTRACT(YEAR FROM filing_date)) as max_year
        FROM filings
        WHERE cik = ?
    """, [cik]).fetchone()
    return result


# =============================================================================
# DATA TRANSFORMATION
# =============================================================================

def build_data_dict(results: List[Tuple]) -> Tuple[Dict, List[int], Dict]:
    """
    Build a dictionary of concept -> year -> value from raw results.
    Returns (data_dict, sorted_years, units_dict)
    """
    data = {}
    units = {}
    years = set()
    
    for concept, year, value, unit, period_type in results:
        year = int(year) if year else None
        if year is None:
            continue
        
        years.add(year)
        
        if concept not in data:
            data[concept] = {}
            units[concept] = unit
        
        # Keep the largest absolute value for each year (handles duplicates)
        if year not in data[concept]:
            data[concept][year] = value
        else:
            current = data[concept][year]
            try:
                if abs(float(value)) > abs(float(current)):
                    data[concept][year] = value
            except (ValueError, TypeError):
                pass
    
    return data, sorted(years), units


def get_value(data: Dict, concepts: List[str], year: int) -> Optional[float]:
    """Get value for a concept (tries multiple concept names in order)."""
    for concept in concepts:
        if concept in data and year in data[concept]:
            try:
                return float(data[concept][year])
            except (ValueError, TypeError):
                continue
    return None


def get_revenue(data: Dict, year: int) -> Optional[float]:
    """Get consolidated revenue value."""
    return get_value(data, REVENUE_CONCEPTS, year)


def get_cost_of_revenue(data: Dict, year: int) -> Optional[float]:
    """Get consolidated cost of revenue."""
    return get_value(data, COST_CONCEPTS, year)


# =============================================================================
# RATIO CALCULATIONS
# =============================================================================

def calculate_ratios(data: Dict, year: int) -> Dict[str, Optional[float]]:
    """Calculate all financial ratios for a given year."""
    revenue = get_revenue(data, year)
    net_income = get_value(data, ["NetIncomeLoss"], year)
    gross_profit = get_value(data, ["GrossProfit"], year)
    operating_income = get_value(data, ["OperatingIncomeLoss"], year)
    assets = get_value(data, ["Assets"], year)
    equity = get_value(data, ["StockholdersEquity"], year)
    current_assets = get_value(data, ["AssetsCurrent"], year)
    current_liabilities = get_value(data, ["LiabilitiesCurrent"], year)
    long_term_debt = get_value(data, ["LongTermDebtNoncurrent", "LongTermDebt"], year)
    operating_cf = get_value(data, ["NetCashProvidedByUsedInOperatingActivities"], year)
    capex = get_value(data, ["PaymentsToAcquirePropertyPlantAndEquipment"], year)
    
    # Calculate ratios
    gross_margin = safe_divide(gross_profit, revenue)
    operating_margin = safe_divide(operating_income, revenue)
    net_margin = safe_divide(net_income, revenue)
    roe = safe_divide(net_income, equity)
    roa = safe_divide(net_income, assets)
    current_ratio = safe_divide(current_assets, current_liabilities)
    debt_to_equity = safe_divide(long_term_debt, equity)
    debt_to_assets = safe_divide(long_term_debt, assets)
    
    # Free cash flow
    free_cash_flow = None
    if operating_cf is not None:
        capex_val = capex if capex is not None else 0
        free_cash_flow = operating_cf - capex_val
    
    return {
        "gross_margin": gross_margin * 100 if gross_margin else None,
        "operating_margin": operating_margin * 100 if operating_margin else None,
        "net_margin": net_margin * 100 if net_margin else None,
        "roe": roe * 100 if roe else None,
        "roa": roa * 100 if roa else None,
        "current_ratio": current_ratio,
        "debt_to_equity": debt_to_equity,
        "debt_to_assets": debt_to_assets,
        "free_cash_flow": free_cash_flow,
    }


# =============================================================================
# OUTPUT FORMATTING
# =============================================================================

def print_header(company_name: str, ticker: str, filing_count: int, 
                 min_year: int, max_year: int, fact_count: int) -> None:
    """Print the report header."""
    width = 80
    print()
    print("=" * width)
    title = f"{company_name.upper()} ({ticker})"
    print(f"{title:^{width}}")
    print(f"{'10-K Financial Analysis Report':^{width}}")
    print("=" * width)
    print(f"Filings: {filing_count} | Data Range: {min_year}-{max_year} | Facts: {fact_count:,}")
    print()


def print_section_box(title: str, rows: List[List[str]], col_widths: List[int]) -> None:
    """Print a section with box-drawing characters."""
    if not rows:
        return
    
    # Calculate total width
    total_width = sum(col_widths) + len(col_widths) + 1  # +1 for each separator and edges
    
    # Top border
    print(f"{'─' * total_width}")
    print(f" {title}")
    print(f"{'─' * total_width}")
    
    # Rows
    for row in rows:
        formatted = []
        for i, cell in enumerate(row):
            w = col_widths[i]
            if i == 0:
                formatted.append(f" {cell:<{w}}")
            else:
                formatted.append(f"{cell:>{w}} ")
        print("│".join(formatted))
    
    print()


def get_smart_format(value: Optional[float], concept_name: str, units: Dict) -> str:
    """Determine the right format based on concept type."""
    if value is None:
        return "-"
    
    # Per-share data
    if "PerShare" in concept_name or "EarningsPerShare" in concept_name:
        try:
            return f"${float(value):.2f}"
        except:
            return "-"
    
    # Share counts
    if "Shares" in concept_name:
        return format_shares(value)
    
    # Default to currency
    return format_currency(value)


# =============================================================================
# MAIN DISPLAY FUNCTIONS
# =============================================================================

def display_structured_report(db: duckdb.DuckDBPyConnection, ticker: str) -> None:
    """Display the full structured financial report."""
    # Get company info
    company_info = get_company_info(db, ticker)
    if not company_info:
        print(f"\nError: Company with ticker '{ticker}' not found.\n")
        list_companies(db)
        return
    
    cik, company_name, db_ticker = company_info
    
    # Get raw facts
    results = get_raw_facts(db, cik)
    if not results:
        print(f"\nNo facts found for {company_name} ({db_ticker})")
        return
    
    # Build data dictionary
    data, years, units = build_data_dict(results)
    
    # Get filing stats
    filing_count, min_year, max_year = get_filing_stats(db, cik)
    
    # Calculate ratios for all years
    ratios = {year: calculate_ratios(data, year) for year in years}
    
    # Print header
    print_header(company_name, db_ticker, filing_count, 
                 int(min_year) if min_year else years[0], 
                 int(max_year) if max_year else years[-1],
                 len(results))
    
    # Column widths
    label_width = 26
    value_width = 10
    col_widths = [label_width] + [value_width] * len(years)
    
    # Year headers
    year_headers = ["Metric"] + [str(y) for y in years]
    
    # Process each section
    for section_key, section in TEMPLATE_SECTIONS.items():
        title = section["title"]
        rows = []
        
        # Header row with years
        rows.append(year_headers)
        
        if section.get("calculated"):
            # Calculated ratios section
            for metric_key in section["metrics"]:
                row = [RATIO_NAMES.get(metric_key, metric_key)]
                for year in years:
                    val = ratios[year].get(metric_key)
                    if metric_key in ["current_ratio", "debt_to_equity", "debt_to_assets"]:
                        row.append(format_ratio(val, is_percentage=False))
                    else:
                        row.append(format_ratio(val, is_percentage=True))
                rows.append(row)
        else:
            # Regular metrics section
            for metric_name, concepts in section["metrics"]:
                row = [metric_name]
                values = []
                for year in years:
                    val = get_value(data, concepts, year)
                    values.append(val)
                    
                    # Smart formatting based on metric type
                    if "EPS" in metric_name or "Per Share" in metric_name:
                        if val is not None:
                            row.append(f"${val:.2f}")
                        else:
                            row.append("-")
                    elif "Shares" in metric_name:
                        row.append(format_shares(val))
                    else:
                        row.append(format_currency(val))
                
                rows.append(row)
                
                # Add growth row for key metrics
                if section.get("show_growth") and metric_name in ["Revenue", "Net Income"]:
                    growth_row = [f"  YoY Growth"]
                    for i, year in enumerate(years):
                        if i == 0:
                            growth_row.append("-")
                        else:
                            growth_row.append(format_growth(values[i], values[i-1]))
                    rows.append(growth_row)
            
            # Add calculated rows if any
            if "calculated_rows" in section:
                for calc_name, calc_key in section["calculated_rows"]:
                    row = [calc_name]
                    for year in years:
                        val = ratios[year].get(calc_key)
                        row.append(format_currency(val))
                    rows.append(row)
        
        print_section_box(title, rows, col_widths)


def display_raw_data(db: duckdb.DuckDBPyConnection, ticker: str, 
                     concept_filter: Optional[str] = None) -> None:
    """Display raw XBRL data (original format)."""
    company_info = get_company_info(db, ticker)
    if not company_info:
        print(f"\nError: Company with ticker '{ticker}' not found.\n")
        list_companies(db)
        return
    
    cik, company_name, db_ticker = company_info
    
    # Build query with optional filter
    concept_clause = ""
    params = [cik]
    if concept_filter:
        concept_clause = "AND LOWER(f.concept_name) LIKE LOWER(?)"
        params.append(f"%{concept_filter}%")
    
    results = db.execute(f"""
        SELECT 
            REPLACE(f.concept_name, 'us-gaap:', '') as concept,
            EXTRACT(YEAR FROM f.period_end) as year,
            f.value,
            f.unit,
            f.period_type
        FROM facts f
        JOIN filings fl ON f.accession_number = fl.accession_number
        WHERE fl.cik = ?
          AND f.value IS NOT NULL
          AND f.period_end IS NOT NULL
          {concept_clause}
        ORDER BY concept, year
    """, params).fetchall()
    
    if not results:
        print(f"\nNo facts found for {company_name} ({db_ticker})")
        return
    
    # Build data dict and pivot
    data, years, units = build_data_dict(results)
    
    # Build rows
    headers = ["Concept"] + [str(y) for y in years]
    rows = []
    
    for concept in sorted(data.keys()):
        row = [concept]
        unit = units.get(concept, "USD")
        for year in years:
            val = data[concept].get(year)
            if val is None:
                row.append("-")
            elif unit == "USD":
                row.append(format_currency(float(val)))
            elif unit == "shares":
                row.append(format_shares(float(val)))
            else:
                try:
                    row.append(f"{float(val):.3f}")
                except:
                    row.append(str(val))
        rows.append(row)
    
    # Print header
    filing_count, _, _ = get_filing_stats(db, cik)
    print()
    print("=" * 70)
    print(f"  {company_name} ({db_ticker}) - Raw XBRL Data")
    print(f"  Filings: {filing_count} | Concepts: {len(data)} | Facts: {len(results):,}")
    if concept_filter:
        print(f"  Filter: '{concept_filter}'")
    print("=" * 70)
    print()
    
    print(tabulate(rows, headers=headers, tablefmt="simple", numalign="right"))
    print()


# =============================================================================
# EXTENDED DATA DISPLAY FUNCTIONS
# =============================================================================

def display_all_concepts(db: duckdb.DuckDBPyConnection, ticker: str) -> None:
    """Display all concepts for a company grouped by section."""
    company_info = get_company_info(db, ticker)
    if not company_info:
        print(f"\nError: Company with ticker '{ticker}' not found.\n")
        list_companies(db)
        return
    
    cik, company_name, db_ticker = company_info
    
    # Get all facts with section info
    results = db.execute("""
        SELECT 
            COALESCE(f.section, 'Other') as section,
            REPLACE(f.concept_name, 'us-gaap:', '') as concept,
            f.label,
            EXTRACT(YEAR FROM f.period_end) as year,
            f.value,
            f.unit,
            f.depth
        FROM facts f
        JOIN filings fl ON f.accession_number = fl.accession_number
        WHERE fl.cik = ?
          AND f.value IS NOT NULL
          AND f.period_end IS NOT NULL
        ORDER BY section, concept, year
    """, [cik]).fetchall()
    
    if not results:
        print(f"\nNo facts found for {company_name} ({db_ticker})")
        return
    
    # Group by section
    sections = {}
    for section, concept, label, year, value, unit, depth in results:
        if section not in sections:
            sections[section] = {}
        if concept not in sections[section]:
            sections[section][concept] = {'label': label, 'depth': depth, 'values': {}, 'unit': unit}
        if year:
            sections[section][concept]['values'][int(year)] = value
    
    # Get all years
    all_years = sorted(set(int(r[3]) for r in results if r[3]))
    
    # Print header
    filing_count, _, _ = get_filing_stats(db, cik)
    print()
    print("=" * 80)
    print(f"  {company_name} ({db_ticker}) - All XBRL Concepts")
    print(f"  Filings: {filing_count} | Sections: {len(sections)} | Concepts: {sum(len(s) for s in sections.values())}")
    print("=" * 80)
    print()
    
    # Display each section
    for section in sorted(sections.keys()):
        concepts = sections[section]
        print(f"\n{'─' * 80}")
        print(f" {section} ({len(concepts)} concepts)")
        print(f"{'─' * 80}")
        
        # Build rows
        rows = []
        for concept in sorted(concepts.keys()):
            data = concepts[concept]
            label = data['label'] or concept
            unit = data['unit']
            
            row = [label[:40]]
            for year in all_years[-5:]:  # Last 5 years
                val = data['values'].get(year)
                if val is None:
                    row.append("-")
                elif unit == "USD":
                    row.append(format_currency(float(val)))
                elif unit == "shares":
                    row.append(format_shares(float(val)))
                else:
                    try:
                        row.append(f"{float(val):.2f}")
                    except:
                        row.append(str(val)[:10])
            rows.append(row)
        
        headers = ["Concept"] + [str(y) for y in all_years[-5:]]
        print(tabulate(rows[:20], headers=headers, tablefmt="simple", numalign="right"))
        
        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more concepts")
    
    print()


def display_section(db: duckdb.DuckDBPyConnection, ticker: str, section: str) -> None:
    """Display all concepts in a specific section."""
    company_info = get_company_info(db, ticker)
    if not company_info:
        print(f"\nError: Company with ticker '{ticker}' not found.\n")
        list_companies(db)
        return
    
    cik, company_name, db_ticker = company_info
    
    # Get facts for section
    results = db.execute("""
        SELECT 
            REPLACE(f.concept_name, 'us-gaap:', '') as concept,
            f.label,
            EXTRACT(YEAR FROM f.period_end) as year,
            f.value,
            f.unit,
            f.depth,
            f.parent_concept
        FROM facts f
        JOIN filings fl ON f.accession_number = fl.accession_number
        WHERE fl.cik = ?
          AND LOWER(COALESCE(f.section, '')) LIKE LOWER(?)
          AND f.value IS NOT NULL
          AND f.period_end IS NOT NULL
        ORDER BY f.depth, concept, year
    """, [cik, f"%{section}%"]).fetchall()
    
    if not results:
        # List available sections
        sections = db.execute("""
            SELECT DISTINCT COALESCE(f.section, 'Other') as section, COUNT(*) as cnt
            FROM facts f
            JOIN filings fl ON f.accession_number = fl.accession_number
            WHERE fl.cik = ?
            GROUP BY section
            ORDER BY cnt DESC
        """, [cik]).fetchall()
        
        print(f"\nNo facts found for section '{section}'")
        print("\nAvailable sections:")
        for sec, cnt in sections:
            print(f"  - {sec} ({cnt} facts)")
        return
    
    # Build data dict
    data = {}
    for concept, label, year, value, unit, depth, parent in results:
        if concept not in data:
            data[concept] = {'label': label, 'depth': depth or 0, 'values': {}, 'unit': unit}
        if year:
            data[concept]['values'][int(year)] = value
    
    all_years = sorted(set(int(r[2]) for r in results if r[2]))
    
    # Print header
    print()
    print("=" * 80)
    print(f"  {company_name} ({db_ticker}) - {section}")
    print(f"  Concepts: {len(data)}")
    print("=" * 80)
    print()
    
    # Build rows with tree-like indentation
    rows = []
    for concept in sorted(data.keys(), key=lambda c: (data[c]['depth'], c)):
        info = data[concept]
        indent = "  " * info['depth']
        label = info['label'] or concept
        display_name = f"{indent}{label}"[:45]
        
        row = [display_name]
        for year in all_years[-5:]:
            val = info['values'].get(year)
            if val is None:
                row.append("-")
            elif info['unit'] == "USD":
                row.append(format_currency(float(val)))
            elif info['unit'] == "shares":
                row.append(format_shares(float(val)))
            else:
                try:
                    row.append(f"{float(val):.2f}")
                except:
                    row.append("-")
        rows.append(row)
    
    headers = ["Concept"] + [str(y) for y in all_years[-5:]]
    print(tabulate(rows, headers=headers, tablefmt="simple", numalign="right"))
    print()


def search_concepts(db: duckdb.DuckDBPyConnection, ticker: str, search_term: str) -> None:
    """Search for concepts by name or label."""
    company_info = get_company_info(db, ticker)
    if not company_info:
        print(f"\nError: Company with ticker '{ticker}' not found.\n")
        list_companies(db)
        return
    
    cik, company_name, db_ticker = company_info
    
    # Search in concept name and label
    results = db.execute("""
        SELECT 
            REPLACE(f.concept_name, 'us-gaap:', '') as concept,
            f.label,
            f.section,
            EXTRACT(YEAR FROM f.period_end) as year,
            f.value,
            f.unit
        FROM facts f
        JOIN filings fl ON f.accession_number = fl.accession_number
        WHERE fl.cik = ?
          AND (
            LOWER(f.concept_name) LIKE LOWER(?)
            OR LOWER(COALESCE(f.label, '')) LIKE LOWER(?)
          )
          AND f.value IS NOT NULL
          AND f.period_end IS NOT NULL
        ORDER BY concept, year
    """, [cik, f"%{search_term}%", f"%{search_term}%"]).fetchall()
    
    if not results:
        print(f"\nNo concepts found matching '{search_term}'")
        return
    
    # Build data dict
    data = {}
    for concept, label, section, year, value, unit in results:
        if concept not in data:
            data[concept] = {'label': label, 'section': section, 'values': {}, 'unit': unit}
        if year:
            data[concept]['values'][int(year)] = value
    
    all_years = sorted(set(int(r[3]) for r in results if r[3]))
    
    # Print header
    print()
    print("=" * 80)
    print(f"  {company_name} ({db_ticker}) - Search: '{search_term}'")
    print(f"  Found: {len(data)} concepts")
    print("=" * 80)
    print()
    
    # Build rows
    rows = []
    for concept in sorted(data.keys()):
        info = data[concept]
        label = info['label'] or concept
        
        row = [label[:40], info['section'] or '-']
        for year in all_years[-5:]:
            val = info['values'].get(year)
            if val is None:
                row.append("-")
            elif info['unit'] == "USD":
                row.append(format_currency(float(val)))
            elif info['unit'] == "shares":
                row.append(format_shares(float(val)))
            else:
                try:
                    row.append(f"{float(val):.2f}")
                except:
                    row.append("-")
        rows.append(row)
    
    headers = ["Concept", "Section"] + [str(y) for y in all_years[-5:]]
    print(tabulate(rows, headers=headers, tablefmt="simple", numalign="right"))
    print()


def list_sections(db: duckdb.DuckDBPyConnection, ticker: str) -> None:
    """List all available sections for a company."""
    company_info = get_company_info(db, ticker)
    if not company_info:
        print(f"\nError: Company with ticker '{ticker}' not found.\n")
        list_companies(db)
        return
    
    cik, company_name, db_ticker = company_info
    
    sections = db.execute("""
        SELECT 
            COALESCE(f.section, 'Other') as section,
            COUNT(DISTINCT f.concept_name) as concepts,
            COUNT(*) as facts
        FROM facts f
        JOIN filings fl ON f.accession_number = fl.accession_number
        WHERE fl.cik = ?
        GROUP BY section
        ORDER BY facts DESC
    """, [cik]).fetchall()
    
    print()
    print(f"=== Available Sections for {company_name} ({db_ticker}) ===\n")
    headers = ["Section", "Concepts", "Facts"]
    print(tabulate(sections, headers=headers, tablefmt="simple"))
    print()


def display_tree_view(
    db: duckdb.DuckDBPyConnection,
    ticker: str,
    section: str = None,
    year: int = None
) -> None:
    """Display hierarchical tree view of financial data."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.display.tree_builder import TreeBuilder
    
    company_info = get_company_info(db, ticker)
    if not company_info:
        print(f"\nError: Company with ticker '{ticker}' not found.\n")
        list_companies(db)
        return
    
    cik, company_name, db_ticker = company_info
    
    # Build query - deduplicate by concept (get latest value per concept)
    sql = """
        WITH ranked_facts AS (
            SELECT 
                f.concept_name,
                f.label,
                f.value,
                f.unit,
                f.parent_concept,
                f.depth,
                f.section,
                EXTRACT(YEAR FROM f.period_end) as year,
                ROW_NUMBER() OVER (
                    PARTITION BY f.concept_name 
                    ORDER BY f.period_end DESC
                ) as rn
            FROM facts f
            JOIN filings fl ON f.accession_number = fl.accession_number
            WHERE fl.cik = ?
              AND f.value IS NOT NULL
              AND f.period_end IS NOT NULL
        )
        SELECT concept_name, label, value, unit, parent_concept, depth, section, year
        FROM ranked_facts
        WHERE rn = 1
    """
    params = [cik]
    
    # Filter by section if provided
    if section:
        sql = sql.replace("WHERE fl.cik = ?", "WHERE fl.cik = ? AND LOWER(COALESCE(f.section, '')) LIKE LOWER(?)")
        params.append(f"%{section}%")
    
    # Filter by year if provided
    if year:
        sql = sql.replace("WHERE rn = 1", f"WHERE rn = 1 AND year = {year}")
    
    sql += " ORDER BY section, depth, concept_name"
    
    results = db.execute(sql, params).fetchall()
    
    if not results:
        print(f"\nNo data found for the specified criteria")
        return
    
    # Convert to fact dictionaries
    facts = []
    for concept_name, label, value, unit, parent_concept, depth, sec, yr in results:
        facts.append({
            'concept_name': concept_name,
            'label': label or concept_name,
            'value': value,
            'unit': unit,
            'parent_concept': parent_concept,
            'depth': depth or 0,
            'section': sec,
        })
    
    # Print header
    print()
    print("=" * 80)
    print(f"  {company_name} ({db_ticker})")
    if section:
        print(f"  Section: {section}")
    print(f"  Fiscal Year: {year}")
    print("=" * 80)
    print()
    
    # Build proper tree with TreeBuilder
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.display.tree_builder import TreeBuilder
    
    builder = TreeBuilder()
    roots = builder.build_tree(facts)
    
    if roots:
        lines = builder.render_tree(roots)
        for line in lines:
            print(line)
    else:
        print("No hierarchical data available")
    
    print()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Display structured financial analysis for SEC-filing companies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python view_company.py AAPL              Full structured financial report
  python view_company.py MSFT              Show all data for Microsoft
  python view_company.py --list            List all available companies
  python view_company.py AAPL --raw        Show raw XBRL data
  python view_company.py AAPL --raw -c Revenue   Filter raw data by concept
  python view_company.py AAPL --section IncomeStatement   Show section data
  python view_company.py AAPL --tree --section FinancialInstruments   Hierarchical tree view
  python view_company.py AAPL --search securities   Search for concepts
  python view_company.py AAPL --sections   List available sections
  python view_company.py AAPL --all        Show all concepts grouped by section
        """
    )
    
    parser.add_argument(
        "ticker",
        nargs="?",
        help="Company ticker symbol (e.g., AAPL, MSFT, GOOGL)"
    )
    
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List all available companies"
    )
    
    parser.add_argument(
        "--raw", "-r",
        action="store_true",
        help="Show raw XBRL data instead of structured report"
    )
    
    parser.add_argument(
        "--concept", "-c",
        help="Filter raw data by concept name (partial match)"
    )
    
    parser.add_argument(
        "--section",
        help="Show all concepts in a specific section (e.g., IncomeStatement, FinancialInstruments)"
    )
    
    parser.add_argument(
        "--sections",
        action="store_true",
        help="List all available sections for a company"
    )
    
    parser.add_argument(
        "--search", "-s",
        help="Search for concepts by name or label"
    )
    
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Show all concepts grouped by section"
    )
    
    parser.add_argument(
        "--tree", "-t",
        action="store_true",
        help="Display data in hierarchical tree format with box-drawing characters"
    )
    
    parser.add_argument(
        "--year", "-y",
        type=int,
        help="Specify fiscal year for tree view (default: latest)"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.list and not args.ticker:
        parser.print_help()
        sys.exit(1)
    
    # Connect to database
    db_path = get_database_path()
    
    if not db_path.exists():
        print(f"\nError: Database not found at {db_path}")
        print("Run the backfill script first to populate the database.")
        sys.exit(1)
    
    try:
        db = duckdb.connect(str(db_path), read_only=True)
        
        if args.list:
            list_companies(db)
        elif args.tree:
            display_tree_view(db, args.ticker, args.section, args.year)
        elif args.sections:
            list_sections(db, args.ticker)
        elif args.section:
            display_section(db, args.ticker, args.section)
        elif args.search:
            search_concepts(db, args.ticker, args.search)
        elif args.all:
            display_all_concepts(db, args.ticker)
        elif args.raw:
            display_raw_data(db, args.ticker, args.concept)
        else:
            display_structured_report(db, args.ticker)
        
        db.close()
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
