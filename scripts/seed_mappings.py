#!/usr/bin/env python3
"""
Seed standardized metrics and concept mappings.

This script populates the normalization layer with:
1. Standardized metric definitions (the "Bloomberg fields")
2. XBRL concept mappings with priority/fallback logic

Based on the 126 concepts common to all companies + top frequently used concepts.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.utils.logger import get_logger

logger = get_logger("finloom.seed")


# Core metric definitions with concept mappings
CORE_METRICS = {
    # ==================== Income Statement ====================
    "revenue": {
        "name": "revenue",
        "label": "Total Revenue",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Total revenue from all sources",
        "concepts": [
            ("us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax", 1, 1.0),
            ("us-gaap:SalesRevenueNet", 2, 0.95),
            ("us-gaap:Revenues", 3, 0.90),
        ]
    },
    "cost_of_revenue": {
        "name": "cost_of_revenue",
        "label": "Cost of Revenue",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Cost of goods and services sold",
        "concepts": [
            ("us-gaap:CostOfGoodsAndServicesSold", 1, 1.0),
            ("us-gaap:CostOfRevenue", 2, 0.95),
        ]
    },
    "gross_profit": {
        "name": "gross_profit",
        "label": "Gross Profit",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Revenue minus cost of revenue",
        "concepts": [
            ("us-gaap:GrossProfit", 1, 1.0),
        ]
    },
    "operating_expenses": {
        "name": "operating_expenses",
        "label": "Operating Expenses",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Total operating expenses",
        "concepts": [
            ("us-gaap:OperatingExpenses", 1, 1.0),
        ]
    },
    "rd_expense": {
        "name": "rd_expense",
        "label": "R&D Expense",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Research and development expenses",
        "concepts": [
            ("us-gaap:ResearchAndDevelopmentExpense", 1, 1.0),
        ]
    },
    "sga_expense": {
        "name": "sga_expense",
        "label": "SG&A Expense",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Selling, general and administrative expenses",
        "concepts": [
            ("us-gaap:SellingGeneralAndAdministrativeExpense", 1, 1.0),
        ]
    },
    "operating_income": {
        "name": "operating_income",
        "label": "Operating Income",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Income from operations",
        "concepts": [
            ("us-gaap:OperatingIncomeLoss", 1, 1.0),
        ]
    },
    "interest_expense": {
        "name": "interest_expense",
        "label": "Interest Expense",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Interest paid on debt",
        "concepts": [
            ("us-gaap:InterestExpense", 1, 1.0),
        ]
    },
    "interest_income": {
        "name": "interest_income",
        "label": "Interest Income",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Interest earned on investments",
        "concepts": [
            ("us-gaap:InterestIncome", 1, 1.0),
        ]
    },
    "pretax_income": {
        "name": "pretax_income",
        "label": "Income Before Tax",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Income before income taxes",
        "concepts": [
            ("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", 1, 1.0),
        ]
    },
    "income_tax": {
        "name": "income_tax",
        "label": "Income Tax Expense",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Income tax expense or benefit",
        "concepts": [
            ("us-gaap:IncomeTaxExpenseBenefit", 1, 1.0),
        ]
    },
    "net_income": {
        "name": "net_income",
        "label": "Net Income",
        "category": "income_statement",
        "data_type": "monetary",
        "description": "Net income attributable to the company",
        "concepts": [
            ("us-gaap:NetIncomeLoss", 1, 1.0),
            ("us-gaap:NetIncomeLossAttributableToParent", 2, 0.95),
            ("us-gaap:ProfitLoss", 3, 0.85),
        ]
    },
    
    # ==================== Balance Sheet ====================
    "total_assets": {
        "name": "total_assets",
        "label": "Total Assets",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Sum of all assets",
        "concepts": [
            ("us-gaap:Assets", 1, 1.0),
        ]
    },
    "current_assets": {
        "name": "current_assets",
        "label": "Current Assets",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Assets expected to be converted to cash within one year",
        "concepts": [
            ("us-gaap:AssetsCurrent", 1, 1.0),
        ]
    },
    "cash_and_equivalents": {
        "name": "cash_and_equivalents",
        "label": "Cash and Cash Equivalents",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Cash and highly liquid investments",
        "concepts": [
            ("us-gaap:CashAndCashEquivalentsAtCarryingValue", 1, 1.0),
        ]
    },
    "short_term_investments": {
        "name": "short_term_investments",
        "label": "Short-term Investments",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Marketable securities and short-term investments",
        "concepts": [
            ("us-gaap:ShortTermInvestments", 1, 1.0),
        ]
    },
    "accounts_receivable": {
        "name": "accounts_receivable",
        "label": "Accounts Receivable",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Amounts owed by customers",
        "concepts": [
            ("us-gaap:AccountsReceivableNetCurrent", 1, 1.0),
        ]
    },
    "inventory": {
        "name": "inventory",
        "label": "Inventory",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Value of goods held for sale",
        "concepts": [
            ("us-gaap:InventoryNet", 1, 1.0),
        ]
    },
    "ppe_net": {
        "name": "ppe_net",
        "label": "Property, Plant & Equipment (Net)",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Net value of fixed assets",
        "concepts": [
            ("us-gaap:PropertyPlantAndEquipmentNet", 1, 1.0),
        ]
    },
    "goodwill": {
        "name": "goodwill",
        "label": "Goodwill",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Premium paid for acquisitions",
        "concepts": [
            ("us-gaap:Goodwill", 1, 1.0),
        ]
    },
    "intangible_assets": {
        "name": "intangible_assets",
        "label": "Intangible Assets",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Value of intangible assets excluding goodwill",
        "concepts": [
            ("us-gaap:IntangibleAssetsNetExcludingGoodwill", 1, 1.0),
        ]
    },
    "total_liabilities": {
        "name": "total_liabilities",
        "label": "Total Liabilities",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Sum of all liabilities",
        "concepts": [
            ("us-gaap:Liabilities", 1, 1.0),
        ]
    },
    "current_liabilities": {
        "name": "current_liabilities",
        "label": "Current Liabilities",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Liabilities due within one year",
        "concepts": [
            ("us-gaap:LiabilitiesCurrent", 1, 1.0),
        ]
    },
    "accounts_payable": {
        "name": "accounts_payable",
        "label": "Accounts Payable",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Amounts owed to suppliers",
        "concepts": [
            ("us-gaap:AccountsPayableCurrent", 1, 1.0),
        ]
    },
    "short_term_debt": {
        "name": "short_term_debt",
        "label": "Short-term Debt",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Debt due within one year",
        "concepts": [
            ("us-gaap:ShortTermBorrowings", 1, 1.0),
        ]
    },
    "long_term_debt": {
        "name": "long_term_debt",
        "label": "Long-term Debt",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Debt due after one year",
        "concepts": [
            ("us-gaap:LongTermDebtNoncurrent", 1, 1.0),
            ("us-gaap:LongTermDebt", 2, 0.95),
        ]
    },
    "stockholders_equity": {
        "name": "stockholders_equity",
        "label": "Stockholders' Equity",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Net worth of the company",
        "concepts": [
            ("us-gaap:StockholdersEquity", 1, 1.0),
            ("us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", 2, 0.95),
        ]
    },
    "retained_earnings": {
        "name": "retained_earnings",
        "label": "Retained Earnings",
        "category": "balance_sheet",
        "data_type": "monetary",
        "description": "Cumulative net income not distributed as dividends",
        "concepts": [
            ("us-gaap:RetainedEarningsAccumulatedDeficit", 1, 1.0),
        ]
    },
    
    # ==================== Cash Flow Statement ====================
    "operating_cash_flow": {
        "name": "operating_cash_flow",
        "label": "Operating Cash Flow",
        "category": "cash_flow",
        "data_type": "monetary",
        "description": "Cash generated from operations",
        "concepts": [
            ("us-gaap:NetCashProvidedByUsedInOperatingActivities", 1, 1.0),
        ]
    },
    "investing_cash_flow": {
        "name": "investing_cash_flow",
        "label": "Investing Cash Flow",
        "category": "cash_flow",
        "data_type": "monetary",
        "description": "Cash used in/provided by investments",
        "concepts": [
            ("us-gaap:NetCashProvidedByUsedInInvestingActivities", 1, 1.0),
        ]
    },
    "financing_cash_flow": {
        "name": "financing_cash_flow",
        "label": "Financing Cash Flow",
        "category": "cash_flow",
        "data_type": "monetary",
        "description": "Cash from/to financing activities",
        "concepts": [
            ("us-gaap:NetCashProvidedByUsedInFinancingActivities", 1, 1.0),
        ]
    },
    "capex": {
        "name": "capex",
        "label": "Capital Expenditures",
        "category": "cash_flow",
        "data_type": "monetary",
        "description": "Cash spent on fixed assets",
        "concepts": [
            ("us-gaap:PaymentsToAcquirePropertyPlantAndEquipment", 1, 1.0),
        ]
    },
    "depreciation": {
        "name": "depreciation",
        "label": "Depreciation & Amortization",
        "category": "cash_flow",
        "data_type": "monetary",
        "description": "Non-cash depreciation expense",
        "concepts": [
            ("us-gaap:DepreciationDepletionAndAmortization", 1, 1.0),
        ]
    },
    "stock_buybacks": {
        "name": "stock_buybacks",
        "label": "Stock Repurchases",
        "category": "cash_flow",
        "data_type": "monetary",
        "description": "Cash paid to repurchase company stock",
        "concepts": [
            ("us-gaap:PaymentsForRepurchaseOfCommonStock", 1, 1.0),
        ]
    },
    "dividends_paid": {
        "name": "dividends_paid",
        "label": "Dividends Paid",
        "category": "cash_flow",
        "data_type": "monetary",
        "description": "Cash dividends paid to shareholders",
        "concepts": [
            ("us-gaap:PaymentsOfDividendsCommonStock", 1, 1.0),
        ]
    },
    
    # ==================== Per Share Data ====================
    "eps_basic": {
        "name": "eps_basic",
        "label": "EPS (Basic)",
        "category": "per_share",
        "data_type": "per_share",
        "description": "Basic earnings per share",
        "concepts": [
            ("us-gaap:EarningsPerShareBasic", 1, 1.0),
        ]
    },
    "eps_diluted": {
        "name": "eps_diluted",
        "label": "EPS (Diluted)",
        "category": "per_share",
        "data_type": "per_share",
        "description": "Diluted earnings per share",
        "concepts": [
            ("us-gaap:EarningsPerShareDiluted", 1, 1.0),
        ]
    },
    "shares_outstanding": {
        "name": "shares_outstanding",
        "label": "Shares Outstanding",
        "category": "per_share",
        "data_type": "shares",
        "description": "Common stock shares outstanding",
        "concepts": [
            ("us-gaap:CommonStockSharesOutstanding", 1, 1.0),
        ]
    },
    "weighted_avg_shares_basic": {
        "name": "weighted_avg_shares_basic",
        "label": "Weighted Average Shares (Basic)",
        "category": "per_share",
        "data_type": "shares",
        "description": "Weighted average shares for EPS calculation",
        "concepts": [
            ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", 1, 1.0),
        ]
    },
    "weighted_avg_shares_diluted": {
        "name": "weighted_avg_shares_diluted",
        "label": "Weighted Average Shares (Diluted)",
        "category": "per_share",
        "data_type": "shares",
        "description": "Weighted average diluted shares for EPS",
        "concepts": [
            ("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", 1, 1.0),
        ]
    },
    "dividends_per_share": {
        "name": "dividends_per_share",
        "label": "Dividends Per Share",
        "category": "per_share",
        "data_type": "per_share",
        "description": "Cash dividends declared per common share",
        "concepts": [
            ("us-gaap:CommonStockDividendsPerShareDeclared", 1, 1.0),
        ]
    },
}


def seed_database(db: Database, clear_existing: bool = False) -> None:
    """
    Seed the database with standardized metrics and concept mappings.
    
    Args:
        db: Database instance
        clear_existing: If True, clear existing mappings first
    """
    if clear_existing:
        logger.info("Clearing existing mappings...")
        db.connection.execute("DELETE FROM concept_mappings")
        db.connection.execute("DELETE FROM standardized_metrics")
    
    logger.info(f"Seeding {len(CORE_METRICS)} standardized metrics...")
    
    for metric_id, config in CORE_METRICS.items():
        # Insert standardized metric
        db.upsert_standardized_metric(
            metric_id=metric_id,
            metric_name=config["name"],
            display_label=config["label"],
            category=config["category"],
            data_type=config.get("data_type"),
            description=config.get("description"),
        )
        
        # Insert concept mappings
        for concept_name, priority, confidence in config["concepts"]:
            db.insert_concept_mapping(
                metric_id=metric_id,
                concept_name=concept_name,
                priority=priority,
                confidence_score=confidence,
            )
    
    logger.info("Seeding complete!")
    
    # Print summary
    metrics_count = db.connection.execute("SELECT COUNT(*) FROM standardized_metrics").fetchone()[0]
    mappings_count = db.connection.execute("SELECT COUNT(*) FROM concept_mappings").fetchone()[0]
    
    logger.info(f"Total standardized metrics: {metrics_count}")
    logger.info(f"Total concept mappings: {mappings_count}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Seed standardized metrics and mappings")
    parser.add_argument("--clear", action="store_true", help="Clear existing data first")
    args = parser.parse_args()
    
    logger.info("Initializing database...")
    db = Database()
    db.initialize_schema()
    
    try:
        seed_database(db, clear_existing=args.clear)
        logger.info("Success!")
    except Exception as e:
        logger.error(f"Failed to seed database: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
