"""
Industry Classifier: Detect company industry from SIC code.

Uses standard SIC (Standard Industrial Classification) codes to classify
companies into industry categories for applying appropriate financial templates.
"""

from typing import Optional

from ..utils.logger import get_logger

logger = get_logger("finloom.business.industry")


# Industry classification based on SIC codes
INDUSTRY_MAPPINGS = {
    "technology": {
        "name": "Technology",
        "sic_ranges": [
            (3570, 3579),  # Computer equipment
            (3670, 3679),  # Electronic components
            (7370, 7379),  # Computer programming, data processing
            (7371, 7379),  # Software
        ],
        "key_metrics": [
            "revenue", "rd_expense", "gross_profit", "operating_income",
            "deferred_revenue", "subscription_revenue"
        ],
        "special_sections": ["segment_reporting", "cloud_revenue", "subscription_metrics"]
    },
    "banking": {
        "name": "Banking & Financial Services",
        "sic_ranges": [
            (6000, 6099),  # Depository institutions
            (6200, 6299),  # Security & commodity brokers
            (6300, 6399),  # Insurance carriers
            (6700, 6799),  # Holding companies
        ],
        "key_metrics": [
            "net_interest_income", "loan_loss_provision", "noninterest_income",
            "tier_1_capital", "deposits", "loans"
        ],
        "special_sections": ["loan_portfolio", "credit_quality", "capital_adequacy"]
    },
    "retail": {
        "name": "Retail & Consumer",
        "sic_ranges": [
            (5200, 5299),  # Building materials dealers
            (5300, 5399),  # General merchandise stores
            (5400, 5499),  # Food stores
            (5600, 5699),  # Apparel stores
            (5700, 5799),  # Home furniture stores
            (5900, 5999),  # Miscellaneous retail
        ],
        "key_metrics": [
            "revenue", "same_store_sales", "inventory_turnover",
            "store_count", "ecommerce_revenue"
        ],
        "special_sections": ["store_operations", "inventory_management"]
    },
    "manufacturing": {
        "name": "Manufacturing",
        "sic_ranges": [
            (2000, 3999),  # Manufacturing (broad)
            (3711, 3711),  # Motor vehicles & passenger car bodies
            (3714, 3714),  # Motor vehicle parts
        ],
        "key_metrics": [
            "revenue", "cost_of_revenue", "inventory", "capex",
            "production_capacity", "inventory_turnover"
        ],
        "special_sections": ["production_metrics", "supply_chain"]
    },
    "energy": {
        "name": "Energy & Utilities",
        "sic_ranges": [
            (1300, 1399),  # Oil & gas extraction
            (2900, 2999),  # Petroleum refining
            (4900, 4999),  # Electric, gas, sanitary services
        ],
        "key_metrics": [
            "revenue", "production_volume", "reserves",
            "capex", "operating_expenses"
        ],
        "special_sections": ["reserves", "production", "environmental"]
    },
    "healthcare": {
        "name": "Healthcare & Pharmaceuticals",
        "sic_ranges": [
            (2833, 2836),  # Medicinal chemicals & pharmaceutical products
            (3841, 3841),  # Surgical & medical instruments
            (8000, 8099),  # Health services
        ],
        "key_metrics": [
            "revenue", "rd_expense", "clinical_trials",
            "drug_pipeline", "patent_expiration"
        ],
        "special_sections": ["pipeline", "regulatory", "clinical_trials"]
    },
    "telecommunications": {
        "name": "Telecommunications",
        "sic_ranges": [
            (4810, 4899),  # Communications
        ],
        "key_metrics": [
            "revenue", "subscribers", "arpu", "churn_rate",
            "network_capex"
        ],
        "special_sections": ["subscriber_metrics", "network_infrastructure"]
    },
}


class IndustryClassifier:
    """
    Classifies companies into industry categories based on SIC code.
    
    Example:
        classifier = IndustryClassifier()
        industry = classifier.classify("7370")  # Returns "technology"
        template = classifier.get_template("technology")
    """
    
    def __init__(self):
        """Initialize the classifier with industry mappings."""
        self.mappings = INDUSTRY_MAPPINGS
        logger.info(f"Initialized industry classifier with {len(self.mappings)} industries")
    
    def classify(self, sic_code: str) -> str:
        """
        Classify a company based on SIC code.
        
        Args:
            sic_code: Standard Industrial Classification code
        
        Returns:
            Industry identifier (e.g., 'technology', 'banking', 'general')
        """
        if not sic_code:
            return "general"
        
        try:
            sic_int = int(sic_code)
        except (ValueError, TypeError):
            logger.warning(f"Invalid SIC code: {sic_code}")
            return "general"
        
        # Check each industry
        for industry_id, config in self.mappings.items():
            for start, end in config["sic_ranges"]:
                if start <= sic_int <= end:
                    logger.debug(f"Classified SIC {sic_code} as {industry_id}")
                    return industry_id
        
        logger.debug(f"SIC {sic_code} not matched, using general")
        return "general"
    
    def get_template_config(self, industry: str) -> dict:
        """
        Get template configuration for an industry.
        
        Args:
            industry: Industry identifier
        
        Returns:
            Dictionary with template configuration
        """
        if industry in self.mappings:
            return self.mappings[industry]
        
        # Return default/general template
        return {
            "name": "General",
            "key_metrics": [
                "revenue", "net_income", "total_assets", "stockholders_equity",
                "eps_diluted", "operating_cash_flow"
            ],
            "special_sections": []
        }
    
    def get_industry_name(self, industry: str) -> str:
        """Get display name for industry."""
        if industry in self.mappings:
            return self.mappings[industry]["name"]
        return "General"
    
    def get_all_industries(self) -> list[dict]:
        """Get list of all supported industries."""
        return [
            {
                "id": industry_id,
                "name": config["name"],
                "sic_ranges": config["sic_ranges"]
            }
            for industry_id, config in self.mappings.items()
        ]
