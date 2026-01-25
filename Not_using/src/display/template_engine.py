"""
Template Engine: Generate display templates based on industry and preferences.

Combines base financial metrics with industry-specific overlays to create
tailored views similar to Bloomberg Terminal's industry-specific screens.
"""

from typing import Dict, List, Optional

from ..business.industry_classifier import IndustryClassifier
from ..utils.logger import get_logger

logger = get_logger("finloom.display.template")


# Base template for all companies
BASE_TEMPLATE = {
    "sections": [
        {
            "id": "summary",
            "title": "Company Summary",
            "metrics": ["revenue", "net_income", "total_assets", "stockholders_equity"]
        },
        {
            "id": "income_statement",
            "title": "Income Statement",
            "metrics": [
                "revenue", "cost_of_revenue", "gross_profit",
                "operating_expenses", "operating_income",
                "pretax_income", "net_income"
            ]
        },
        {
            "id": "balance_sheet",
            "title": "Balance Sheet",
            "metrics": [
                "total_assets", "current_assets", "cash_and_equivalents",
                "total_liabilities", "current_liabilities",
                "long_term_debt", "stockholders_equity"
            ]
        },
        {
            "id": "cash_flow",
            "title": "Cash Flow Statement",
            "metrics": [
                "operating_cash_flow", "investing_cash_flow",
                "financing_cash_flow", "capex", "dividends_paid"
            ]
        },
        {
            "id": "per_share",
            "title": "Per Share Data",
            "metrics": [
                "eps_basic", "eps_diluted", "dividends_per_share",
                "shares_outstanding"
            ]
        }
    ]
}


# Industry-specific overlays
INDUSTRY_OVERLAYS = {
    "technology": {
        "additional_sections": [
            {
                "id": "tech_metrics",
                "title": "Technology Metrics",
                "metrics": ["rd_expense", "sga_expense", "deferred_revenue"]
            }
        ],
        "metric_priorities": {
            # Emphasize R&D for tech companies
            "rd_expense": "high",
            "deferred_revenue": "high",
        }
    },
    "banking": {
        "additional_sections": [
            {
                "id": "banking_metrics",
                "title": "Banking Metrics",
                "metrics": ["interest_income", "interest_expense", "accounts_receivable"]
            }
        ],
        "metric_priorities": {
            "interest_income": "high",
            "interest_expense": "high",
        }
    },
    "retail": {
        "additional_sections": [
            {
                "id": "retail_metrics",
                "title": "Retail Metrics",
                "metrics": ["inventory", "accounts_receivable", "accounts_payable"]
            }
        ],
        "metric_priorities": {
            "inventory": "high",
        }
    },
    "manufacturing": {
        "additional_sections": [
            {
                "id": "manufacturing_metrics",
                "title": "Manufacturing Metrics",
                "metrics": ["inventory", "ppe_net", "capex", "depreciation"]
            }
        ],
        "metric_priorities": {
            "inventory": "high",
            "capex": "high",
        }
    }
}


class TemplateEngine:
    """
    Generates display templates based on company industry and user preferences.
    
    Example:
        engine = TemplateEngine()
        template = engine.get_template("technology", view_type="standard")
        # Returns merged template with tech-specific sections
    """
    
    def __init__(self, classifier: Optional[IndustryClassifier] = None):
        """
        Initialize template engine.
        
        Args:
            classifier: Industry classifier instance (creates new one if not provided)
        """
        self.classifier = classifier or IndustryClassifier()
        self.base_template = BASE_TEMPLATE
        self.overlays = INDUSTRY_OVERLAYS
        logger.info("Initialized template engine")
    
    def get_template(
        self,
        industry: str,
        view_type: str = "standard"
    ) -> dict:
        """
        Get merged template for industry.
        
        Args:
            industry: Industry identifier (from classifier)
            view_type: Type of view - 'standard', 'detailed', 'executive_summary'
        
        Returns:
            Template configuration dictionary
        """
        # Start with base template
        template = self._deep_copy_template(self.base_template)
        
        # Apply industry overlay
        if industry in self.overlays:
            template = self._merge_template(template, self.overlays[industry])
        
        # Apply view type modifications
        template = self._apply_view_type(template, view_type)
        
        logger.debug(f"Generated template for {industry} ({view_type})")
        return template
    
    def _deep_copy_template(self, template: dict) -> dict:
        """Deep copy template dictionary."""
        import copy
        return copy.deepcopy(template)
    
    def _merge_template(self, base: dict, overlay: dict) -> dict:
        """
        Merge overlay into base template.
        
        Args:
            base: Base template
            overlay: Industry-specific overlay
        
        Returns:
            Merged template
        """
        # Add additional sections
        if "additional_sections" in overlay:
            base["sections"].extend(overlay["additional_sections"])
        
        # Apply metric priorities (for future sorting/highlighting)
        if "metric_priorities" in overlay:
            base["metric_priorities"] = overlay["metric_priorities"]
        
        return base
    
    def _apply_view_type(self, template: dict, view_type: str) -> dict:
        """
        Modify template based on view type.
        
        Args:
            template: Template to modify
            view_type: View type identifier
        
        Returns:
            Modified template
        """
        if view_type == "executive_summary":
            # Only show summary section
            template["sections"] = [
                s for s in template["sections"]
                if s["id"] == "summary"
            ]
        
        elif view_type == "detailed":
            # Include all metrics (no filtering)
            pass
        
        # 'standard' is default - no modifications needed
        
        return template
    
    def get_metrics_for_display(
        self,
        industry: str,
        view_type: str = "standard"
    ) -> List[str]:
        """
        Get flat list of metric IDs for display.
        
        Args:
            industry: Industry identifier
            view_type: View type
        
        Returns:
            List of metric IDs in display order
        """
        template = self.get_template(industry, view_type)
        
        metrics = []
        for section in template["sections"]:
            metrics.extend(section["metrics"])
        
        return metrics
    
    def format_section_header(self, section_title: str, width: int = 80) -> str:
        """
        Format a section header.
        
        Args:
            section_title: Title to format
            width: Total width
        
        Returns:
            Formatted header string
        """
        padding = (width - len(section_title) - 2) // 2
        return f"{'─' * padding} {section_title} {'─' * padding}"
