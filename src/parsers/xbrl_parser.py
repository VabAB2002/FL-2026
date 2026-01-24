"""
XBRL parser for SEC filings.

Extracts financial data from XBRL instance documents using Arelle library.
"""

import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

from ..utils.config import get_settings
from ..utils.logger import get_logger

logger = get_logger("finloom.parsers.xbrl")


# Priority concepts to extract (US-GAAP taxonomy)
CORE_CONCEPTS = [
    # Balance Sheet - Assets
    "us-gaap:Assets",
    "us-gaap:AssetsCurrent",
    "us-gaap:AssetsNoncurrent",
    "us-gaap:CashAndCashEquivalentsAtCarryingValue",
    "us-gaap:ShortTermInvestments",
    "us-gaap:AccountsReceivableNetCurrent",
    "us-gaap:InventoryNet",
    "us-gaap:PropertyPlantAndEquipmentNet",
    "us-gaap:Goodwill",
    "us-gaap:IntangibleAssetsNetExcludingGoodwill",
    
    # Balance Sheet - Liabilities
    "us-gaap:Liabilities",
    "us-gaap:LiabilitiesCurrent",
    "us-gaap:LiabilitiesNoncurrent",
    "us-gaap:AccountsPayableCurrent",
    "us-gaap:LongTermDebt",
    "us-gaap:LongTermDebtNoncurrent",
    "us-gaap:ShortTermBorrowings",
    
    # Balance Sheet - Equity
    "us-gaap:StockholdersEquity",
    "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    "us-gaap:RetainedEarningsAccumulatedDeficit",
    "us-gaap:CommonStockValue",
    "us-gaap:AdditionalPaidInCapital",
    "us-gaap:TreasuryStockValue",
    
    # Income Statement
    "us-gaap:Revenues",
    "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "us-gaap:SalesRevenueNet",
    "us-gaap:CostOfRevenue",
    "us-gaap:CostOfGoodsAndServicesSold",
    "us-gaap:GrossProfit",
    "us-gaap:OperatingExpenses",
    "us-gaap:SellingGeneralAndAdministrativeExpense",
    "us-gaap:ResearchAndDevelopmentExpense",
    "us-gaap:OperatingIncomeLoss",
    "us-gaap:InterestExpense",
    "us-gaap:InterestIncome",
    "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "us-gaap:IncomeTaxExpenseBenefit",
    "us-gaap:NetIncomeLoss",
    "us-gaap:NetIncomeLossAttributableToParent",
    
    # Per Share Data
    "us-gaap:EarningsPerShareBasic",
    "us-gaap:EarningsPerShareDiluted",
    "us-gaap:CommonStockSharesOutstanding",
    "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
    
    # Cash Flow Statement
    "us-gaap:NetCashProvidedByUsedInOperatingActivities",
    "us-gaap:NetCashProvidedByUsedInInvestingActivities",
    "us-gaap:NetCashProvidedByUsedInFinancingActivities",
    "us-gaap:DepreciationDepletionAndAmortization",
    "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    "us-gaap:PaymentsForRepurchaseOfCommonStock",
    "us-gaap:PaymentsOfDividendsCommonStock",
    "us-gaap:ProceedsFromIssuanceOfLongTermDebt",
    "us-gaap:RepaymentsOfLongTermDebt",
    
    # Other Important Metrics
    "us-gaap:CommonStockDividendsPerShareDeclared",
    "us-gaap:EffectiveIncomeTaxRateContinuingOperations",
]


@dataclass
class XBRLFact:
    """Represents an extracted XBRL fact."""
    concept_name: str
    concept_namespace: str
    concept_local_name: str
    value: Optional[Decimal]
    value_text: Optional[str]
    unit: Optional[str]
    decimals: Optional[int]
    period_type: str  # 'instant' or 'duration'
    period_start: Optional[date]
    period_end: Optional[date]
    dimensions: dict = field(default_factory=dict)
    is_custom: bool = False
    is_negated: bool = False
    # New fields for hierarchy and categorization
    section: Optional[str] = None
    parent_concept: Optional[str] = None
    label: Optional[str] = None
    depth: Optional[int] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        return {
            "concept_name": self.concept_name,
            "concept_namespace": self.concept_namespace,
            "concept_local_name": self.concept_local_name,
            "value": self.value,
            "value_text": self.value_text,
            "unit": self.unit,
            "decimals": self.decimals,
            "period_type": self.period_type,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "dimensions": self.dimensions if self.dimensions else None,
            "is_custom": self.is_custom,
            "is_negated": self.is_negated,
            "section": self.section,
            "parent_concept": self.parent_concept,
            "label": self.label,
            "depth": self.depth,
        }


@dataclass
class ConceptHierarchy:
    """Represents concept hierarchy from presentation linkbase."""
    concept_name: str
    section: str
    parent_concept: Optional[str]
    depth: int
    order: float


@dataclass
class XBRLParseResult:
    """Result of parsing an XBRL document."""
    success: bool
    accession_number: str
    facts: list[XBRLFact] = field(default_factory=list)
    core_facts: list[XBRLFact] = field(default_factory=list)
    filing_date: Optional[date] = None
    period_end: Optional[date] = None
    company_name: Optional[str] = None
    document_type: Optional[str] = None
    error_message: Optional[str] = None
    parse_time_ms: float = 0.0
    
    @property
    def fact_count(self) -> int:
        return len(self.facts)
    
    @property
    def core_fact_count(self) -> int:
        return len(self.core_facts)


class XBRLParser:
    """
    XBRL parser using Arelle library.
    
    Extracts financial facts from XBRL instance documents,
    including contexts, units, and dimensional information.
    """
    
    def __init__(self, extract_all_facts: bool = False) -> None:
        """
        Initialize XBRL parser.
        
        Args:
            extract_all_facts: If True, extract all facts, not just core concepts.
        """
        self.extract_all_facts = extract_all_facts
        self.core_concepts = set(CORE_CONCEPTS)
        
        logger.info("XBRL parser initialized")
    
    def _load_xbrl_with_arelle(self, xbrl_file: Path):
        """Load XBRL file using Arelle and return ModelXbrl."""
        try:
            from arelle import Cntlr, FileSource
            
            # Create controller with minimal output
            controller = Cntlr.Cntlr(hasGui=False, logFileName=None)
            model_manager = controller.modelManager
            
            # Create FileSource from the XBRL file
            file_source = FileSource.FileSource(str(xbrl_file))
            
            # Load the model
            model_xbrl = model_manager.load(file_source)
            
            logger.debug(f"Arelle loaded {len(model_xbrl.facts) if hasattr(model_xbrl, 'facts') else 0} facts")
            return model_xbrl
            
        except ImportError as e:
            logger.error(f"Failed to import Arelle: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to load XBRL with Arelle: {e}")
            raise
    
    def parse_filing(
        self,
        filing_path: Path,
        accession_number: str,
    ) -> XBRLParseResult:
        """
        Parse XBRL files from a filing directory.
        
        Args:
            filing_path: Path to the filing directory containing XBRL files.
            accession_number: The filing's accession number.
        
        Returns:
            XBRLParseResult with extracted facts.
        """
        import time
        start_time = time.time()
        
        logger.info(f"Parsing XBRL for {accession_number}")
        
        # Find XBRL instance document
        xbrl_file = self._find_xbrl_instance(filing_path)
        
        if not xbrl_file:
            return XBRLParseResult(
                success=False,
                accession_number=accession_number,
                error_message="No XBRL instance document found",
            )
        
        try:
            # Parse linkbases for hierarchy and labels
            hierarchy = {}
            labels = {}
            
            if self.extract_all_facts:
                pre_file, lab_file = self.find_linkbase_files(filing_path)
                
                if pre_file:
                    hierarchy = self.parse_presentation_linkbase(pre_file)
                    logger.debug(f"Loaded {len(hierarchy)} hierarchy entries")
                
                if lab_file:
                    labels = self.parse_label_linkbase(lab_file)
                    logger.debug(f"Loaded {len(labels)} labels")
            
            # Parse using Arelle
            facts = self._parse_with_arelle(xbrl_file)
            
            # Enrich facts with hierarchy and labels
            if self.extract_all_facts:
                for fact in facts:
                    # Add hierarchy info
                    hier = hierarchy.get(fact.concept_name)
                    if hier:
                        fact.section = hier.section
                        fact.parent_concept = hier.parent_concept
                        fact.depth = hier.depth
                    
                    # Add label
                    label = labels.get(fact.concept_name)
                    if label:
                        fact.label = label
                    else:
                        # Generate label from concept name
                        fact.label = self._generate_label(fact.concept_local_name)
            
            # Separate core facts
            core_facts = [f for f in facts if f.concept_name in self.core_concepts]
            
            # Extract metadata
            period_end = self._extract_period_end(facts)
            
            elapsed_ms = (time.time() - start_time) * 1000
            
            logger.info(
                f"Parsed {len(facts)} facts ({len(core_facts)} core) "
                f"from {accession_number} in {elapsed_ms:.0f}ms"
            )
            
            return XBRLParseResult(
                success=True,
                accession_number=accession_number,
                facts=facts if self.extract_all_facts else core_facts,
                core_facts=core_facts,
                period_end=period_end,
                parse_time_ms=elapsed_ms,
            )
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.error(f"Failed to parse XBRL for {accession_number}: {e}")
            
            return XBRLParseResult(
                success=False,
                accession_number=accession_number,
                error_message=str(e),
                parse_time_ms=elapsed_ms,
            )
    
    def _generate_label(self, concept_local_name: str) -> str:
        """Generate a human-readable label from concept name."""
        # Split camelCase
        import re
        words = re.sub(r'([A-Z])', r' \1', concept_local_name).strip()
        return words
    
    def _find_xbrl_instance(self, filing_path: Path) -> Optional[Path]:
        """Find the main XBRL instance document in a filing."""
        # Look for common XBRL instance patterns
        patterns = [
            "*_htm.xml",      # Inline XBRL
            "*-*.xml",        # Standard XBRL instance
            "*.xml",          # Any XML
        ]
        
        # Exclude taxonomy files
        exclude_patterns = ["_cal.xml", "_def.xml", "_lab.xml", "_pre.xml", ".xsd"]
        
        for pattern in patterns:
            for file in filing_path.glob(pattern):
                # Skip taxonomy files
                if any(file.name.lower().endswith(ex) for ex in exclude_patterns):
                    continue
                
                # Check if it looks like an XBRL instance
                if self._is_xbrl_instance(file):
                    logger.debug(f"Found XBRL instance: {file.name}")
                    return file
        
        return None
    
    def _is_xbrl_instance(self, file_path: Path) -> bool:
        """Check if a file is an XBRL instance document."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                # Read first few KB to check
                content = f.read(8192)
                
                # Look for XBRL indicators - including default namespace patterns
                xbrl_indicators = [
                    # Prefixed XBRL namespace
                    "xmlns:xbrli",
                    "<xbrli:",
                    # Default XBRL namespace (common in modern filings)
                    'xmlns="http://www.xbrl.org/2003/instance"',
                    "<xbrl",  # Root element for XBRL instance
                    # Context elements (definitive XBRL indicator)
                    "<context",
                    # Inline XBRL indicators
                    "ix:header",
                    "ix:resources",
                    "xmlns:ix",
                ]
                
                return any(indicator in content for indicator in xbrl_indicators)
        except Exception:
            return False
    
    def _parse_with_arelle(self, xbrl_file: Path) -> list[XBRLFact]:
        """Parse XBRL file using Arelle."""
        # Load the XBRL document
        model_xbrl = self._load_xbrl_with_arelle(xbrl_file)
        
        if model_xbrl is None:
            raise ValueError("Failed to load XBRL document")
        
        facts = []
        
        try:
            # Extract facts from the loaded model
            if hasattr(model_xbrl, 'facts') and model_xbrl.facts:
                for model_fact in model_xbrl.facts:
                    fact = self._extract_fact(model_fact)
                    if fact:
                        facts.append(fact)
            else:
                logger.warning("Model has no facts")
        finally:
            # Clean up
            try:
                model_xbrl.close()
            except Exception as e:
                logger.debug(f"Error closing model: {e}")
        
        return facts
    
    def _extract_fact(self, model_fact) -> Optional[XBRLFact]:
        """Extract a single fact from Arelle model fact."""
        try:
            # Get concept information
            concept = model_fact.concept
            if concept is None:
                return None
            
            qname = model_fact.qname
            namespace = qname.namespaceURI if qname else ""
            local_name = qname.localName if qname else ""
            
            # Build full concept name
            prefix = self._get_namespace_prefix(namespace)
            concept_name = f"{prefix}:{local_name}" if prefix else local_name
            
            # Determine if custom concept
            is_custom = not namespace.startswith("http://fasb.org/us-gaap")
            
            # Get value
            value = None
            value_text = None
            
            if model_fact.isNumeric:
                try:
                    value = Decimal(str(model_fact.value)) if model_fact.value else None
                except (InvalidOperation, ValueError):
                    value_text = str(model_fact.value)
            else:
                value_text = str(model_fact.value) if model_fact.value else None
            
            # Get unit
            unit = None
            if model_fact.unit is not None:
                unit_measures = model_fact.unit.measures
                if unit_measures and unit_measures[0]:
                    unit = str(unit_measures[0][0].localName)
            
            # Get decimals
            decimals = None
            if hasattr(model_fact, "decimals") and model_fact.decimals:
                try:
                    decimals = int(model_fact.decimals)
                except ValueError:
                    pass
            
            # Get context (period and dimensions)
            context = model_fact.context
            period_type = "instant"
            period_start = None
            period_end = None
            dimensions = {}
            
            if context is not None:
                if context.isInstantPeriod:
                    period_type = "instant"
                    period_end = context.instantDatetime.date() if context.instantDatetime else None
                elif context.isStartEndPeriod:
                    period_type = "duration"
                    period_start = context.startDatetime.date() if context.startDatetime else None
                    period_end = context.endDatetime.date() if context.endDatetime else None
                
                # Extract dimensions
                for dim_value in context.qnameDims.values():
                    dim_name = str(dim_value.dimensionQname)
                    member_name = str(dim_value.memberQname) if dim_value.isExplicit else str(dim_value.typedMember)
                    dimensions[dim_name] = member_name
            
            return XBRLFact(
                concept_name=concept_name,
                concept_namespace=prefix,
                concept_local_name=local_name,
                value=value,
                value_text=value_text,
                unit=unit,
                decimals=decimals,
                period_type=period_type,
                period_start=period_start,
                period_end=period_end,
                dimensions=dimensions,
                is_custom=is_custom,
            )
            
        except Exception as e:
            logger.debug(f"Failed to extract fact: {e}")
            return None
    
    def _get_namespace_prefix(self, namespace: str) -> str:
        """Get standard prefix for namespace."""
        prefix_map = {
            "http://fasb.org/us-gaap/": "us-gaap",
            "http://xbrl.sec.gov/dei/": "dei",
            "http://www.xbrl.org/2003/instance": "xbrli",
        }
        
        for ns, prefix in prefix_map.items():
            if namespace.startswith(ns):
                return prefix
        
        return ""
    
    def parse_presentation_linkbase(self, pre_file: Path) -> dict:
        """
        Parse presentation linkbase for concept hierarchy.
        
        Returns dict: {concept_name: ConceptHierarchy}
        """
        from bs4 import BeautifulSoup
        
        hierarchy = {}
        
        try:
            with open(pre_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'lxml-xml')
            
            # Find all presentation links (each represents a section/role)
            pres_links = soup.find_all('link:presentationLink') or soup.find_all('presentationLink')
            
            for pres_link in pres_links:
                # Get the role (section name)
                role = pres_link.get('xlink:role', '') or pres_link.get('role', '')
                section = self._extract_section_from_role(role)
                
                # Build parent-child relationships from arcs
                arcs = pres_link.find_all('link:presentationArc') or pres_link.find_all('presentationArc')
                locs = pres_link.find_all('link:loc') or pres_link.find_all('loc')
                
                # Map labels to concepts
                label_to_concept = {}
                for loc in locs:
                    label = loc.get('xlink:label', '') or loc.get('label', '')
                    href = loc.get('xlink:href', '') or loc.get('href', '')
                    # Extract concept name from href
                    if '#' in href:
                        concept = href.split('#')[-1]
                        # Normalize concept name
                        if 'us-gaap_' in concept:
                            concept = 'us-gaap:' + concept.split('us-gaap_')[-1]
                        elif 'us-gaap:' not in concept and 'dei:' not in concept:
                            # Try to determine namespace from href
                            if 'us-gaap' in href.lower():
                                concept = f'us-gaap:{concept}'
                        label_to_concept[label] = concept
                
                # Build hierarchy from arcs
                parent_child = {}  # child -> parent
                orders = {}  # child -> order
                
                for arc in arcs:
                    from_label = arc.get('xlink:from', '') or arc.get('from', '')
                    to_label = arc.get('xlink:to', '') or arc.get('to', '')
                    order = float(arc.get('order', 1.0))
                    
                    from_concept = label_to_concept.get(from_label)
                    to_concept = label_to_concept.get(to_label)
                    
                    if from_concept and to_concept:
                        parent_child[to_concept] = from_concept
                        orders[to_concept] = order
                
                # Calculate depths
                def get_depth(concept, visited=None):
                    if visited is None:
                        visited = set()
                    if concept in visited:
                        return 0
                    visited.add(concept)
                    parent = parent_child.get(concept)
                    if parent:
                        return 1 + get_depth(parent, visited)
                    return 0
                
                # Add to hierarchy
                for concept in label_to_concept.values():
                    if concept and concept not in hierarchy:
                        hierarchy[concept] = ConceptHierarchy(
                            concept_name=concept,
                            section=section,
                            parent_concept=parent_child.get(concept),
                            depth=get_depth(concept),
                            order=orders.get(concept, 999.0)
                        )
            
            logger.debug(f"Parsed {len(hierarchy)} concepts from presentation linkbase")
            return hierarchy
            
        except Exception as e:
            logger.warning(f"Failed to parse presentation linkbase: {e}")
            return {}
    
    def _extract_section_from_role(self, role: str) -> str:
        """Extract section name from role URI."""
        if not role:
            return "Other"
        
        # Common patterns
        # http://www.apple.com/role/CONSOLIDATEDSTATEMENTSOFOPERATIONS
        # http://fasb.org/us-gaap/role/statement/StatementOfFinancialPositionClassified
        
        # Get last part of URI
        parts = role.rstrip('/').split('/')
        section = parts[-1] if parts else "Other"
        
        # Clean up common patterns
        section = section.replace('role', '').replace('Role', '')
        section = section.replace('CONSOLIDATED', '').replace('Consolidated', '')
        section = section.replace('STATEMENTS', 'Statement').replace('Statements', 'Statement')
        section = section.replace('Statement', '')
        
        # Map to standard names
        section_map = {
            'IncomeStatement': 'IncomeStatement',
            'OFOPERATIONS': 'IncomeStatement',
            'Operations': 'IncomeStatement',
            'ComprehensiveIncome': 'IncomeStatement',
            'BalanceSheet': 'BalanceSheet',
            'FinancialPosition': 'BalanceSheet',
            'OFFINANCIALPOSITION': 'BalanceSheet',
            'CashFlow': 'CashFlowStatement',
            'OFCASHFLOWS': 'CashFlowStatement',
            'CashFlows': 'CashFlowStatement',
            'Equity': 'StockholdersEquity',
            'OFSTOCKHOLDERSEQUITY': 'StockholdersEquity',
            'StockholdersEquity': 'StockholdersEquity',
            'FinancialInstruments': 'FinancialInstruments',
            'FairValue': 'FairValue',
            'Debt': 'Debt',
            'Leases': 'Leases',
            'Commitments': 'Commitments',
            'IncomeTaxes': 'IncomeTaxes',
            'Taxes': 'IncomeTaxes',
            'SegmentReporting': 'Segments',
            'Segments': 'Segments',
            'CoverPage': 'CoverPage',
            'DocumentAndEntityInformation': 'CoverPage',
        }
        
        for pattern, standard in section_map.items():
            if pattern.lower() in section.lower():
                return standard
        
        # Return cleaned section or Other
        return section.strip('_-') if section else "Other"
    
    def parse_label_linkbase(self, lab_file: Path) -> dict:
        """
        Parse label linkbase for human-readable labels.
        
        Returns dict: {concept_name: label}
        """
        from bs4 import BeautifulSoup
        
        labels = {}
        
        try:
            with open(lab_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'lxml-xml')
            
            # Find label links
            label_links = soup.find_all('link:labelLink') or soup.find_all('labelLink')
            
            for label_link in label_links:
                # Map loc labels to concepts
                locs = label_link.find_all('link:loc') or label_link.find_all('loc')
                loc_to_concept = {}
                
                for loc in locs:
                    label = loc.get('xlink:label', '') or loc.get('label', '')
                    href = loc.get('xlink:href', '') or loc.get('href', '')
                    if '#' in href:
                        concept = href.split('#')[-1]
                        # Normalize concept name
                        if 'us-gaap_' in concept:
                            concept = 'us-gaap:' + concept.split('us-gaap_')[-1]
                        elif 'us-gaap' in href.lower() and 'us-gaap:' not in concept:
                            concept = f'us-gaap:{concept}'
                        loc_to_concept[label] = concept
                
                # Get labels from labelArc -> label elements
                arcs = label_link.find_all('link:labelArc') or label_link.find_all('labelArc')
                label_elements = label_link.find_all('link:label') or label_link.find_all('label')
                
                # Map label element labels to text
                label_texts = {}
                for label_elem in label_elements:
                    label_id = label_elem.get('xlink:label', '') or label_elem.get('label', '')
                    role = label_elem.get('xlink:role', '') or label_elem.get('role', '')
                    text = label_elem.get_text(strip=True)
                    
                    # Prefer terseLabel or label roles
                    if 'terse' in role.lower() or 'label' in role.lower():
                        label_texts[label_id] = text
                    elif label_id not in label_texts:
                        label_texts[label_id] = text
                
                # Connect concepts to labels via arcs
                for arc in arcs:
                    from_label = arc.get('xlink:from', '') or arc.get('from', '')
                    to_label = arc.get('xlink:to', '') or arc.get('to', '')
                    
                    concept = loc_to_concept.get(from_label)
                    label_text = label_texts.get(to_label)
                    
                    if concept and label_text and concept not in labels:
                        labels[concept] = label_text
            
            logger.debug(f"Parsed {len(labels)} labels from label linkbase")
            return labels
            
        except Exception as e:
            logger.warning(f"Failed to parse label linkbase: {e}")
            return {}
    
    def find_linkbase_files(self, filing_path: Path) -> tuple:
        """Find presentation and label linkbase files."""
        pre_file = None
        lab_file = None
        
        for file in filing_path.glob("*_pre.xml"):
            pre_file = file
            break
        
        for file in filing_path.glob("*_lab.xml"):
            lab_file = file
            break
        
        return pre_file, lab_file
    
    def _extract_period_end(self, facts: list[XBRLFact]) -> Optional[date]:
        """Extract the main period end date from facts."""
        # Look for document period end date
        for fact in facts:
            if fact.concept_local_name == "DocumentPeriodEndDate" and fact.value_text:
                try:
                    return datetime.strptime(fact.value_text, "%Y-%m-%d").date()
                except ValueError:
                    pass
        
        # Fall back to most common period end date
        period_ends = [f.period_end for f in facts if f.period_end]
        if period_ends:
            from collections import Counter
            most_common = Counter(period_ends).most_common(1)
            if most_common:
                return most_common[0][0]
        
        return None
    
    def parse_inline_xbrl(
        self,
        html_path: Path,
        accession_number: str,
    ) -> XBRLParseResult:
        """
        Parse inline XBRL from an HTML document.
        
        Args:
            html_path: Path to the HTML document with inline XBRL.
            accession_number: The filing's accession number.
        
        Returns:
            XBRLParseResult with extracted facts.
        """
        # For inline XBRL, we use the same Arelle approach
        # The HTML file should be parseable as XBRL
        return self.parse_filing(html_path.parent, accession_number)


class SimpleXBRLParser:
    """
    Simplified XBRL parser without Arelle dependency.
    
    Uses regex and XML parsing for basic extraction.
    Less comprehensive but no external dependencies.
    """
    
    def __init__(self) -> None:
        """Initialize simple parser."""
        self.core_concepts = set(CORE_CONCEPTS)
        logger.info("Simple XBRL parser initialized")
    
    def parse_filing(
        self,
        filing_path: Path,
        accession_number: str,
    ) -> XBRLParseResult:
        """Parse XBRL using simple XML parsing."""
        import time
        import xml.etree.ElementTree as ET
        
        start_time = time.time()
        logger.info(f"Parsing XBRL (simple) for {accession_number}")
        
        # Find XBRL file
        xbrl_files = list(filing_path.glob("*.xml"))
        xbrl_files = [f for f in xbrl_files if not any(
            f.name.endswith(x) for x in ["_cal.xml", "_def.xml", "_lab.xml", "_pre.xml"]
        )]
        
        if not xbrl_files:
            return XBRLParseResult(
                success=False,
                accession_number=accession_number,
                error_message="No XBRL files found",
            )
        
        facts = []
        
        for xbrl_file in xbrl_files:
            try:
                tree = ET.parse(xbrl_file)
                root = tree.getroot()
                
                # Extract namespaces
                namespaces = dict([node for _, node in ET.iterparse(
                    str(xbrl_file), events=["start-ns"]
                )])
                
                # Find all facts (elements with numeric values and context refs)
                for elem in root.iter():
                    fact = self._parse_element(elem, namespaces)
                    if fact:
                        facts.append(fact)
                        
            except ET.ParseError as e:
                logger.warning(f"Failed to parse {xbrl_file.name}: {e}")
                continue
        
        core_facts = [f for f in facts if f.concept_name in self.core_concepts]
        elapsed_ms = (time.time() - start_time) * 1000
        
        logger.info(
            f"Parsed {len(facts)} facts ({len(core_facts)} core) in {elapsed_ms:.0f}ms"
        )
        
        return XBRLParseResult(
            success=True,
            accession_number=accession_number,
            facts=facts,
            core_facts=core_facts,
            parse_time_ms=elapsed_ms,
        )
    
    def _parse_element(self, elem, namespaces: dict) -> Optional[XBRLFact]:
        """Parse an XML element as a potential fact."""
        tag = elem.tag
        
        # Skip non-fact elements
        if not elem.text or not elem.text.strip():
            return None
        
        # Check for contextRef (indicates a fact)
        context_ref = elem.get("contextRef")
        if not context_ref:
            return None
        
        # Parse tag to get namespace and local name
        if tag.startswith("{"):
            namespace, local_name = tag[1:].split("}")
        else:
            namespace = ""
            local_name = tag
        
        # Get prefix
        prefix = ""
        for p, ns in namespaces.items():
            if ns == namespace:
                prefix = p
                break
        
        concept_name = f"{prefix}:{local_name}" if prefix else local_name
        
        # Parse value
        value = None
        value_text = None
        text = elem.text.strip()
        
        try:
            value = Decimal(text.replace(",", ""))
        except (InvalidOperation, ValueError):
            value_text = text
        
        # Get unit
        unit_ref = elem.get("unitRef")
        
        # Get decimals
        decimals = None
        decimals_str = elem.get("decimals")
        if decimals_str and decimals_str != "INF":
            try:
                decimals = int(decimals_str)
            except ValueError:
                pass
        
        return XBRLFact(
            concept_name=concept_name,
            concept_namespace=prefix,
            concept_local_name=local_name,
            value=value,
            value_text=value_text,
            unit=unit_ref,
            decimals=decimals,
            period_type="unknown",
            period_start=None,
            period_end=None,
            is_custom=prefix not in ["us-gaap", "dei"],
        )
