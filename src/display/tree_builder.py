"""
TreeBuilder: Build hierarchical tree structures from parent-child relationships.

Creates beautiful tree displays with box-drawing characters:
├── ├── └── │

Example output:
    Cash, Cash Equivalents and Marketable Securities
    ├── Cash and Cash Equivalents                    $29.9B
    ├── Current Marketable Securities                $35.2B
    │   ├── Money Market Funds                        $2.1B
    │   ├── US Treasury Securities                   $15.8B
    │   └── Other                                     $7.1B
    └── Total                                        $165.6B
"""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class TreeNode:
    """A node in the hierarchical tree."""
    concept_name: str
    label: str
    value: Optional[float]
    depth: int
    unit: Optional[str] = None
    children: List['TreeNode'] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []


class TreeBuilder:
    """
    Builds hierarchical tree structures from facts with parent-child relationships.
    """
    
    def build_tree(self, facts: List[dict]) -> List[TreeNode]:
        """
        Build tree from facts using parent_concept relationships.
        Handles cases where parent concepts are abstract (no values).
        
        Args:
            facts: List of fact dictionaries with parent_concept, depth, etc.
        
        Returns:
            List of root nodes (nodes without parents or with missing parents)
        """
        if not facts:
            return []
        
        # Build all nodes - only create one node per unique concept
        nodes_by_concept = {}
        all_parents = set()
        
        for fact in facts:
            concept_name = fact['concept_name']
            parent_concept = fact.get('parent_concept')
            
            # Track all parent concepts mentioned
            if parent_concept:
                all_parents.add(parent_concept)
            
            # Only create node if we haven't seen this concept yet
            if concept_name not in nodes_by_concept:
                nodes_by_concept[concept_name] = TreeNode(
                    concept_name=concept_name,
                    label=fact.get('label') or concept_name.replace('us-gaap:', '').replace('aapl:', ''),
                    value=float(fact['value']) if fact.get('value') else None,
                    depth=fact.get('depth') or 0,
                    unit=fact.get('unit'),
                    children=[]
                )
        
        # Create placeholder nodes for abstract parents that don't have fact values
        for parent_concept in all_parents:
            if parent_concept not in nodes_by_concept:
                # Create abstract parent node
                parent_label = parent_concept.replace('us-gaap:', '').replace('aapl:', '')
                # Convert CamelCase to Title Case
                import re
                parent_label = re.sub(r'([A-Z])', r' \1', parent_label).strip()
                
                nodes_by_concept[parent_concept] = TreeNode(
                    concept_name=parent_concept,
                    label=parent_label,
                    value=None,
                    depth=0,  # Will be calculated
                    unit=None,
                    children=[]
                )
        
        # Build parent-child relationships
        children_concepts = set()
        
        for concept_name, node in nodes_by_concept.items():
            # Find the fact to get parent info
            fact = next((f for f in facts if f['concept_name'] == concept_name), None)
            
            if fact:
                parent_concept = fact.get('parent_concept')
            else:
                parent_concept = None
            
            if parent_concept and parent_concept in nodes_by_concept:
                # Add as child to parent
                parent_node = nodes_by_concept[parent_concept]
                parent_node.children.append(node)
                children_concepts.add(concept_name)
        
        # Find roots (nodes that aren't children of anything)
        roots = [node for concept, node in nodes_by_concept.items() if concept not in children_concepts]
        
        # Sort children by depth and label
        self._sort_tree(roots)
        
        return roots
    
    def _sort_tree(self, nodes: List[TreeNode]) -> None:
        """Recursively sort tree nodes by depth then label."""
        nodes.sort(key=lambda n: (n.depth, n.label))
        for node in nodes:
            if node.children:
                self._sort_tree(node.children)
    
    def render_tree(
        self,
        nodes: List[TreeNode],
        prefix: str = "",
        is_last: bool = True,
        value_formatter = None
    ) -> List[str]:
        """
        Render tree with box-drawing characters.
        
        Args:
            nodes: List of tree nodes to render
            prefix: Prefix for indentation (used in recursion)
            is_last: Whether this is the last node at this level
            value_formatter: Optional function to format values
        
        Returns:
            List of formatted strings (one per line)
        """
        if not nodes:
            return []
        
        if value_formatter is None:
            value_formatter = self._default_formatter
        
        output = []
        
        for i, node in enumerate(nodes):
            is_last_child = (i == len(nodes) - 1)
            
            # Choose connector based on position
            connector = "└── " if is_last_child else "├── "
            
            # Format value
            value_str = value_formatter(node.value, node.unit) if node.value is not None else ""
            
            # Format line
            if value_str:
                # Pad label to align values
                label_width = 50 - len(prefix) - len(connector)
                label_padded = node.label[:label_width].ljust(label_width)
                line = f"{prefix}{connector}{label_padded} {value_str}"
            else:
                line = f"{prefix}{connector}{node.label}"
            
            output.append(line)
            
            # Recursively render children
            if node.children:
                # Add vertical line or space
                extension = "    " if is_last_child else "│   "
                child_prefix = prefix + extension
                
                child_lines = self.render_tree(
                    node.children,
                    child_prefix,
                    is_last_child,
                    value_formatter
                )
                output.extend(child_lines)
        
        return output
    
    def _default_formatter(self, value: float, unit: Optional[str]) -> str:
        """Default value formatter."""
        if unit == "USD":
            return self._format_currency(value)
        elif unit == "shares":
            return self._format_shares(value)
        else:
            return f"{value:,.2f}"
    
    def _format_currency(self, value: float) -> str:
        """Format currency values with B/M suffixes."""
        abs_value = abs(value)
        sign = "-" if value < 0 else ""
        
        if abs_value >= 1e9:
            return f"{sign}${abs_value/1e9:.1f}B"
        elif abs_value >= 1e6:
            return f"{sign}${abs_value/1e6:.1f}M"
        elif abs_value >= 1e3:
            return f"{sign}${abs_value/1e3:.1f}K"
        else:
            return f"{sign}${abs_value:.2f}"
    
    def _format_shares(self, value: float) -> str:
        """Format share counts."""
        abs_value = abs(value)
        sign = "-" if value < 0 else ""
        
        if abs_value >= 1e9:
            return f"{sign}{abs_value/1e9:.2f}B shares"
        elif abs_value >= 1e6:
            return f"{sign}{abs_value/1e6:.1f}M shares"
        else:
            return f"{sign}{abs_value:,.0f} shares"
    
    def render_simple(self, nodes: List[TreeNode], indent: int = 0) -> List[str]:
        """
        Render tree with simple indentation (no box characters).
        
        Args:
            nodes: List of tree nodes
            indent: Current indentation level
        
        Returns:
            List of formatted strings
        """
        output = []
        
        for node in nodes:
            indent_str = "  " * indent
            value_str = self._default_formatter(node.value, node.unit) if node.value is not None else ""
            
            if value_str:
                line = f"{indent_str}{node.label:45} {value_str}"
            else:
                line = f"{indent_str}{node.label}"
            
            output.append(line)
            
            if node.children:
                child_lines = self.render_simple(node.children, indent + 1)
                output.extend(child_lines)
        
        return output
