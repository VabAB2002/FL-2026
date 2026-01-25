#!/usr/bin/env python3
"""Test section parser on problematic filing."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.parsers.section_parser import InlineXBRLSectionParser

# Test AMD 2023 filing
filing_path = Path("/Users/V-Personal/FinLoom-2026/data/raw/2023/0000002488/000000248823000047")
accession = "0000002488-23-000047"

print(f"Testing parser on: {filing_path}")
print(f"Filing exists: {filing_path.exists()}")
print(f"Filing is directory: {filing_path.is_dir()}")

# List files in directory
if filing_path.is_dir():
    print("\nFiles in directory:")
    for f in sorted(filing_path.glob("*")):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name} ({size_mb:.2f} MB)")

# Test _find_primary_document
parser = InlineXBRLSectionParser(priority_only=False, preserve_html=True)

# Access private method for testing
primary_doc = parser._find_primary_document(filing_path)
print(f"\n_find_primary_document result: {primary_doc}")

if primary_doc:
    print(f"Primary document exists: {primary_doc.exists()}")
    print(f"Primary document size: {primary_doc.stat().st_size / (1024 * 1024):.2f} MB")

    # Try parsing
    print("\nAttempting to parse filing...")
    result = parser.parse_filing(filing_path, accession)

    print(f"\nParsing result:")
    print(f"  Success: {result.success}")
    print(f"  Sections found: {len(result.sections)}")
    print(f"  Error message: {result.error_message}")

    if result.sections:
        print("\nSection types extracted:")
        for section in result.sections:
            print(f"  - {section.section_type}")
else:
    print("\n❌ Could not find primary document")
