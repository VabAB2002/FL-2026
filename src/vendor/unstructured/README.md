# Unstructured Library (Vendored)

## Overview

This directory contains a vendored copy of the [Unstructured](https://github.com/Unstructured-IO/unstructured) library, specifically used for HTML-to-Markdown conversion in the FinLoom SEC data pipeline.

## Why Vendored?

The Unstructured library is **not** available via `pip install` in our requirements.txt. Instead, we vendor a minimal subset of the library to:

1. **Minimize dependencies** - The full Unstructured library has many heavy dependencies we don't need
2. **Stability** - Pinning a specific version prevents breaking changes from upstream
3. **Customization** - We can make modifications if needed for SEC filing processing
4. **Lightweight** - We only include the HTML partitioning functionality we actually use

## What We Use

From the Unstructured library, we specifically use:

- **`unstructured.partition.html.partition_html`** - Parse HTML documents into structured elements
- **`unstructured.staging.base.elements_to_md`** - Convert structured elements to clean Markdown

## Usage in FinLoom

The library is automatically added to Python's path in `src/processing/unstructured_pipeline.py`:

```python
import sys
from pathlib import Path

# Add unstructured library to path
UNSTRUCTURED_PATH = Path(__file__).parent.parent.parent / "unstructured-main"
if str(UNSTRUCTURED_PATH) not in sys.path:
    sys.path.insert(0, str(UNSTRUCTURED_PATH))

from unstructured.partition.html import partition_html
from unstructured.staging.base import elements_to_md
```

## Version Information

- **Source:** https://github.com/Unstructured-IO/unstructured
- **Version:** Vendored snapshot (check git history for exact commit)
- **License:** Apache 2.0 (see upstream repository)

## Maintenance

### Updating the Vendored Library

If you need to update to a newer version:

1. Clone the upstream repository:
   ```bash
   git clone https://github.com/Unstructured-IO/unstructured.git temp-unstructured
   cd temp-unstructured
   git checkout <desired-version-tag>
   ```

2. Copy only the needed modules:
   ```bash
   rsync -av --include='*.py' \
       temp-unstructured/unstructured/ \
       FinLoom-2026/unstructured-main/unstructured/
   ```

3. Test the integration:
   ```bash
   python -m pytest tests/test_unstructured_pipeline.py
   ```

4. Document the version in git commit message

### Testing Compatibility

After any updates, verify:

```bash
# Test HTML to Markdown conversion
python scripts/check_data_quality.py --mode quick

# Run full pipeline test
python -m pytest src/processing/
```

## Structure

```
unstructured-main/
└── unstructured/
    ├── __init__.py
    ├── partition/
    │   ├── html/          # HTML parsing (what we use)
    │   └── common/        # Common utilities
    ├── staging/
    │   └── base.py        # Markdown conversion (what we use)
    ├── documents/         # Document element models
    ├── cleaners/          # Text cleaning utilities
    └── utils.py           # General utilities
```

## Alternative Approaches

If vendoring becomes problematic, alternatives include:

1. **Install from pip** - Add `unstructured` to requirements.txt (but adds ~100MB of dependencies)
2. **Git submodule** - Use git submodule instead of vendored copy
3. **Custom implementation** - Write our own HTML-to-Markdown converter (reinventing the wheel)

For now, vendoring provides the best balance of simplicity and control.

## Troubleshooting

### ImportError: No module named 'unstructured'

The path addition may have failed. Check:
1. Is `unstructured-main/` in the repository root?
2. Does it contain `unstructured/` subdirectory with `__init__.py`?
3. Is the path being added before imports in `unstructured_pipeline.py`?

### Conversion Errors

If HTML-to-Markdown conversion fails:
1. Check the HTML file is valid (not corrupted)
2. Verify the HTML file exists at the specified path
3. Check logs for specific error messages
4. Consider updating the vendored library version

## License

The Unstructured library is licensed under Apache 2.0. See the upstream repository for full license text: https://github.com/Unstructured-IO/unstructured/blob/main/LICENSE.md

FinLoom's use of this library complies with the Apache 2.0 license terms.
