# Vendored Libraries

This directory contains vendored (bundled) copies of third-party libraries.

## Why Vendor?

Libraries in this directory are vendored for specific reasons:
- Not available on PyPI or has excessive dependencies
- Require customization for our use case
- Need version stability and predictability
- Want to minimize external dependencies

## Included Libraries

### unstructured

**Purpose:** HTML-to-Markdown conversion for SEC filings

**Source:** https://github.com/Unstructured-IO/unstructured

**License:** Apache 2.0

**Version:** Vendored snapshot (see git history for exact commit)

**Usage:**
```python
from src.vendor.unstructured.partition.html import partition_html
from src.vendor.unstructured.staging.base import elements_to_md
```

**Documentation:** See [unstructured/README.md](unstructured/README.md) for details

**What we use:**
- `unstructured.partition.html.partition_html` - Parse HTML into structured elements
- `unstructured.staging.base.elements_to_md` - Convert elements to Markdown

**What we don't use:**
- OCR models
- PDF parsing
- Image processing
- Other document formats

## Adding New Vendored Libraries

When vendoring a new library:

1. Create a subdirectory: `src/vendor/library_name/`
2. Copy only the necessary files (minimize footprint)
3. Add a README.md explaining:
   - Why it's vendored
   - Source URL and version
   - License information
   - What parts are used
4. Update this README with the new library
5. Test imports work correctly

## Updating Vendored Libraries

To update a vendored library:

1. Check the upstream repository for changes
2. Review the changelog for breaking changes
3. Copy new version to the directory
4. Test all imports and functionality
5. Update version information in README
6. Document changes in git commit message

## License Compliance

All vendored libraries must:
- Have compatible licenses (Apache 2.0, MIT, BSD, etc.)
- Include original license files
- Be properly attributed
- Not violate any licensing terms

The Unstructured library is Apache 2.0 licensed, which is compatible with our project.
