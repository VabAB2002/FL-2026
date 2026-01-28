"""
FinLoom SEC Data Pipeline
=========================

A production-grade SEC 10-K data extraction and storage system.

Source code organization:
- business/    - Business logic and concept mapping
- caching/     - Redis caching layer
- config/      - Configuration management (deprecated, use utils.config)
- core/        - Core domain types, exceptions, repository protocols
- ingestion/   - SEC API client and file downloading
- monitoring/  - Health checks, metrics, tracing
- parsers/     - XBRL parsing
- processing/  - Data processing pipelines
- storage/     - Database operations and repositories
- utils/       - Shared utilities (config, logging, rate limiting)
- validation/  - Data quality and validation
- vendor/      - Vendored third-party libraries
"""

__version__ = "0.1.0"
