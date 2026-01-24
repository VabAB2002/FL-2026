# FinLoom SEC Data Pipeline

A production-grade SEC 10-K data extraction and storage system designed for financial analysis, RAG implementations, and knowledge graph research.

## Overview

This pipeline downloads, parses, and stores SEC 10-K filings for target companies. It extracts:

- **Structured Financial Data**: XBRL-tagged financial statements (balance sheet, income statement, cash flow)
- **Text Sections**: Business description, risk factors, MD&A, and more
- **Metadata**: Filing dates, company information, and processing status

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Local Machine                        │
├─────────────────────────────────────────────────────────┤
│  SEC RSS Poller → Downloader → XBRL Parser              │
│                              → Section Extractor        │
│                                      ↓                  │
│                               DuckDB Database           │
│                               Local File Storage        │
└─────────────────────────────────────────────────────────┘
                              ↓
                    AWS S3 (Backup Only)
```

## Quick Start

### Prerequisites

- Python 3.11+
- 500GB+ disk space (for raw files)
- AWS account (optional, for backups)

### Installation

```bash
# Clone the repository
cd FinLoom-2026

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env
# Edit .env with your settings
```

### Configuration

1. Edit `.env` with your SEC User-Agent (required by SEC):
   ```
   SEC_API_USER_AGENT="YourName your.email@example.com"
   ```

2. Optionally configure AWS credentials for S3 backups.

3. Review `config/settings.yaml` for company list and extraction settings.

### Running the Pipeline

```bash
# Initial historical data backfill (10 companies × 10 years)
python scripts/01_backfill_historical.py

# Daily updates (run via cron)
python scripts/02_daily_update.py

# Backup to S3 (optional)
python scripts/03_backup_to_s3.py
```

## Project Structure

```
FinLoom-2026/
├── config/
│   ├── settings.yaml       # Main configuration
│   └── logging.yaml        # Logging configuration
├── data/
│   ├── raw/                # Raw SEC filings
│   ├── processed/          # Processed parquet files
│   └── database/           # DuckDB database
├── src/
│   ├── ingestion/          # SEC API and downloader
│   ├── parsers/            # XBRL and HTML parsers
│   ├── storage/            # Database and S3 operations
│   ├── validation/         # Data quality checks
│   └── utils/              # Common utilities
├── scripts/                # Automation scripts
├── tests/                  # Test suite
└── logs/                   # Log files
```

## Target Companies (MVP)

| Ticker | Company | CIK |
|--------|---------|-----|
| AAPL | Apple Inc | 0000320193 |
| AMZN | Amazon.com Inc | 0001018724 |
| GOOGL | Alphabet Inc | 0001652044 |
| MSFT | Microsoft Corporation | 0000789019 |
| META | Meta Platforms Inc | 0001326801 |
| TSLA | Tesla Inc | 0001318605 |
| NVDA | NVIDIA Corporation | 0001045810 |
| BRK-B | Berkshire Hathaway Inc | 0000886982 |
| JPM | JPMorgan Chase & Co | 0000019617 |
| IBM | IBM Corporation | 0000051143 |

## Data Schema

### Companies Table
- `cik`: SEC Central Index Key (primary key)
- `company_name`: Official company name
- `ticker`: Stock ticker symbol

### Filings Table
- `accession_number`: Unique filing identifier
- `cik`: Company CIK (foreign key)
- `form_type`: Filing type (10-K)
- `filing_date`: Date filed
- `period_of_report`: Fiscal period end date

### Facts Table
- `concept_name`: XBRL concept (e.g., us-gaap:Assets)
- `value`: Numeric value
- `unit`: Unit of measure (USD, shares)
- `period_end`: Reporting period end date

### Sections Table
- `section_type`: Section identifier (item_1, item_1a, etc.)
- `content`: Extracted text content
- `word_count`: Word count for analytics

## Cost Estimate

| Component | Monthly Cost |
|-----------|--------------|
| AWS S3 Storage | ~$5-10 |
| Local processing | Free |
| **Total** | **~$5-10/month** |

## Development

```bash
# Run tests
pytest tests/

# Code formatting
black src/ tests/
ruff check src/ tests/

# Type checking
mypy src/
```

## License

MIT License - See LICENSE file for details.
