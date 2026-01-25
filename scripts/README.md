# FinLoom Scripts

## Directory Structure

```
scripts/
├── 01_backfill_historical.py   # Initial data load
├── 02_daily_update.py          # Daily cron job
├── 03_backup_to_s3.py          # Weekly backup
├── 04_generate_dashboard.py    # Monitoring dashboard
├── normalize_all.py            # Data normalization
├── seed_mappings.py            # DB initialization
├── reparse_all.py              # Re-parse all filings
├── backup_manager.py           # Advanced backup tool
├── clean_duplicates.py         # Deduplication
├── perf_test.py                # Performance testing
├── quick_check.py              # Quick status
├── verify_system.py            # System verification
├── cron_setup.sh               # Cron configuration
│
├── extraction/                 # Unstructured data extraction
│   ├── extract_unstructured.py
│   ├── extract_with_staging.py
│   ├── extract_tables_only.py
│   ├── validate_extraction.py
│   └── check_data_health.py
│
├── analysis/                   # Data viewing & quality
│   ├── view_company.py
│   ├── view_normalized.py
│   ├── compare_companies.py
│   ├── assess_quality.py
│   └── validate_normalized.py
│
└── archive/                    # Historical/one-off scripts
    ├── migrate_unstructured_schema.py
    ├── reprocess_missing_sections.py
    ├── reprocess_for_tables.py
    ├── test_parser.py
    └── test_idempotency.py
```

## Core Pipeline (Run in Order)

1. **Initial Setup** (run once):
   ```bash
   python scripts/seed_mappings.py
   python scripts/01_backfill_historical.py
   python scripts/normalize_all.py
   ```

2. **Daily Operations** (cron):
   ```bash
   # 9 AM daily
   python scripts/02_daily_update.py
   python scripts/normalize_all.py

   # 10 AM daily
   python scripts/04_generate_dashboard.py
   ```

3. **Weekly Backup** (cron):
   ```bash
   # Sunday 2 AM
   python scripts/03_backup_to_s3.py
   ```

## Script Categories

### Production (Root)
| Script | Purpose | Schedule |
|--------|---------|----------|
| `01_backfill_historical.py` | Load historical 10-K filings | Once |
| `02_daily_update.py` | Check for new filings | Daily 9 AM |
| `03_backup_to_s3.py` | S3 backup | Weekly |
| `04_generate_dashboard.py` | HTML dashboard | Daily |
| `normalize_all.py` | XBRL to normalized metrics | After updates |

### Extraction (`extraction/`)
| Script | Purpose |
|--------|---------|
| `extract_unstructured.py` | Extract sections, tables, footnotes |
| `extract_with_staging.py` | Robust extraction with staging tables |
| `extract_tables_only.py` | Re-extract tables only |
| `validate_extraction.py` | Validate extraction completeness |
| `check_data_health.py` | Quick health check |

### Analysis (`analysis/`)
| Script | Purpose |
|--------|---------|
| `view_company.py` | CLI: View company financials |
| `view_normalized.py` | View normalized metrics |
| `compare_companies.py` | Compare companies side-by-side |
| `assess_quality.py` | Data quality scoring |
| `validate_normalized.py` | Validate normalization |

### Archive (`archive/`)
Historical scripts kept for reference - not needed for regular operations.
