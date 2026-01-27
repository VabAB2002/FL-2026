# FinLoom Scripts

This directory contains operational scripts for FinLoom data processing and analysis.

## Directory Structure

```
scripts/
├── README.md                       # This file
├── 01_backfill_historical.py      # Historical data backfill
├── 02_daily_update.py             # Daily incremental updates
├── 03_backup_to_s3.py             # S3 backup operations
├── 04_generate_dashboard.py       # Dashboard generation
├── backup_manager.py              # Backup management
├── normalize_all.py               # Batch normalization
├── reparse_all.py                 # Batch re-parsing
├── seed_mappings.py               # Initialize concept mappings
├── quick_check.py                 # Quick system check
├── perf_test.py                   # Performance testing
├── extraction/                    # Extraction scripts
│   ├── extract_unstructured.py    # Unstructured data extraction
│   ├── extract_with_staging.py    # Staged extraction
│   ├── extract_tables_only.py     # Table-only extraction
│   ├── validate_extraction.py     # Extraction validation
│   └── check_data_health.py       # Data health checks
└── analysis/                      # Analysis scripts
    ├── assess_quality.py          # Quality assessment
    ├── validate_normalized.py     # Normalization validation
    ├── view_normalized.py         # View normalized data
    ├── compare_companies.py       # Company comparisons
    └── view_company.py            # View company data
```

## ⚠️ Deprecated Scripts (Removed)

The following scripts have been **removed** because their functionality has been moved to the core codebase:

| Deprecated Script | Replaced By | Notes |
|-------------------|-------------|-------|
| `fix_missing_sections.py` | `finloom recovery reprocess` | Root cause fixed: pipeline now validates section extraction |
| `clean_duplicates.py` | `finloom db clean-duplicates --execute` | Built into Database class with proper transactions |
| `verify_system.py` | `finloom status --verify-integrity` | Built into DatabaseHealthChecker |
| `archive/reprocess_missing_sections.py` | `finloom recovery reprocess` | Same as fix_missing_sections.py |
| `archive/reprocess_for_tables.py` | `finloom recovery reprocess` | Replaced by comprehensive recovery system |
| `archive/migrate_unstructured_schema.py` | `migrations/001_unstructured_schema.py` | Moved to proper migrations directory |
| `archive/test_*.py` | N/A | Ad-hoc tests, should use proper test framework |

### Migration Guide

If you were using the deprecated scripts, here's how to migrate:

#### fix_missing_sections.py → finloom recovery reprocess

**Before:**
```bash
python scripts/fix_missing_sections.py --dry-run
python scripts/fix_missing_sections.py --execute
```

**After:**
```bash
# Dry run
python finloom.py recovery reprocess --dry-run

# Execute
python finloom.py recovery reprocess

# Reprocess specific ticker
python finloom.py recovery reprocess --ticker AAPL

# Force reprocess even if has sections
python finloom.py recovery reprocess --force
```

#### clean_duplicates.py → finloom db clean-duplicates

**Before:**
```bash
python scripts/clean_duplicates.py --dry-run
python scripts/clean_duplicates.py --execute
```

**After:**
```bash
# Detect duplicates
python finloom.py db detect-duplicates

# Preview cleanup (dry run)
python finloom.py db clean-duplicates

# Execute cleanup
python finloom.py db clean-duplicates --execute
```

#### verify_system.py → finloom status --verify-integrity

**Before:**
```bash
python scripts/verify_system.py
```

**After:**
```bash
# Basic status
python finloom.py status

# Comprehensive verification
python finloom.py status --verify-integrity
```

---

## Production Scripts

### 01_backfill_historical.py

**Purpose:** Backfill historical SEC filings for a list of companies

**Usage:**
```bash
python scripts/01_backfill_historical.py
```

**What it does:**
- Downloads historical 10-K filings
- Processes XBRL data
- Extracts unstructured content
- Normalizes financial metrics

**Schedule:** Run once during initial setup or when adding new companies

---

### 02_daily_update.py

**Purpose:** Daily incremental update of SEC filings

**Usage:**
```bash
python scripts/02_daily_update.py
```

**What it does:**
- Checks for new filings since last run
- Downloads new filings
- Processes and normalizes data
- Updates metrics

**Schedule:** Run daily (recommended: 6 AM ET after SEC updates)

---

### 03_backup_to_s3.py

**Purpose:** Backup database and raw data to S3

**Usage:**
```bash
python scripts/03_backup_to_s3.py
```

**What it does:**
- Creates database backup
- Uploads to S3 with versioning
- Validates backup integrity
- Manages retention policy

**Schedule:** Run daily (recommended: after daily_update.py)

**Configuration:** Requires AWS credentials in `.env`:
```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
FINLOOM_S3_BUCKET=your-bucket-name
```

---

### 04_generate_dashboard.py

**Purpose:** Generate analytics dashboard and reports

**Usage:**
```bash
python scripts/04_generate_dashboard.py
```

**What it does:**
- Generates HTML dashboard
- Creates summary statistics
- Generates charts and visualizations
- Exports CSV reports

**Schedule:** Run daily (recommended: after daily_update.py)

---

## Operational Scripts

### backup_manager.py

**Purpose:** Manual backup management and restoration

**Usage:**
```bash
# Create backup
python scripts/backup_manager.py create

# List backups
python scripts/backup_manager.py list

# Restore backup
python scripts/backup_manager.py restore <backup_id>
```

---

### normalize_all.py

**Purpose:** Batch normalize all XBRL facts to standardized metrics

**Usage:**
```bash
python scripts/normalize_all.py
```

**What it does:**
- Normalizes all filings
- Applies concept mappings
- Handles currency conversions
- Validates results

**When to use:**
- After updating concept mappings
- After fixing normalization bugs
- During initial setup

---

### reparse_all.py

**Purpose:** Re-parse all filings with updated parser logic

**Usage:**
```bash
python scripts/reparse_all.py --parser xbrl
python scripts/reparse_all.py --parser section
```

**What it does:**
- Re-runs parsers on existing filings
- Updates extracted data
- Preserves original raw files

**When to use:**
- After parser bug fixes
- After parser enhancements
- When adding new extraction features

---

### seed_mappings.py

**Purpose:** Initialize concept mapping rules

**Usage:**
```bash
python scripts/seed_mappings.py
```

**What it does:**
- Loads concept mapping rules from config
- Populates `concept_mappings` table
- Validates mapping completeness

**When to use:**
- During initial setup
- After adding new concepts
- After updating mapping rules

---

## Extraction Scripts

### extraction/extract_unstructured.py

**Purpose:** Extract unstructured data (sections, tables, footnotes, chunks)

**Usage:**
```bash
# Extract all unprocessed filings
python scripts/extraction/extract_unstructured.py --all

# Extract specific filing
python scripts/extraction/extract_unstructured.py --accession 0000320193-23-000077

# Extract specific company
python scripts/extraction/extract_unstructured.py --ticker AAPL
```

---

### extraction/extract_with_staging.py

**Purpose:** Extract with staging (safe, transactional)

**Usage:**
```bash
python scripts/extraction/extract_with_staging.py --accession 0000320193-23-000077
```

**What it does:**
- Extracts to staging tables
- Validates extraction quality
- Merges to production tables (transactional)
- Rolls back on error

---

### extraction/validate_extraction.py

**Purpose:** Validate extraction quality

**Usage:**
```bash
python scripts/extraction/validate_extraction.py
```

**What it does:**
- Checks extraction completeness
- Validates data integrity
- Reports quality metrics
- Identifies issues

---

### extraction/check_data_health.py

**Purpose:** Check data health and consistency

**Usage:**
```bash
python scripts/extraction/check_data_health.py
```

**What it does:**
- Checks for orphaned records
- Validates foreign keys
- Checks for duplicates
- Reports data quality issues

**Alternative:** Use `finloom status --verify-integrity` for comprehensive verification

---

## Analysis Scripts

### analysis/assess_quality.py

**Purpose:** Assess data quality and extraction accuracy

**Usage:**
```bash
python scripts/analysis/assess_quality.py
```

**What it does:**
- Calculates quality scores
- Identifies low-quality extractions
- Reports on extraction issues
- Generates quality report

---

### analysis/validate_normalized.py

**Purpose:** Validate normalized financial data

**Usage:**
```bash
python scripts/analysis/validate_normalized.py
```

**What it does:**
- Validates normalization logic
- Checks for mapping errors
- Identifies outliers
- Reports validation results

---

### analysis/view_normalized.py

**Purpose:** View normalized financial metrics

**Usage:**
```bash
# View all normalized data
python scripts/analysis/view_normalized.py

# View specific company
python scripts/analysis/view_normalized.py --ticker AAPL

# View specific metric
python scripts/analysis/view_normalized.py --metric revenue
```

---

### analysis/compare_companies.py

**Purpose:** Compare financial metrics across companies

**Usage:**
```bash
python scripts/analysis/compare_companies.py --tickers AAPL,MSFT,GOOGL
python scripts/analysis/compare_companies.py --tickers AAPL,MSFT --metric revenue
```

---

### analysis/view_company.py

**Purpose:** View detailed company information

**Usage:**
```bash
python scripts/analysis/view_company.py --ticker AAPL
```

**What it does:**
- Shows company metadata
- Lists all filings
- Shows extraction statistics
- Displays quality metrics

---

## Utility Scripts

### quick_check.py

**Purpose:** Quick system health check

**Usage:**
```bash
python scripts/quick_check.py
```

**What it does:**
- Checks database connectivity
- Shows basic statistics
- Verifies data integrity
- Reports system status

**Alternative:** Use `finloom status` for more comprehensive status

---

### perf_test.py

**Purpose:** Performance testing and benchmarking

**Usage:**
```bash
python scripts/perf_test.py
```

**What it does:**
- Benchmarks database queries
- Tests extraction performance
- Measures processing throughput
- Generates performance report

---

## Best Practices

### 1. Use CLI Commands When Available

Many operations that were previously scripts are now built into the `finloom` CLI:

```bash
# System status and verification
python finloom.py status
python finloom.py status --verify-integrity

# Recovery operations
python finloom.py recovery reprocess

# Database maintenance
python finloom.py db detect-duplicates
python finloom.py db clean-duplicates --execute

# Monitoring
python finloom.py monitor start
python finloom.py monitor stop
```

### 2. Production Deployment

For production deployments:
1. Use cron/systemd for scheduling
2. Implement proper logging
3. Add error notifications
4. Monitor execution time
5. Set up alerting

### 3. Error Handling

All scripts should:
- Exit with non-zero code on error
- Log errors with stack traces
- Clean up resources (close connections)
- Support dry-run mode when applicable

### 4. Testing

Before running in production:
1. Test on dev database
2. Use dry-run mode
3. Verify output
4. Check for side effects

---

## Scheduling Production Scripts

### Recommended Crontab

```bash
# FinLoom Production Schedule

# Daily update (6 AM ET, after SEC updates)
0 6 * * * cd /path/to/FinLoom-2026 && python scripts/02_daily_update.py >> logs/daily_update.log 2>&1

# Backup to S3 (7 AM ET, after daily update)
0 7 * * * cd /path/to/FinLoom-2026 && python scripts/03_backup_to_s3.py >> logs/backup.log 2>&1

# Generate dashboard (8 AM ET, after backup)
0 8 * * * cd /path/to/FinLoom-2026 && python scripts/04_generate_dashboard.py >> logs/dashboard.log 2>&1

# Weekly data quality check (Sunday 2 AM)
0 2 * * 0 cd /path/to/FinLoom-2026 && python finloom.py status --verify-integrity >> logs/health_check.log 2>&1
```

### Systemd Timer (Alternative)

Create systemd service and timer files for more robust scheduling.

---

## Troubleshooting

### Script Fails with Import Error

**Problem:** `ModuleNotFoundError` or `ImportError`

**Solution:**
1. Activate virtual environment: `source venv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt`
3. Check PYTHONPATH includes project root

### Script Fails with Database Error

**Problem:** `database is locked` or connection error

**Solution:**
1. Check no other processes accessing database
2. Verify database file permissions
3. Ensure database path is correct in config

### Script Runs But Produces No Output

**Problem:** Script completes but doesn't do anything

**Solution:**
1. Check filters/arguments (e.g., `--ticker` filter)
2. Verify database has data
3. Check logs for warnings
4. Run with `--dry-run` to see what would be processed

---

## Future Enhancements

Potential improvements to the scripts system:

1. **Unified Script Runner**
   ```bash
   python finloom.py script run 02_daily_update
   python finloom.py script list
   python finloom.py script schedule --cron "0 6 * * *" 02_daily_update
   ```

2. **Script Monitoring Dashboard**
   - Track execution times
   - Monitor success/failure rates
   - Alert on failures
   - Show last run timestamps

3. **Dependency Graph**
   - Visualize script dependencies
   - Automatic execution ordering
   - Parallel execution where safe

4. **Integration Testing**
   - End-to-end test suite for scripts
   - Automated testing on PR
   - Production smoke tests

---

## Getting Help

- **Documentation:** See `docs/ARCHITECTURE.md`
- **CLI Help:** `python finloom.py --help`
- **Issues:** Check logs in `logs/` directory
- **Support:** Open GitHub issue with script name and error message
