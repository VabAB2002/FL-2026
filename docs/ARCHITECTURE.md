# FinLoom System Architecture

> **Note**: This system has been simplified to a markdown-only architecture for unstructured data.
> Section/table extraction parsers have been removed in favor of storing complete documents as markdown.
> This ensures 100% data capture across all SEC filing formats without parsing failures.

## 1. High-Level System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FINLOOM SYSTEM                                  │
│                    SEC Financial Data Extraction Platform                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  SEC EDGAR  │───▶│  INGESTION  │───▶│   PARSERS   │───▶│   STORAGE   │  │
│  │     API     │    │   LAYER     │    │    LAYER    │    │    LAYER    │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                  │          │
│                                                                  ▼          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │    CLI      │◀───│  BUSINESS   │◀───│ VALIDATION  │◀───│   DUCKDB    │  │
│  │  INTERFACE  │    │    LOGIC    │    │    LAYER    │    │  DATABASE   │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         CROSS-CUTTING CONCERNS                        │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  │  │
│  │  │  Config  │  │  Logging │  │ Caching  │  │Monitoring│  │  Core  │  │  │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 2. Data Flow Pipeline

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           DATA EXTRACTION PIPELINE                            │
└──────────────────────────────────────────────────────────────────────────────┘

    ┌─────────┐         ┌─────────────────────────────────────────────────┐
    │   SEC   │         │              INGESTION LAYER                     │
    │  EDGAR  │────────▶│  ┌─────────┐  ┌────────────┐  ┌──────────────┐  │
    │   API   │         │  │ SEC API │─▶│ Downloader │─▶│ Rate Limiter │  │
    └─────────┘         │  └─────────┘  └────────────┘  └──────────────┘  │
                        └───────────────────────┬─────────────────────────┘
                                                │
                                                ▼
                        ┌─────────────────────────────────────────────────┐
                        │               PARSING LAYER                      │
                        │          (Markdown-Only Architecture)            │
                        │  ┌────────────────┐    ┌────────────────────┐   │
                        │  │  XBRL Parser   │    │ Unstructured Lib   │   │
                        │  │  (Structured)  │    │  (Markdown Only)   │   │
                        │  │                │    │                    │   │
                        │  │ • All Facts    │    │ • Full Markdown    │   │
                        │  │ • 1,131+ items │    │ • Section Markers  │   │
                        │  │ • Segments     │    │ • 100% Capture     │   │
                        │  │ • Hierarchy    │    │ • RAG Ready        │   │
                        │  └───────┬────────┘    └─────────┬──────────┘   │
                        │          │                       │              │
                        └──────────┼───────────────────────┼──────────────┘
                                   │                       │
                                   ▼                       ▼
                        ┌─────────────────────────────────────────────────┐
                        │               STORAGE LAYER                      │
                        │                                                  │
                        │  ┌──────────────────────────────────────────┐   │
                        │  │            REPOSITORY PATTERN             │   │
                        │  │  ┌────────────┐  ┌────────────────────┐  │   │
                        │  │  │ Filing Repo│  │ Fact Repository    │  │   │
                        │  │  ├────────────┤  ├────────────────────┤  │   │
                        │  │  │Company Repo│  │  (Markdown Only)   │  │   │
                        │  │  └────────────┘  └────────────────────┘  │   │
                        │  └──────────────────────────────────────────┘   │
                        │                        │                        │
                        │                        ▼                        │
                        │  ┌──────────────────────────────────────────┐   │
                        │  │              DUCKDB DATABASE              │   │
                        │  │  • 20 Companies  • 343,900 Facts         │   │
                        │  │  • 213 Filings   • Full Markdown         │   │
                        │  │  • 100% Data Capture • RAG Ready         │   │
                        │  └──────────────────────────────────────────┘   │
                        └─────────────────────────────────────────────────┘
```

## 3. Module Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            src/ MODULE STRUCTURE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                          CORE LAYER                                  │   │
│  │  src/core/                                                          │   │
│  │  ├── exceptions.py    # 18 exception types (FinLoomError hierarchy)│   │
│  │  ├── repository.py    # Repository protocols (FilingRepo, FactRepo)│   │
│  │  └── types.py         # Type aliases (CIK, AccessionNumber, etc.)  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                    ┌─────────────────┼─────────────────┐                   │
│                    ▼                 ▼                 ▼                   │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐      │
│  │    INGESTION      │  │     PARSERS       │  │     STORAGE       │      │
│  │  src/ingestion/   │  │   src/parsers/    │  │   src/storage/    │      │
│  │  ├── sec_api.py   │  │  └── xbrl_parser  │  │  ├── database.py  │      │
│  │  └── downloader   │  │                   │  │  ├── repositories │      │
│  │                   │  │  • XBRL parsing   │  │  ├── s3_backup    │      │
│  │  • SEC API client │  │  • All facts      │  │  └── schema.sql   │      │
│  │  • File downloads │  │  • Segments       │  │                   │      │
│  │  • Rate limiting  │  │  • 1,131+ items   │  │  • DuckDB ops     │      │
│  └───────────────────┘  │                   │  │  • CRUD repos     │      │
│                         └───────────────────┘  │  • Connection pool│      │
│                                                └───────────────────┘      │
│                                      │                                      │
│                    ┌─────────────────┼─────────────────┐                   │
│                    ▼                 ▼                 ▼                   │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐      │
│  │    BUSINESS       │  │    VALIDATION     │  │    PROCESSING     │      │
│  │  src/business/    │  │  src/validation/  │  │  src/processing/  │      │
│  │  └── concept_mapper│ │  ├── schemas.py   │  │  └── unstructured │      │
│  │                   │  │  ├── data_quality │  │     _pipeline     │      │
│  │  • XBRL→Standard  │  │  └── quality_scorer│ │                   │      │
│  │  • Normalization  │  │                   │  │  • Markdown only  │      │
│  │  • Mapping rules  │  │  • Pydantic models│  │  • RAG ready      │      │
│  └───────────────────┘  │  • Quality checks │  └───────────────────┘      │
│                         └───────────────────┘                              │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      INFRASTRUCTURE LAYER                            │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │src/config/  │  │ src/utils/  │  │src/caching/ │  │src/monitoring│ │  │
│  │  │• env_config │  │ • logger    │  │• redis_cache│  │• health      │ │  │
│  │  │• settings   │  │ • config    │  │• query_cache│  │• tracing     │ │  │
│  │  │             │  │ • rate_limit│  │             │  │• circuit_brkr│ │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 4. Database Schema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DUCKDB SCHEMA (18 tables)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐         ┌─────────────────┐                           │
│  │   COMPANIES     │         │    FILINGS      │                           │
│  ├─────────────────┤         ├─────────────────┤                           │
│  │ cik (PK)        │◀────────│ cik (FK)        │                           │
│  │ company_name    │         │ accession_number│                           │
│  │ ticker          │         │ form_type       │                           │
│  │ sic_code        │         │ filing_date     │                           │
│  │ fiscal_year_end │         │ primary_document│                           │
│  └─────────────────┘         │ full_markdown   │  ← MARKDOWN STORAGE       │
│                               │ markdown_count  │                           │
│                               └─────────────────┘                           │
│                                       │                                     │
│                                       ▼                                     │
│  ┌─────────────────┐                                                       │
│  │     FACTS       │         Note: sections/tables/footnotes               │
│  ├─────────────────┤         tables removed - data is in markdown          │
│  │ id (PK)         │                                                       │
│  │ accession_number│                                                       │
│  │ concept_name    │                                                       │
│  │ concept_namespace│                                                      │
│  │ value           │                                                       │
│  │ unit            │                                                       │
│  │ period_start    │                                                       │
│  │ period_end      │                                                       │
│  │ dimensions      │                                                       │
│  └─────────────────┘                                                       │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      DERIVED/LOOKUP TABLES                           │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │concept_mappings│ │normalized_   │ │standardized_ │               │   │
│  │  │              │  │financials    │  │metrics       │               │   │
│  │  │• concept→std │  │• revenue     │  │• ratio calcs │               │   │
│  │  │• categories  │  │• net_income  │  │• benchmarks  │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  │                                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │data_quality_ │  │processing_   │  │ footnotes    │               │   │
│  │  │issues        │  │logs          │  │              │               │   │
│  │  │• validation  │  │• audit trail │  │• references  │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 5. Repository Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          REPOSITORY PATTERN                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    PROTOCOLS (src/core/repository.py)                │   │
│  │                                                                      │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │   │
│  │  │ FilingRepository│  │ FactRepository  │  │CompanyRepository│      │   │
│  │  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤      │   │
│  │  │ get_filing()    │  │ get_facts()     │  │ get_company()   │      │   │
│  │  │ save_filing()   │  │ save_facts()    │  │ save_company()  │      │   │
│  │  │ list_filings()  │  │ get_for_filing()│  │ list_companies()│      │   │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘      │   │
│  └───────────┼────────────────────┼────────────────────┼────────────────┘   │
│              │                    │                    │                    │
│              ▼                    ▼                    ▼                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │              IMPLEMENTATIONS (src/storage/repositories.py)           │   │
│  │                                                                      │   │
│  │  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐  │   │
│  │  │DuckDBFilingRepo   │  │DuckDBFactRepo     │  │DuckDBCompanyRepo│  │   │
│  │  └─────────┬─────────┘  └─────────┬─────────┘  └────────┬────────┘  │   │
│  │            │                      │                     │           │   │
│  │            └──────────────────────┼─────────────────────┘           │   │
│  │                                   ▼                                 │   │
│  │                        ┌─────────────────────┐                      │   │
│  │                        │   Database Class    │                      │   │
│  │                        │  (Connection Pool)  │                      │   │
│  │                        └──────────┬──────────┘                      │   │
│  │                                   │                                 │   │
│  │                                   ▼                                 │   │
│  │                        ┌─────────────────────┐                      │   │
│  │                        │      DuckDB         │                      │   │
│  │                        │   finloom.duckdb    │                      │   │
│  │                        └─────────────────────┘                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  SINGLETON ACCESS:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  get_filing_repository()  →  DuckDBFilingRepository (cached)        │   │
│  │  get_fact_repository()    →  DuckDBFactRepository (cached)          │   │
│  │  get_company_repository() →  DuckDBCompanyRepository (cached)       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 6. Exception Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXCEPTION HIERARCHY                                  │
│                      (src/core/exceptions.py)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                           FinLoomError                                       │
│                               │                                              │
│         ┌─────────────────────┼─────────────────────┐                       │
│         │                     │                     │                       │
│         ▼                     ▼                     ▼                       │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │IngestionError│     │ ParsingError │     │ StorageError │                │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│         │                    │                    │                        │
│    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐                   │
│    ▼         ▼          ▼         ▼          ▼         ▼                   │
│ SECApiError  Download  XBRL      Section   Database  Connection            │
│    │         Error     Parsing   Parsing   Error     Error                  │
│    ▼                   Error     Error                                      │
│ RateLimitError                                                              │
│                                                                             │
│         ┌─────────────────────┼─────────────────────┐                       │
│         │                     │                     │                       │
│         ▼                     ▼                     ▼                       │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │ValidationError│    │ProcessingError│    │ ConfigError  │                │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│         │                    │                    │                        │
│    ┌────┴────┐               ▼               ┌────┴────┐                   │
│    ▼         ▼         PipelineError         ▼                             │
│  Schema   DataQuality                   MissingConfig                       │
│  Error    Error                         Error                               │
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐                                     │
│  │  CacheError  │     │MonitoringError│                                    │
│  └──────┬───────┘     └──────────────┘                                     │
│         │                                                                   │
│         ▼                                                                   │
│  CacheConnectionError                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 7. Data Statistics

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CURRENT DATA STATISTICS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  COMPANIES (20)                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │ AAPL  AMD   AMZN  BAC   BRK-B  CSCO  DIS   GOOGL  GS   HD         │    │
│  │ IBM   INTC  JPM   META  MSFT   NVDA  ORCL  TSLA   WFC  WMT        │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  STRUCTURED DATA (XBRL)          │  UNSTRUCTURED DATA (TEXT)               │
│  ┌────────────────────────────┐  │  ┌────────────────────────────┐        │
│  │ Facts:        343,900      │  │  │ Sections:        838       │        │
│  │ Concepts:     ~2,000+      │  │  │ Words:           11.6M     │        │
│  │ Per Company:  ~17,000 avg  │  │  │ Characters:      76M       │        │
│  └────────────────────────────┘  │  └────────────────────────────┘        │
│                                  │                                         │
│  FILINGS                         │  COVERAGE                               │
│  ┌────────────────────────────┐  │  ┌────────────────────────────┐        │
│  │ Total:        213          │  │  │ Years:    ~10 per company  │        │
│  │ Form Type:    10-K         │  │  │ Sections: Item 1,1A,7,8,9A │        │
│  │ Per Company:  ~10 avg      │  │  │ Tables:   50+ per filing   │        │
│  └────────────────────────────┘  │  └────────────────────────────┘        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 8. Technology Stack

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TECHNOLOGY STACK                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  LANGUAGE & RUNTIME                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Python 3.13  │  Type Hints  │  Pydantic  │  Dataclasses           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  DATA PROCESSING                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  DuckDB (OLAP)  │  Pandas  │  BeautifulSoup  │  Arelle (XBRL)      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  EXTERNAL SERVICES                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  SEC EDGAR API  │  Redis (Caching)  │  Prometheus (Metrics)        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  INFRASTRUCTURE                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  FastAPI (Health)  │  OpenTelemetry (Tracing)  │  AWS S3 (Backup)  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  DEVELOPMENT                                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Black  │  Ruff  │  Pytest  │  Pre-commit  │  Mypy                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 8. Error Handling and Recovery

### 8.1 Section Extraction Validation

The UnstructuredDataPipeline includes critical validation to prevent incomplete data:

**Validation Checks:**
1. **Empty Sections Check**: Rejects filings that produce zero sections
2. **Priority Sections Check**: Ensures key sections (Item 1, 1A, 7, 8) are present
3. **Processing Flag**: Only sets `sections_processed=TRUE` for valid extractions

**Example:**
```python
# Pipeline validates after extraction
if not sections or len(sections) == 0:
    return ProcessingResult(success=False, error_message="No sections extracted")

# Check for priority sections
PRIORITY_SECTIONS = {'item_1', 'item_1a', 'item_7', 'item_8'}
if not extracted_section_types.intersection(PRIORITY_SECTIONS):
    return ProcessingResult(success=False, error_message="No priority sections")
```

### 8.2 Recovery System

For filings that failed extraction or need reprocessing:

**Built-in Recovery Method:**
```python
from src.processing.unstructured_pipeline import UnstructuredDataPipeline

pipeline = UnstructuredDataPipeline(db_path)
result = pipeline.reprocess_filing(
    accession_number="0000320193-23-000077",
    filing_path=Path("data/raw/320193/0000320193-23-000077"),
    force=True  # Reprocess even if already has sections
)
```

**CLI Recovery Command:**
```bash
# Find and reprocess orphaned filings (processed but 0 sections)
python finloom.py recovery reprocess

# Dry run to see what would be reprocessed
python finloom.py recovery reprocess --dry-run

# Reprocess specific company
python finloom.py recovery reprocess --ticker AAPL

# Force reprocess even if has sections
python finloom.py recovery reprocess --force

# Include all failed (sections_processed=FALSE)
python finloom.py recovery reprocess --all
```

**Recovery Process:**
1. Identifies filings needing reprocessing (orphaned or failed)
2. Deletes existing data (sections, tables, footnotes, chunks)
3. Resets `sections_processed=FALSE`
4. Re-extracts using fixed parser
5. Validates results before marking complete

**Idempotent Operations:**
- Safe to run multiple times
- Uses DELETE + INSERT pattern
- Transactional (all-or-nothing)
- Production-ready error handling

### 8.3 Duplicate Management

The Database class includes built-in duplicate detection and removal:

**Duplicate Detection:**
```python
from src.storage.database import Database

db = Database()
duplicates = db.detect_duplicates("normalized_financials")

for dup in duplicates:
    print(f"{dup['ticker']} {dup['year']} {dup['metric']}: {dup['count']} entries")
    # Each duplicate group includes details of all records
```

**Duplicate Removal:**
```python
# Dry run (preview only)
stats = db.remove_duplicates("normalized_financials", dry_run=True)

# Actually delete duplicates (keeps best: highest confidence, most recent)
stats = db.remove_duplicates("normalized_financials", dry_run=False)
```

**CLI Commands:**
```bash
# Detect and display duplicates
python finloom.py db detect-duplicates --table normalized_financials

# Preview what would be deleted (dry run)
python finloom.py db clean-duplicates --table normalized_financials

# Actually delete duplicates
python finloom.py db clean-duplicates --table normalized_financials --execute
```

**Duplicate Detection Logic:**
- For `normalized_financials`: Groups by (ticker, year, quarter, metric_id)
- Keeps record with highest `confidence_score`
- If tied, keeps most recent `created_at`
- Uses transactions (safe rollback on error)
- Logs all operations

### 8.4 System Verification and Health Checks

The `DatabaseHealthChecker` class provides comprehensive system verification:

**System Integrity Verification:**
```python
from src.monitoring.health_checker import DatabaseHealthChecker

checker = DatabaseHealthChecker(db_path)
report = checker.verify_system_integrity()

# Report includes:
# - Database schema validation
# - Extraction progress statistics
# - Top processed companies
# - Quality metrics (confidence scores)
# - Metadata features (tables, lists, parts)
# - Hierarchical chunking distribution
# - Database health (size, read-only status)
```

**CLI Command:**
```bash
# Basic system status
python finloom.py status

# Comprehensive system verification (production readiness check)
python finloom.py status --verify-integrity
```

**Verification Checks:**
1. **Schema**: Validates all required tables exist
2. **Extraction Progress**: Shows processing rates and coverage
3. **Top Companies**: Lists companies with most processed filings
4. **Quality Metrics**: Average confidence scores, min/max ranges
5. **Features**: Sections with tables, lists, part labels
6. **Chunking**: Hierarchical chunk distribution (Section/Topic/Paragraph)
7. **Database Health**: Size, read-only status, integrity

**Health Status Levels:**
- `healthy` ✅ - System is production ready
- `warning` ⚠️ - Has warnings, review recommended
- `critical` ❌ - Has issues, fixes required

### 8.5 Migration from Workaround Scripts

**Deprecated Scripts (Removed):**
- `scripts/fix_missing_sections.py` → Use `finloom recovery reprocess`
- `scripts/clean_duplicates.py` → Use `finloom db clean-duplicates --execute`
- `scripts/verify_system.py` → Use `finloom status --verify-integrity`

The root causes that necessitated workaround scripts have been fixed:
- ✅ Pipeline now validates section extraction
- ✅ Built-in recovery method with proper error handling
- ✅ Built-in duplicate detection and removal in Database class
- ✅ Built-in comprehensive system verification
- ✅ CLI commands for all operational use
- ✅ Metrics and logging integration
- ✅ Transaction support for data safety
- ✅ Production-ready health checks

## 9. File Structure

```
FinLoom-2026/
├── finloom.py                 # CLI entry point
├── pyproject.toml             # Project config & tool settings
├── Makefile                   # Development commands
├── requirements.txt           # Dependencies
│
├── src/
│   ├── core/                  # Domain layer
│   │   ├── exceptions.py      # Exception hierarchy (18 types)
│   │   ├── repository.py      # Repository protocols
│   │   └── types.py           # Type aliases
│   │
│   ├── ingestion/             # Data ingestion
│   │   ├── sec_api.py         # SEC EDGAR API client
│   │   └── downloader.py      # Filing downloader
│   │
│   ├── parsers/               # Document parsing
│   │   ├── xbrl_parser.py     # XBRL fact extraction
│   │   ├── section_parser.py  # 10-K section extraction
│   │   ├── table_parser.py    # Table extraction
│   │   └── footnote_parser.py # Footnote extraction
│   │
│   ├── storage/               # Data persistence
│   │   ├── database.py        # DuckDB operations
│   │   ├── repositories.py    # Repository implementations
│   │   └── schema.sql         # Database schema
│   │
│   ├── business/              # Business logic
│   │   └── concept_mapper.py  # XBRL→Standard mapping
│   │
│   ├── validation/            # Data validation
│   │   ├── schemas.py         # Pydantic models
│   │   └── data_quality.py    # Quality checks
│   │
│   ├── processing/            # Text processing
│   │   └── unstructured_pipeline.py
│   │
│   ├── caching/               # Redis caching
│   │   └── redis_cache.py
│   │
│   ├── monitoring/            # Observability
│   │   ├── health.py          # Health checks
│   │   └── tracing.py         # OpenTelemetry
│   │
│   ├── config/                # Configuration
│   │   └── env_config.py
│   │
│   └── utils/                 # Utilities
│       ├── config.py          # Unified config
│       ├── logger.py          # Logging
│       └── rate_limiter.py
│
├── data/
│   ├── database/
│   │   └── finloom.duckdb     # Main database (20 companies)
│   └── raw/                   # Downloaded filings
│
├── scripts/                   # Utility scripts
│   ├── analysis/              # Analysis scripts
│   ├── extraction/            # Extraction scripts
│   └── archive/               # Archived scripts
│
└── docs/                      # Documentation
    └── ARCHITECTURE.md        # This file
```

## 11. XBRL Extraction Modes

FinLoom supports two XBRL extraction modes, controlled by the `extract_all_xbrl_facts` configuration flag:

### Core Mode (extract_all_xbrl_facts: false)

**Purpose**: Fast extraction of essential financial metrics for quick analysis and dashboards.

**Coverage**:
- 94 core US-GAAP concepts
- Balance sheet fundamentals (Assets, Liabilities, Equity)
- Income statement essentials (Revenue, Net Income, EPS)
- Cash flow primary items
- No segment/dimensional data
- No hierarchy metadata

**Use Cases**:
- Quick financial summaries
- Cross-company comparisons
- Dashboard metrics
- Rapid prototyping

**Performance**: ~50-100 facts per filing, <5 second parse time

### Full Mode (extract_all_xbrl_facts: true) ✅ DEFAULT

**Purpose**: Comprehensive extraction for in-depth analysis, RAG queries, and detailed research.

**Coverage**:
- 1,131+ facts per filing (all XBRL concepts)
- All core concepts PLUS:
  - **Segment Analysis**: Revenue/income by geography and product line
  - **Detailed Schedules**: Debt maturities, lease obligations, tax reconciliations
  - **Fair Value Hierarchies**: Investment classifications and valuations
  - **Disclosures**: Risk factors, commitments, contingencies
  - **Hierarchy Metadata**: section, parent_concept, depth, labels
  - **Dimensional Data**: Multi-axis breakdowns (geography × product, etc.)

**Use Cases**:
- RAG/LLM question answering ("What percentage of revenue comes from China?")
- Detailed financial modeling
- Segment performance analysis
- Regulatory compliance research
- Academic research

**Performance**: 1,000-1,500 facts per filing, ~10 second parse time

### Configuration

Set in `config/settings.yaml`:

```yaml
extraction:
  extract_all_xbrl_facts: true  # true = full mode (default), false = core mode
```

Or via environment variable:

```bash
export FINLOOM_EXTRACT_ALL_FACTS=true
```

### Comparison Example (Apple Inc. 10-K 2025)

| Metric | Core Mode | Full Mode |
|--------|-----------|-----------|
| **Facts Extracted** | 94 | 1,131 |
| **Parse Time** | ~4s | ~9s |
| **Sections Identified** | 0 | 15+ |
| **Has Labels** | No | Yes (90%+) |
| **Has Hierarchy** | No | Yes |
| **Segment Data** | No | Yes |
| **Database Size** | ~5KB/filing | ~50KB/filing |

### Implementation Details

**Parser Configuration** (automatic):

```python
from src.parsers.xbrl_parser import XBRLParser
from src.utils.config import get_settings

settings = get_settings()
parser = XBRLParser(extract_all_facts=settings.extraction.extract_all_xbrl_facts)
```

**Fact Enrichment** (Full Mode Only):

When `extract_all_facts=True`, the parser:
1. Parses presentation linkbase (`*_pre.xml`) for hierarchy
2. Parses label linkbase (`*_lab.xml`) for human-readable labels
3. Enriches each fact with:
   - `section`: Financial statement or note category
   - `parent_concept`: Parent in concept hierarchy
   - `label`: Human-readable name
   - `depth`: Indentation level in hierarchy

### Data Quality

Both modes include validation:
- Balance sheet equation checks
- Required concept verification
- Value range validation
- **Full mode adds**: Fact count completeness checks (expects 500+ facts)

### Storage Impact

- **Core Mode**: ~94 facts × 3 periods × 20 companies = ~5,600 rows
- **Full Mode**: ~1,200 facts × 3 periods × 20 companies = ~72,000 rows
- **Database Growth**: ~13x increase (still trivial for DuckDB)
- **Query Performance**: No degradation (proper indexes in place)

### Recommendation

**Use Full Mode (default)** for:
- Production RAG systems
- Comprehensive financial analysis
- Research applications
- Any use case requiring segment data or detailed disclosures

**Use Core Mode** only for:
- Development/testing with minimal data
- Simple dashboard applications
- Performance-constrained environments
