# FinLoom System Architecture

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
                        │                                                  │
                        │  ┌────────────────┐    ┌────────────────────┐   │
                        │  │  XBRL Parser   │    │  Section Parser    │   │
                        │  │  (Structured)  │    │  (Unstructured)    │   │
                        │  │                │    │                    │   │
                        │  │ • Facts        │    │ • Item 1 (Business)│   │
                        │  │ • Concepts     │    │ • Item 1A (Risks)  │   │
                        │  │ • Periods      │    │ • Item 7 (MD&A)    │   │
                        │  │ • Units        │    │ • Item 8 (Financials)│ │
                        │  └───────┬────────┘    └─────────┬──────────┘   │
                        │          │                       │              │
                        │          │    ┌─────────────┐    │              │
                        │          │    │Table Parser │    │              │
                        │          │    │ • 50+ tables│    │              │
                        │          │    └──────┬──────┘    │              │
                        └──────────┼───────────┼───────────┼──────────────┘
                                   │           │           │
                                   ▼           ▼           ▼
                        ┌─────────────────────────────────────────────────┐
                        │               STORAGE LAYER                      │
                        │                                                  │
                        │  ┌──────────────────────────────────────────┐   │
                        │  │            REPOSITORY PATTERN             │   │
                        │  │  ┌────────────┐  ┌────────────────────┐  │   │
                        │  │  │ Filing Repo│  │ Fact Repository    │  │   │
                        │  │  ├────────────┤  ├────────────────────┤  │   │
                        │  │  │Company Repo│  │ Section Repository │  │   │
                        │  │  └────────────┘  └────────────────────┘  │   │
                        │  └──────────────────────────────────────────┘   │
                        │                        │                        │
                        │                        ▼                        │
                        │  ┌──────────────────────────────────────────┐   │
                        │  │              DUCKDB DATABASE              │   │
                        │  │  • 20 Companies  • 343,900 Facts         │   │
                        │  │  • 213 Filings   • 838 Sections          │   │
                        │  │  • 11.6M Words   • 76M Characters        │   │
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
│  │  ├── sec_api.py   │  │  ├── xbrl_parser  │  │  ├── database.py  │      │
│  │  └── downloader   │  │  ├── section_parser│ │  ├── repositories │      │
│  │                   │  │  ├── table_parser │  │  └── schema.sql   │      │
│  │  • SEC API client │  │  └── footnote_parser│ │                   │      │
│  │  • File downloads │  │                   │  │  • DuckDB ops     │      │
│  │  • Rate limiting  │  │  • HTML parsing   │  │  • CRUD repos     │      │
│  └───────────────────┘  │  • XBRL extraction│  │  • Connection pool│      │
│                         └───────────────────┘  └───────────────────┘      │
│                                      │                                      │
│                    ┌─────────────────┼─────────────────┐                   │
│                    ▼                 ▼                 ▼                   │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐      │
│  │    BUSINESS       │  │    VALIDATION     │  │    PROCESSING     │      │
│  │  src/business/    │  │  src/validation/  │  │  src/processing/  │      │
│  │  └── concept_mapper│ │  ├── schemas.py   │  │  ├── unstructured │      │
│  │                   │  │  ├── data_quality │  │  └── semantic_chunk│     │
│  │  • XBRL→Standard  │  │  └── quality_scorer│ │                   │      │
│  │  • Normalization  │  │                   │  │  • Text chunking  │      │
│  │  • Mapping rules  │  │  • Pydantic models│  │  • RAG prep       │      │
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
│  │ company_name    │         │ accession_number│──┐                        │
│  │ ticker          │         │ form_type       │  │                        │
│  │ sic_code        │         │ filing_date     │  │                        │
│  │ fiscal_year_end │         │ primary_document│  │                        │
│  └─────────────────┘         └─────────────────┘  │                        │
│                                                   │                        │
│         ┌─────────────────────────────────────────┼────────────────┐       │
│         │                                         │                │       │
│         ▼                                         ▼                ▼       │
│  ┌─────────────────┐         ┌─────────────────┐  │  ┌─────────────────┐   │
│  │     FACTS       │         │   SECTIONS      │  │  │    TABLES       │   │
│  ├─────────────────┤         ├─────────────────┤  │  ├─────────────────┤   │
│  │ id (PK)         │         │ id (PK)         │  │  │ id (PK)         │   │
│  │ accession_number│◀────────│ accession_number│──┘  │ accession_number│   │
│  │ concept_name    │         │ section_type    │     │ table_index     │   │
│  │ concept_namespace│        │ section_title   │     │ table_type      │   │
│  │ value           │         │ content_text    │     │ markdown        │   │
│  │ unit            │         │ word_count      │     │ json_data       │   │
│  │ period_start    │         │ paragraph_count │     └─────────────────┘   │
│  │ period_end      │         │ extraction_conf │                           │
│  │ dimensions      │         └─────────────────┘                           │
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
