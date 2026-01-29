-- FinLoom SEC Data Pipeline Database Schema
-- DuckDB database for storing extracted SEC filing data
-- ==================================================

-- Companies: Master list of tracked companies
CREATE TABLE IF NOT EXISTS companies (
    cik VARCHAR PRIMARY KEY,
    company_name VARCHAR NOT NULL,
    ticker VARCHAR,
    sic_code VARCHAR,
    sic_description VARCHAR,
    state_of_incorporation VARCHAR,
    fiscal_year_end VARCHAR,
    category VARCHAR,
    ein VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Create index for ticker lookup
CREATE INDEX IF NOT EXISTS idx_companies_ticker ON companies(ticker);

-- Filings: Filing metadata and processing status
CREATE TABLE IF NOT EXISTS filings (
    accession_number VARCHAR PRIMARY KEY,
    cik VARCHAR NOT NULL,
    form_type VARCHAR NOT NULL,
    filing_date DATE NOT NULL,
    period_of_report DATE,
    acceptance_datetime TIMESTAMP,
    
    -- Document information
    primary_document VARCHAR,
    primary_doc_description VARCHAR,
    is_xbrl BOOLEAN DEFAULT FALSE,
    is_inline_xbrl BOOLEAN DEFAULT FALSE,
    
    -- Storage locations
    edgar_url VARCHAR,
    local_path VARCHAR,
    
    -- Processing status
    download_status VARCHAR DEFAULT 'pending',
    xbrl_processed BOOLEAN DEFAULT FALSE,
    sections_processed BOOLEAN DEFAULT FALSE,
    
    -- Unstructured extraction (full markdown)
    full_markdown TEXT,
    markdown_word_count INTEGER,
    
    -- Error tracking
    processing_errors JSON,
    
    -- Timestamps
    created_at TIMESTAMP ,
    updated_at TIMESTAMP 
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings(cik);
CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type);
CREATE INDEX IF NOT EXISTS idx_filings_filing_date ON filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_filings_period ON filings(period_of_report);
CREATE INDEX IF NOT EXISTS idx_filings_status ON filings(download_status);

-- Filing Sections: Structured section data extracted by sec2md
CREATE TABLE IF NOT EXISTS filing_sections (
    id INTEGER PRIMARY KEY,
    accession_number VARCHAR NOT NULL,
    item VARCHAR NOT NULL,              -- "1", "1A", "7", etc.
    item_title VARCHAR,                 -- "Business", "Risk Factors"
    markdown TEXT NOT NULL,
    word_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (accession_number) REFERENCES filings(accession_number),
    UNIQUE(accession_number, item)
);

CREATE INDEX IF NOT EXISTS idx_sections_accession ON filing_sections(accession_number);
CREATE INDEX IF NOT EXISTS idx_sections_item ON filing_sections(item);
CREATE SEQUENCE IF NOT EXISTS filing_sections_id_seq START 1;

-- Facts: XBRL financial data (star schema design)
CREATE TABLE IF NOT EXISTS facts (
    id INTEGER PRIMARY KEY,
    accession_number VARCHAR NOT NULL,
    
    -- XBRL concept identification
    concept_name VARCHAR NOT NULL,          -- e.g., 'us-gaap:Assets'
    concept_namespace VARCHAR,              -- e.g., 'us-gaap'
    concept_local_name VARCHAR,             -- e.g., 'Assets'
    
    -- Value and unit
    value DECIMAL(20, 4),                   -- Numeric value
    value_text TEXT,                        -- For non-numeric (text) facts
    unit VARCHAR,                           -- USD, shares, pure, etc.
    decimals INTEGER,                       -- Decimal precision
    
    -- Time period context
    period_type VARCHAR,                    -- 'instant' or 'duration'
    period_start DATE,                      -- For duration periods
    period_end DATE,                        -- Period end (or instant date)
    
    -- Dimensional context (for segment reporting)
    dimensions JSON,                        -- {axis: member} pairs
    
    -- Metadata
    is_custom BOOLEAN DEFAULT FALSE,        -- Custom extension concept
    is_negated BOOLEAN DEFAULT FALSE,       -- Sign flipped in presentation
    
    -- Hierarchy and categorization (for RAG)
    section VARCHAR,                        -- e.g., 'IncomeStatement', 'FinancialInstruments'
    parent_concept VARCHAR,                 -- Parent concept in hierarchy
    label VARCHAR,                          -- Human-readable label
    depth INTEGER,                          -- Hierarchy depth for indentation
    
    created_at TIMESTAMP 
);

-- Concept Categories: Master reference for concept hierarchy and metadata
CREATE TABLE IF NOT EXISTS concept_categories (
    concept_name VARCHAR PRIMARY KEY,       -- e.g., 'us-gaap:Assets'
    section VARCHAR,                        -- e.g., 'BalanceSheet', 'IncomeStatement'
    subsection VARCHAR,                     -- e.g., 'CurrentAssets', 'RevenueDetails'
    parent_concept VARCHAR,                 -- Parent in hierarchy
    depth INTEGER,                          -- Hierarchy depth
    label VARCHAR,                          -- Human-readable label
    data_type VARCHAR,                      -- 'monetary', 'shares', 'pure', 'string'
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Index for concept category lookups
CREATE INDEX IF NOT EXISTS idx_concept_categories_section ON concept_categories(section);

-- Indexes for fact queries
CREATE INDEX IF NOT EXISTS idx_facts_accession ON facts(accession_number);
CREATE INDEX IF NOT EXISTS idx_facts_concept ON facts(concept_name);
CREATE INDEX IF NOT EXISTS idx_facts_period_end ON facts(period_end);
CREATE INDEX IF NOT EXISTS idx_facts_namespace ON facts(concept_namespace);
CREATE INDEX IF NOT EXISTS idx_facts_section ON facts(section);
CREATE INDEX IF NOT EXISTS idx_facts_label ON facts(label);

-- UNIQUE constraint to prevent duplicate facts
-- Same concept, period, and dimensions for a filing should be unique
CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_unique 
ON facts(accession_number, concept_name, period_end, COALESCE(dimensions::VARCHAR, 'NULL'));

-- Sequence for facts ID (DuckDB doesn't have auto-increment)
CREATE SEQUENCE IF NOT EXISTS facts_id_seq START 1;

-- Note: Sections, tables, and footnotes tables removed in markdown-only architecture.
-- All unstructured data is now stored in filings.full_markdown column.

-- Chunks: Semantic chunks for RAG
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id VARCHAR PRIMARY KEY,
    accession_number VARCHAR NOT NULL,
    section_id INTEGER,
    parent_chunk_id VARCHAR,
    
    chunk_level INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    chunk_markdown TEXT,
    
    token_count INTEGER,
    char_start INTEGER,
    char_end INTEGER,
    
    heading VARCHAR,
    section_type VARCHAR,
    contains_tables BOOLEAN DEFAULT FALSE,
    contains_lists BOOLEAN DEFAULT FALSE,
    contains_numbers BOOLEAN DEFAULT FALSE,
    
    cross_references JSON,
    
    s3_path VARCHAR,
    embedding_vector FLOAT[],
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (accession_number) REFERENCES filings(accession_number),
    FOREIGN KEY (section_id) REFERENCES sections(id)
);

-- Indexes for chunks queries
CREATE INDEX IF NOT EXISTS idx_chunks_accession ON chunks(accession_number);
CREATE INDEX IF NOT EXISTS idx_chunks_section ON chunks(section_id);
CREATE INDEX IF NOT EXISTS idx_chunks_level ON chunks(chunk_level);
CREATE INDEX IF NOT EXISTS idx_chunks_parent ON chunks(parent_chunk_id);
CREATE INDEX IF NOT EXISTS idx_chunks_section_type ON chunks(section_type);

-- Processing logs: Track pipeline operations
CREATE TABLE IF NOT EXISTS processing_logs (
    id INTEGER PRIMARY KEY,
    accession_number VARCHAR,
    cik VARCHAR,
    
    -- Operation details
    pipeline_stage VARCHAR NOT NULL,        -- download, xbrl_parse, section_extract, etc.
    operation VARCHAR,                      -- Specific operation name
    status VARCHAR NOT NULL,                -- started, completed, failed, skipped
    
    -- Timing
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    processing_time_ms INTEGER,
    
    -- Results and errors
    records_processed INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    error_traceback TEXT,
    
    -- Context
    context JSON,                           -- Additional context data
    
    created_at TIMESTAMP 
);

-- Indexes for log queries
CREATE INDEX IF NOT EXISTS idx_logs_accession ON processing_logs(accession_number);
CREATE INDEX IF NOT EXISTS idx_logs_stage ON processing_logs(pipeline_stage);
CREATE INDEX IF NOT EXISTS idx_logs_status ON processing_logs(status);
CREATE INDEX IF NOT EXISTS idx_logs_created ON processing_logs(created_at);

-- Sequence for logs ID
CREATE SEQUENCE IF NOT EXISTS processing_logs_id_seq START 1;

-- Audit log: Immutable log of all data modifications (for compliance)
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    
    -- Who/What
    user_id VARCHAR,
    service_name VARCHAR DEFAULT 'finloom',
    correlation_id VARCHAR,
    
    -- Action details
    action VARCHAR NOT NULL,                -- SELECT, INSERT, UPDATE, DELETE
    table_name VARCHAR NOT NULL,
    record_id VARCHAR,
    
    -- Changes
    old_value JSON,
    new_value JSON,
    
    -- Context
    ip_address VARCHAR,
    user_agent VARCHAR,
    query_text TEXT,
    
    -- Result
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    
    created_at TIMESTAMP
);

-- Indexes for audit queries (immutable table, append-only)
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_table ON audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_correlation ON audit_log(correlation_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);

-- Sequence for audit log ID
CREATE SEQUENCE IF NOT EXISTS audit_log_id_seq START 1;

-- Data quality issues: Track validation failures
CREATE TABLE IF NOT EXISTS data_quality_issues (
    id INTEGER PRIMARY KEY,
    accession_number VARCHAR,
    
    -- Issue details
    issue_type VARCHAR NOT NULL,            -- missing_field, invalid_value, etc.
    severity VARCHAR NOT NULL,              -- error, warning, info
    field_name VARCHAR,
    
    -- Description
    message TEXT NOT NULL,
    expected_value VARCHAR,
    actual_value VARCHAR,
    
    -- Resolution
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    
    created_at TIMESTAMP 
);

-- Indexes for quality issues
CREATE INDEX IF NOT EXISTS idx_quality_accession ON data_quality_issues(accession_number);
CREATE INDEX IF NOT EXISTS idx_quality_type ON data_quality_issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_quality_resolved ON data_quality_issues(resolved);

-- Sequence for quality issues ID
CREATE SEQUENCE IF NOT EXISTS data_quality_issues_id_seq START 1;

-- Views for common queries

-- View: Latest filing per company
CREATE OR REPLACE VIEW latest_filings AS
SELECT 
    f.*,
    c.company_name,
    c.ticker
FROM filings f
JOIN companies c ON f.cik = c.cik
WHERE f.filing_date = (
    SELECT MAX(f2.filing_date) 
    FROM filings f2 
    WHERE f2.cik = f.cik AND f2.form_type = f.form_type
);

-- View: Processing status summary
CREATE OR REPLACE VIEW processing_summary AS
SELECT 
    c.ticker,
    c.company_name,
    f.form_type,
    COUNT(*) as total_filings,
    SUM(CASE WHEN f.download_status = 'completed' THEN 1 ELSE 0 END) as downloaded,
    SUM(CASE WHEN f.xbrl_processed THEN 1 ELSE 0 END) as xbrl_processed,
    SUM(CASE WHEN f.sections_processed THEN 1 ELSE 0 END) as sections_processed,
    MIN(f.filing_date) as earliest_filing,
    MAX(f.filing_date) as latest_filing
FROM filings f
JOIN companies c ON f.cik = c.cik
GROUP BY c.ticker, c.company_name, f.form_type
ORDER BY c.ticker, f.form_type;

-- View: Key financial metrics by company/period
CREATE OR REPLACE VIEW key_financials AS
SELECT 
    c.ticker,
    c.company_name,
    f.accession_number,
    f.period_of_report,
    MAX(CASE WHEN fa.concept_name = 'us-gaap:Assets' THEN fa.value END) as total_assets,
    MAX(CASE WHEN fa.concept_name = 'us-gaap:Liabilities' THEN fa.value END) as total_liabilities,
    MAX(CASE WHEN fa.concept_name = 'us-gaap:StockholdersEquity' THEN fa.value END) as equity,
    MAX(CASE WHEN fa.concept_name IN ('us-gaap:Revenues', 'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax') THEN fa.value END) as revenue,
    MAX(CASE WHEN fa.concept_name = 'us-gaap:NetIncomeLoss' THEN fa.value END) as net_income
FROM filings f
JOIN companies c ON f.cik = c.cik
LEFT JOIN facts fa ON f.accession_number = fa.accession_number 
    AND fa.dimensions IS NULL  -- Exclude segment data
    AND fa.period_type = 'instant' OR (fa.period_type = 'duration' AND fa.period_end = f.period_of_report)
GROUP BY c.ticker, c.company_name, f.accession_number, f.period_of_report
ORDER BY c.ticker, f.period_of_report DESC;

-- ==================================================
-- NORMALIZATION LAYER (Bloomberg-style)
-- ==================================================

-- Standardized metric definitions (the "Bloomberg fields")
CREATE TABLE IF NOT EXISTS standardized_metrics (
    metric_id VARCHAR PRIMARY KEY,
    metric_name VARCHAR NOT NULL,           -- 'revenue', 'net_income', etc.
    display_label VARCHAR NOT NULL,         -- 'Total Revenue', 'Net Income'
    category VARCHAR NOT NULL,              -- 'income_statement', 'balance_sheet', etc.
    data_type VARCHAR,                      -- 'monetary', 'shares', 'ratio', 'percentage'
    description TEXT,
    calculation_rule TEXT,                  -- For derived metrics (JSON)
    created_at TIMESTAMP
);

-- Concept to standardized metric mappings
CREATE TABLE IF NOT EXISTS concept_mappings (
    mapping_id INTEGER PRIMARY KEY,
    metric_id VARCHAR NOT NULL,
    concept_name VARCHAR NOT NULL,          -- XBRL concept like 'us-gaap:Assets'
    priority INTEGER NOT NULL,              -- 1 = try first, 2 = fallback, etc.
    confidence_score DECIMAL(3,2),          -- 0.0 to 1.0
    applies_to_industry VARCHAR,            -- NULL = all, or specific SIC codes
    notes TEXT,
    created_at TIMESTAMP,
    UNIQUE(metric_id, concept_name)
);

CREATE INDEX IF NOT EXISTS idx_mappings_metric ON concept_mappings(metric_id);
CREATE INDEX IF NOT EXISTS idx_mappings_concept ON concept_mappings(concept_name);
CREATE SEQUENCE IF NOT EXISTS concept_mappings_id_seq START 1;

-- Normalized company metrics (the cross-company comparable data)
CREATE TABLE IF NOT EXISTS normalized_financials (
    id INTEGER PRIMARY KEY,
    company_ticker VARCHAR NOT NULL,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,                -- NULL for annual, 1-4 for quarterly
    metric_id VARCHAR NOT NULL,
    metric_value DECIMAL(20, 4),
    source_concept VARCHAR,                 -- Which XBRL concept this came from
    source_accession VARCHAR,               -- Filing accession number
    confidence_score DECIMAL(3,2),          -- How confident we are in this value
    created_at TIMESTAMP,
    UNIQUE(company_ticker, fiscal_year, fiscal_quarter, metric_id)
);

CREATE INDEX IF NOT EXISTS idx_norm_ticker_year ON normalized_financials(company_ticker, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_norm_metric ON normalized_financials(metric_id);
CREATE INDEX IF NOT EXISTS idx_norm_accession ON normalized_financials(source_accession);
CREATE SEQUENCE IF NOT EXISTS normalized_financials_id_seq START 1;

-- Industry classifications and templates
CREATE TABLE IF NOT EXISTS industry_templates (
    template_id VARCHAR PRIMARY KEY,
    template_name VARCHAR NOT NULL,         -- 'technology', 'banking', 'retail'
    sic_codes TEXT,                         -- JSON array of applicable SIC codes
    key_metrics TEXT,                       -- JSON array of important metric_ids
    display_sections TEXT,                  -- JSON config for display
    created_at TIMESTAMP
);

-- View: Normalized metrics with company info
CREATE OR REPLACE VIEW normalized_metrics_view AS
SELECT 
    n.company_ticker,
    c.company_name,
    c.sic_code,
    n.fiscal_year,
    n.fiscal_quarter,
    n.metric_id,
    s.display_label as metric_label,
    s.category as metric_category,
    n.metric_value,
    n.source_concept,
    n.confidence_score
FROM normalized_financials n
JOIN companies c ON n.company_ticker = c.ticker
JOIN standardized_metrics s ON n.metric_id = s.metric_id
ORDER BY n.company_ticker, n.fiscal_year DESC, n.metric_id;
