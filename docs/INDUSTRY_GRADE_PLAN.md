# Industry-Grade Data Pipeline Improvements

## Executive Summary

This plan addresses data quality issues in the **Unstructured Data System** while establishing industry-standard patterns that prevent future issues.

**Current Issues:**
| Issue | Impact | Root Cause |
|-------|--------|------------|
| 421 duplicate sections | Bad data quality, inflated metrics | No idempotent writes |
| 2.3GB database (should be ~500MB) | Slow queries, wasted storage | Duplicates + no cleanup |
| 144 filings missing tables | Incomplete financial data | Pipeline not run/failed |
| 101 filings missing chunks | RAG/search gaps | Pipeline not run/failed |

---

## Recommended Architecture: Idempotent Pipeline with Staging

### Why This Approach?

**Industry Standard Patterns Used:**
1. **Idempotent Operations** - Safe to re-run without creating duplicates
2. **Staging Tables** - Isolate writes, validate before commit
3. **Transactional Merges** - Atomic DELETE + INSERT (no partial states)
4. **Write-Ahead Pattern** - Write to staging first, merge later
5. **Single Writer Principle** - Avoid DuckDB lock contention

**How Other Systems Solve This:**
- **Snowflake/BigQuery**: MERGE statement (DELETE + INSERT in one operation)
- **dbt**: Full refresh models with DROP + CREATE
- **Spark**: Write partitions, then atomic swap
- **Kafka Connect**: Upsert mode with primary keys

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│     (Already Working - No Changes Needed)                        │
│     SEC API → Downloader → Raw Files                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    EXTRACTION LAYER                              │
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │  Section    │   │   Table     │   │  Footnote   │           │
│  │  Parser     │ → │   Parser    │ → │   Parser    │           │
│  │  (Fixed ✅) │   │  (Exists)   │   │  (Exists)   │           │
│  └─────────────┘   └─────────────┘   └─────────────┘           │
│                              ↓                                   │
│                    ┌─────────────┐                              │
│                    │  Semantic   │                              │
│                    │  Chunker    │                              │
│                    └─────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              STAGING LAYER (NEW - Industry Grade)                │
│                                                                  │
│  Each parallel worker writes to isolated staging tables:        │
│                                                                  │
│  Worker 1 → sections_staging_1, tables_staging_1, ...           │
│  Worker 2 → sections_staging_2, tables_staging_2, ...           │
│  Worker N → sections_staging_N, tables_staging_N, ...           │
│                                                                  │
│  Benefits:                                                       │
│  ✓ Zero write contention (each worker isolated)                 │
│  ✓ Can validate data before committing                          │
│  ✓ Can rollback if issues detected                              │
│  ✓ Parallel processing without locks                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              MERGE COORDINATOR (NEW - Single Writer)             │
│                                                                  │
│  Single-threaded process that:                                   │
│                                                                  │
│  1. BEGIN TRANSACTION                                            │
│  2. DELETE existing data for filing (idempotent)                │
│  3. INSERT FROM staging tables                                   │
│  4. UPDATE filings.sections_processed = TRUE                    │
│  5. COMMIT                                                       │
│  6. TRUNCATE staging tables                                      │
│                                                                  │
│  Benefits:                                                       │
│  ✓ Atomic operations (no partial states)                        │
│  ✓ Idempotent (safe to re-run)                                  │
│  ✓ Single writer (no lock contention)                           │
│  ✓ Clean rollback on failure                                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    PRODUCTION TABLES                             │
│                                                                  │
│  sections (1,518 rows → deduplicated)                           │
│  tables (40,749 rows → deduplicated + new extractions)          │
│  footnotes (46,014 rows)                                         │
│  chunks (27,729 rows → new extractions)                         │
│                                                                  │
│  Database size: ~500MB (down from 2.3GB)                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Database Cleanup & Foundation (Day 1)

**Goal:** Clean existing data, establish baseline

**Tasks:**
1. **Backup database** (safety net)
2. **Remove duplicates** from sections table
3. **Remove duplicates** from tables table
4. **VACUUM database** to reclaim space
5. **Verify data integrity** after cleanup

**SQL Pattern (Idempotent Deduplication):**
```sql
-- Keep only the latest/best record for each unique combination
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY accession_number, section_type
               ORDER BY id DESC  -- Keep latest
           ) as rn
    FROM sections
)
DELETE FROM sections
WHERE id IN (SELECT id FROM ranked WHERE rn > 1);
```

**Expected Result:**
- Database size: 2.3GB → ~600MB
- Sections: 1,518 → ~1,000 (unique)
- No duplicate records

---

### Phase 2: Staging Infrastructure (Day 1-2)

**Goal:** Create staging tables and coordinator

**New Files:**
```
src/storage/
├── staging_schema.sql      # Staging table definitions
├── staging_manager.py      # Create/drop staging tables dynamically
└── merge_coordinator.py    # Single-writer merge logic

scripts/
└── extract_with_staging.py # New extraction script using staging
```

**Staging Schema Design:**
```sql
-- Dynamic staging tables (created per-run, not permanent)
CREATE TABLE sections_staging_{run_id} AS SELECT * FROM sections WHERE 1=0;
CREATE TABLE tables_staging_{run_id} AS SELECT * FROM tables WHERE 1=0;
CREATE TABLE footnotes_staging_{run_id} AS SELECT * FROM footnotes WHERE 1=0;
CREATE TABLE chunks_staging_{run_id} AS SELECT * FROM chunks WHERE 1=0;
```

**Merge Coordinator Logic:**
```python
class MergeCoordinator:
    def merge_filing(self, accession_number: str, run_id: str):
        """Atomically merge staging data to production."""
        conn = duckdb.connect(self.db_path)

        try:
            conn.execute("BEGIN TRANSACTION")

            # Step 1: Delete existing (idempotent)
            conn.execute(f"DELETE FROM chunks WHERE accession_number = ?", [accession_number])
            conn.execute(f"DELETE FROM footnotes WHERE accession_number = ?", [accession_number])
            conn.execute(f"DELETE FROM tables WHERE accession_number = ?", [accession_number])
            conn.execute(f"DELETE FROM sections WHERE accession_number = ?", [accession_number])

            # Step 2: Insert from staging
            conn.execute(f"INSERT INTO sections SELECT * FROM sections_staging_{run_id} WHERE accession_number = ?", [accession_number])
            conn.execute(f"INSERT INTO tables SELECT * FROM tables_staging_{run_id} WHERE accession_number = ?", [accession_number])
            conn.execute(f"INSERT INTO footnotes SELECT * FROM footnotes_staging_{run_id} WHERE accession_number = ?", [accession_number])
            conn.execute(f"INSERT INTO chunks SELECT * FROM chunks_staging_{run_id} WHERE accession_number = ?", [accession_number])

            # Step 3: Update status
            conn.execute("""
                UPDATE filings
                SET sections_processed = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE accession_number = ?
            """, [accession_number])

            conn.execute("COMMIT")

        except Exception as e:
            conn.execute("ROLLBACK")
            raise
```

---

### Phase 3: Pipeline Integration (Day 2-3)

**Goal:** Modify pipeline to use staging + coordinator

**Changes to `UnstructuredDataPipeline`:**
1. Add `run_id` parameter for staging table suffix
2. Write to staging tables instead of production
3. Return extracted data without committing

**New Extraction Flow:**
```python
# scripts/extract_with_staging.py

def process_all_filings(filings: list, parallel: int = 4):
    run_id = generate_run_id()  # e.g., "20240125_143022"

    # Step 1: Create staging tables
    staging_manager.create_staging_tables(run_id)

    # Step 2: Parallel extraction to staging (no contention)
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = [
            executor.submit(extract_to_staging, filing, run_id)
            for filing in filings
        ]
        wait(futures)

    # Step 3: Sequential merge (single writer, idempotent)
    coordinator = MergeCoordinator(db_path)
    for filing in filings:
        coordinator.merge_filing(filing.accession_number, run_id)

    # Step 4: Cleanup staging
    staging_manager.drop_staging_tables(run_id)
```

---

### Phase 4: Extract Missing Data (Day 3-4)

**Goal:** Fill gaps in tables and chunks

**After staging infrastructure is ready:**
1. Run extraction for 144 filings missing tables
2. Run extraction for 101 filings missing chunks
3. Verify 100% coverage

**Verification Queries:**
```sql
-- All filings should have sections
SELECT COUNT(*) FROM filings f
LEFT JOIN sections s ON f.accession_number = s.accession_number
WHERE s.id IS NULL;  -- Should be 0

-- All filings should have tables
SELECT COUNT(*) FROM filings f
LEFT JOIN tables t ON f.accession_number = t.accession_number
WHERE t.id IS NULL;  -- Should be 0

-- No duplicates
SELECT accession_number, section_type, COUNT(*)
FROM sections
GROUP BY 1, 2
HAVING COUNT(*) > 1;  -- Should return 0 rows
```

---

### Phase 5: Monitoring & Safeguards (Day 4-5)

**Goal:** Prevent future issues

**Add Data Quality Checks:**
```python
class DataQualityChecker:
    def check_no_duplicates(self, table: str, unique_cols: list[str]) -> bool:
        """Verify no duplicates exist in table."""
        ...

    def check_referential_integrity(self) -> bool:
        """Verify foreign key relationships."""
        ...

    def check_completeness(self) -> dict:
        """Return coverage statistics."""
        ...
```

**Add Pre-Commit Hooks to Coordinator:**
```python
def merge_filing(self, accession_number: str, run_id: str):
    # Validate BEFORE committing
    if not self.validate_staging_data(accession_number, run_id):
        raise ValidationError("Staging data failed validation")

    # Only then proceed with merge
    ...
```

**Add Monitoring Dashboard Queries:**
```sql
-- Daily health check view
CREATE VIEW data_health AS
SELECT
    (SELECT COUNT(DISTINCT accession_number) FROM sections) as filings_with_sections,
    (SELECT COUNT(DISTINCT accession_number) FROM tables) as filings_with_tables,
    (SELECT COUNT(*) FROM filings) as total_filings,
    (SELECT pg_size_pretty(pg_database_size('finloom'))) as db_size;
```

---

## File Changes Summary

| File | Action | Purpose |
|------|--------|---------|
| `src/storage/staging_schema.sql` | NEW | Staging table definitions |
| `src/storage/staging_manager.py` | NEW | Dynamic staging table management |
| `src/storage/merge_coordinator.py` | NEW | Single-writer merge logic |
| `src/processing/unstructured_pipeline.py` | MODIFY | Support staging writes |
| `src/validation/data_quality.py` | MODIFY | Add duplicate checks |
| `scripts/extract_with_staging.py` | NEW | Main extraction script |
| `scripts/cleanup_duplicates.py` | NEW | One-time cleanup script |

---

## Success Criteria

| Metric | Before | After |
|--------|--------|-------|
| Database size | 2.3 GB | ~500 MB |
| Duplicate sections | 421 | 0 |
| Filings with tables | 69/213 | 213/213 |
| Filings with chunks | 112/213 | 213/213 |
| Safe to re-run extraction | ❌ Creates duplicates | ✅ Idempotent |
| Parallel processing | ❌ Lock contention | ✅ Via staging |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Data loss during cleanup | Full backup before any changes |
| Merge failures | Transaction rollback, staging preserved |
| Staging table bloat | Auto-cleanup after successful merge |
| Parallel conflicts | Worker-isolated staging tables |

---

## Questions Before Implementation

1. **Backup Strategy**: Do you have a preferred backup location/method?
2. **Downtime**: Is it OK to have brief read-only periods during cleanup?
3. **Priority**: Should we prioritize tables (financial data) or chunks (RAG) after cleanup?
4. **Parallel Workers**: How many parallel workers should we support (4, 8, 10)?
5. **Retention**: Should we keep staging tables for debugging, or auto-delete?

---

## Recommendation

**I recommend implementing in this order:**

1. **Phase 1** (Cleanup) - Immediate value, fixes data quality
2. **Phase 2** (Staging) - Foundation for safe operations
3. **Phase 3** (Integration) - Enable idempotent pipeline
4. **Phase 4** (Fill Gaps) - Complete data coverage
5. **Phase 5** (Monitoring) - Prevent future issues

This approach gives you **quick wins first** (cleanup) while building toward a **robust long-term solution** (staging architecture).
