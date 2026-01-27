# FinLoom Database Duplicate Analysis Summary

**Date:** January 27, 2026  
**Database:** `data/database/finloom.duckdb` (1,483.51 MB)

## Executive Summary

The duplicate analysis revealed **two main issues** in the structured database:

1. **20 groups of duplicate filings** (all 20 companies have 6-11 duplicate 10-K filings each)
2. **12,808 duplicate fact groups** (16,217 total duplicate records)

**Good News:**
- ✅ No duplicates in: `companies`, `sections`, `chunks`, `normalized_financials`
- ✅ All primary keys are unique (no ID conflicts)

---

## Detailed Findings

### 1. Companies Table ✅
- **Total records:** 20
- **Status:** CLEAN - No duplicates found

### 2. Filings Table ⚠️
- **Total records:** 213 filings
- **Issue:** 20 groups of duplicate filings (same CIK, form type, and filing date)
- **Root cause:** Appears to be from bulk data import or reprocessing

**Example duplicates:**
- **AAPL**: 11 duplicate 10-K filings from 2014-2024 (all filed on 2026-01-24)
- **MSFT**: 11 duplicate 10-K filings from 2014-2024 (all filed on 2026-01-24)
- **GOOGL**: 9 duplicate 10-K filings from 2016-2024 (all filed on 2026-01-24)

**Problem:** Each company's historical filings (from different years) all show the same filing date (Jan 24-25, 2026), which is clearly incorrect. This suggests the data was bulk-imported and the `filing_date` was set to the import date rather than the actual SEC filing date.

### 3. Facts Table ⚠️
- **Total records:** 343,900 facts
- **Issue:** 12,808 duplicate groups (16,217 records to remove)
- **Root cause:** XBRL reprocessing likely created duplicate facts

**Critical Finding:**
- ⚠️ **Some duplicate facts have DIFFERENT values** for the same concept/period
- This indicates a data quality issue requiring careful review
- Example: Disney's Revenue for Q4 2019 appears 8 times with two different values: $69.57B and $19.10B

**Top duplicates:**
- Many companies have 8x duplicates for key metrics like `NetIncomeLoss` and `Revenues`
- Most duplicates are for the same filing but different reporting periods

### 4. Sections Table ✅
- **Total records:** 3,852 sections
- **Status:** CLEAN - No duplicates found

### 5. Chunks Table ℹ️
- **Status:** Table is empty (no data to check)

### 6. Normalized Financials Table ✅
- **Total records:** 6,178 normalized metrics
- **Status:** CLEAN - No duplicates found

---

## Root Cause Analysis

### Filing Duplicates
The filing duplicates appear to be caused by:
1. **Bulk historical data import** where all historical filings were imported on the same date
2. The `filing_date` field was set to the import date (Jan 24-25, 2026) instead of the actual SEC filing date
3. Each company has 6-11 years of 10-K filings, all showing the same filing date

### Fact Duplicates
The fact duplicates likely occurred due to:
1. **XBRL reprocessing** without proper deduplication
2. Multiple extractions of the same filing
3. The presence of different values for some duplicates suggests potential data quality issues in the XBRL parsing logic

---

## Recommendations

### Immediate Actions

1. **STOP** - Do not run cleanup scripts until reviewing the data quality issues
2. **Investigate** - Review why some duplicate facts have different values
3. **Backup** - Create a backup before any cleanup:
   ```bash
   cp data/database/finloom.duckdb data/database/finloom_pre_cleanup_20260127.duckdb
   ```

### Data Quality Issues to Resolve First

Before cleaning duplicates, investigate:

1. **Filing dates** - Why are all historical filings showing import date?
   - Should these be using `acceptance_datetime` or `period_of_report` instead?
   
2. **Fact value conflicts** - Why do some duplicate facts have different values?
   - Example: Disney Revenue Q4 2019: $69.57B vs $19.10B
   - This suggests incorrect XBRL parsing or dimension handling

3. **Filing selection** - Which filing to keep for each duplicate group?
   - Current recommendation: Keep the FIRST accession number (MIN)
   - But should we keep the most recent or most complete filing instead?

### Cleanup Strategy (After Investigation)

#### Option A: Conservative Approach (Recommended)
1. Fix the underlying data quality issues first
2. Re-import or reprocess affected filings correctly
3. Then deduplicate with confidence

#### Option B: Aggressive Cleanup (Higher Risk)
1. Create backup
2. Run fact deduplication (keep lowest ID)
3. Run filing deduplication (keep MIN accession per company)
4. Verify results
5. Add UNIQUE constraints to prevent future duplicates

---

## Cleanup Scripts (DO NOT RUN YET)

### Script 1: Fact Duplicates Cleanup

```sql
-- Create a CTE to identify keepers (lowest ID per group)
WITH duplicate_groups AS (
    SELECT 
        accession_number,
        concept_name,
        period_end,
        COALESCE(dimensions::VARCHAR, 'NULL') as dims,
        MIN(id) as keeper_id
    FROM facts
    GROUP BY 
        accession_number, 
        concept_name, 
        period_end, 
        COALESCE(dimensions::VARCHAR, 'NULL')
    HAVING COUNT(*) > 1
),
facts_to_delete AS (
    SELECT f.id
    FROM facts f
    JOIN duplicate_groups dg 
        ON f.accession_number = dg.accession_number
        AND f.concept_name = dg.concept_name
        AND f.period_end = dg.period_end
        AND COALESCE(f.dimensions::VARCHAR, 'NULL') = dg.dims
    WHERE f.id != dg.keeper_id
)
DELETE FROM facts
WHERE id IN (SELECT id FROM facts_to_delete);
```

### Script 2: Filing Duplicates Cleanup

```sql
-- Step 1: Identify which filings to keep (keep the first accession number)
CREATE TEMP TABLE filings_to_keep AS
SELECT MIN(accession_number) as keeper_accession
FROM filings
GROUP BY cik, form_type, filing_date;

-- Step 2: Delete orphaned facts for filings we'll remove
DELETE FROM facts
WHERE accession_number NOT IN (SELECT keeper_accession FROM filings_to_keep);

-- Step 3: Delete orphaned sections
DELETE FROM sections
WHERE accession_number NOT IN (SELECT keeper_accession FROM filings_to_keep);

-- Step 4: Delete duplicate filings
DELETE FROM filings
WHERE accession_number NOT IN (SELECT keeper_accession FROM filings_to_keep);

-- Step 5: Cleanup
DROP TABLE filings_to_keep;
```

---

## Prevention: Add UNIQUE Constraints

After cleanup, add these constraints to prevent future duplicates:

```sql
-- Prevent duplicate filings
-- Note: Can't use (cik, form_type, filing_date) as some filings may legitimately 
-- share these attributes (amendments). Consider (cik, form_type, period_of_report) instead.

-- Prevent duplicate facts
CREATE UNIQUE INDEX idx_facts_unique 
ON facts(accession_number, concept_name, period_end, 
         COALESCE(dimensions::VARCHAR, 'NULL'));
```

---

## Impact Assessment

### Storage Impact
- **Fact cleanup:** Will free up ~16.2 MB
- **Filing cleanup:** Will remove 193 duplicate filings (keeping 20, one per company)
- **Total space savings:** ~200-300 MB after vacuum

### Data Impact
- **Facts:** Will remove 16,217 duplicate fact records
- **Filings:** Will remove 193 duplicate filing records
- **Sections:** May remove sections associated with duplicate filings
- **Normalized Financials:** Should not be affected (already clean)

---

## Next Steps

1. ✅ **Completed:** Initial duplicate analysis
2. 🔍 **Next:** Investigate data quality issues
   - Why do some facts have different values?
   - Why are filing dates all set to import date?
3. 🔧 **Then:** Fix root causes
4. 🧹 **Finally:** Run cleanup with confidence

---

## Useful Commands

### Re-run duplicate check:
```bash
source venv/bin/activate
python scripts/quick_duplicate_check.py
```

### Generate detailed report:
```bash
source venv/bin/activate
python scripts/duplicate_report.py
```

### Inspect database:
```bash
source venv/bin/activate
python scripts/inspect_main_db.py
```

### Create backup:
```bash
cp data/database/finloom.duckdb data/database/finloom_backup_$(date +%Y%m%d_%H%M%S).duckdb
```

---

## Contact

For questions about this analysis, review the generated scripts in `/scripts/`:
- `quick_duplicate_check.py` - Fast duplicate scanner
- `duplicate_report.py` - Detailed analysis generator
- `inspect_main_db.py` - Database structure inspector
