
import duckdb
from pathlib import Path

db_path = "data/database/finloom.duckdb"
conn = duckdb.connect(db_path, read_only=True)

query = """
    SELECT accession_number, filing_date, form_type, local_path, sections_processed
    FROM filings 
    WHERE cik = '320193' 
      AND EXTRACT(YEAR FROM filing_date) = 2024
      AND form_type = '10-K'
"""

results = conn.execute(query).fetchall()
print(f"Found {len(results)} filings for AAPL 2024 10-K:")
for r in results:
    print(r)
