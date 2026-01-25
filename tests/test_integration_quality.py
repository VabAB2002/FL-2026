"""
Integration tests for data quality validation.

Ensures data integrity across the system.
"""

import pytest

from src.storage.database import Database


class TestDataQualityIntegration:
    """Integration tests for data quality."""
    
    @pytest.fixture
    def db(self):
        """Database fixture."""
        database = Database()
        yield database
        database.close()
    
    def test_no_orphaned_facts(self, db):
        """Ensure all facts link to valid filings."""
        orphaned = db.connection.execute("""
            SELECT COUNT(*) FROM facts f
            LEFT JOIN filings fi ON f.accession_number = fi.accession_number
            WHERE fi.accession_number IS NULL
        """).fetchone()[0]
        
        assert orphaned == 0, f"Found {orphaned} orphaned facts"
    
    def test_no_orphaned_filings(self, db):
        """Ensure all filings link to valid companies."""
        orphaned = db.connection.execute("""
            SELECT COUNT(*) FROM filings f
            LEFT JOIN companies c ON f.cik = c.cik
            WHERE c.cik IS NULL
        """).fetchone()[0]
        
        assert orphaned == 0, f"Found {orphaned} orphaned filings"
    
    def test_no_duplicate_normalized_metrics(self, db):
        """Ensure normalized metrics are unique."""
        duplicates = db.connection.execute("""
            SELECT company_ticker, fiscal_year, fiscal_quarter, metric_id, COUNT(*)
            FROM normalized_financials
            GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
            HAVING COUNT(*) > 1
        """).fetchall()
        
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate metric groups"
    
    def test_balance_sheet_equation(self, db):
        """Test balance sheet equation for all filings."""
        violations = db.connection.execute("""
            WITH balance_sheet AS (
                SELECT 
                    f.accession_number,
                    c.ticker,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:Assets' AND fa.dimensions IS NULL
                        THEN fa.value END) as assets,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:Liabilities' AND fa.dimensions IS NULL
                        THEN fa.value END) as liabilities,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:StockholdersEquity' AND fa.dimensions IS NULL
                        THEN fa.value END) as equity
                FROM filings f
                JOIN companies c ON f.cik = c.cik
                JOIN facts fa ON f.accession_number = fa.accession_number
                WHERE f.xbrl_processed = TRUE
                GROUP BY f.accession_number, c.ticker
            )
            SELECT accession_number, ticker, assets, liabilities, equity
            FROM balance_sheet
            WHERE assets IS NOT NULL 
              AND liabilities IS NOT NULL 
              AND equity IS NOT NULL
              AND ABS((assets - (liabilities + equity)) / assets) > 0.01
        """).fetchall()
        
        if violations:
            print("\nBalance sheet violations:")
            for v in violations[:5]:
                print(f"  {v[1]}: {v[0]}")
        
        # Allow up to 5% violations (some companies use different reporting)
        max_allowed = db.connection.execute(
            "SELECT COUNT(DISTINCT accession_number) FROM filings WHERE xbrl_processed = TRUE"
        ).fetchone()[0] * 0.05
        
        assert len(violations) <= max_allowed, \
            f"Found {len(violations)} balance sheet violations (max allowed: {max_allowed:.0f})"
    
    def test_all_processed_filings_have_facts(self, db):
        """Ensure every processed filing has extracted facts."""
        filings_without_facts = db.connection.execute("""
            SELECT f.accession_number, c.ticker
            FROM filings f
            JOIN companies c ON f.cik = c.cik
            LEFT JOIN facts fa ON f.accession_number = fa.accession_number
            WHERE f.xbrl_processed = TRUE
            GROUP BY f.accession_number, c.ticker
            HAVING COUNT(fa.id) = 0
        """).fetchall()
        
        if filings_without_facts:
            print(f"\nFilings without facts: {len(filings_without_facts)}")
            for acc, ticker in filings_without_facts[:5]:
                print(f"  {ticker}: {acc}")
        
        # Allow some failures (malformed XBRL, etc.)
        total_processed = db.connection.execute(
            "SELECT COUNT(*) FROM filings WHERE xbrl_processed = TRUE"
        ).fetchone()[0]
        
        failure_rate = len(filings_without_facts) / total_processed if total_processed > 0 else 0
        assert failure_rate < 0.05, \
            f"Too many filings without facts: {failure_rate:.1%} ({len(filings_without_facts)}/{total_processed})"
    
    def test_normalized_metrics_have_source_facts(self, db):
        """Ensure normalized metrics link to valid source facts."""
        # Sample check (checking all would be slow)
        sample = db.connection.execute("""
            SELECT id, company_ticker, metric_id, source_accession, source_concept
            FROM normalized_financials
            WHERE source_accession IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 100
        """).fetchall()
        
        missing_sources = []
        for norm_id, ticker, metric_id, source_acc, source_concept in sample:
            exists = db.connection.execute("""
                SELECT COUNT(*) FROM facts
                WHERE accession_number = ? AND concept_name = ?
            """, [source_acc, source_concept]).fetchone()[0]
            
            if exists == 0:
                missing_sources.append((ticker, metric_id, source_acc))
        
        assert len(missing_sources) == 0, \
            f"Found {len(missing_sources)} normalized metrics with missing source facts"
    
    def test_fiscal_year_consistency(self, db):
        """Test that fiscal years are consistent."""
        # Check for filings with facts spanning multiple years
        inconsistent = db.connection.execute("""
            SELECT f.accession_number, c.ticker,
                   MIN(EXTRACT(YEAR FROM fa.period_end)) as min_year,
                   MAX(EXTRACT(YEAR FROM fa.period_end)) as max_year
            FROM filings f
            JOIN companies c ON f.cik = c.cik
            JOIN facts fa ON f.accession_number = fa.accession_number
            WHERE fa.period_end IS NOT NULL
            GROUP BY f.accession_number, c.ticker
            HAVING MAX(EXTRACT(YEAR FROM fa.period_end)) - MIN(EXTRACT(YEAR FROM fa.period_end)) > 1
        """).fetchall()
        
        # Some multi-year facts are expected (comparative data), but should be rare
        total_filings = db.connection.execute(
            "SELECT COUNT(*) FROM filings WHERE xbrl_processed = TRUE"
        ).fetchone()[0]
        
        inconsistent_rate = len(inconsistent) / total_filings if total_filings > 0 else 0
        
        # Allow up to 10% to have multi-year spans (comparative periods)
        assert inconsistent_rate < 0.10, \
            f"Too many filings with inconsistent fiscal years: {inconsistent_rate:.1%}"
    
    def test_required_metrics_coverage(self, db):
        """Test that companies have minimum required metrics."""
        required_metrics = [
            'revenue', 'net_income', 'total_assets',
            'total_liabilities', 'stockholders_equity'
        ]
        
        companies_with_gaps = []
        
        companies = db.connection.execute("""
            SELECT ticker FROM companies
        """).fetchall()
        
        for (ticker,) in companies:
            # Get metrics for most recent year
            metrics = db.connection.execute("""
                SELECT DISTINCT metric_id
                FROM normalized_financials
                WHERE company_ticker = ?
                  AND fiscal_year = (
                      SELECT MAX(fiscal_year)
                      FROM normalized_financials
                      WHERE company_ticker = ?
                  )
            """, [ticker, ticker]).fetchall()
            
            metric_ids = {m[0] for m in metrics}
            missing = set(required_metrics) - metric_ids
            
            if len(missing) > 2:  # Allow up to 2 missing
                companies_with_gaps.append((ticker, missing))
        
        assert len(companies_with_gaps) < 3, \
            f"Found {len(companies_with_gaps)} companies missing required metrics"
    
    def test_data_freshness(self, db):
        """Test that data is reasonably fresh."""
        from datetime import datetime
        
        latest_filing = db.connection.execute("""
            SELECT MAX(filing_date) FROM filings
        """).fetchone()[0]
        
        if latest_filing:
            from datetime import date
            if isinstance(latest_filing, str):
                latest_filing = datetime.strptime(latest_filing, '%Y-%m-%d').date()
            
            days_old = (date.today() - latest_filing).days
            
            # Data should be less than 120 days old (2 quarters)
            assert days_old < 120, \
                f"Data is stale: latest filing is {days_old} days old"


class TestCircuitBreaker:
    """Tests for circuit breaker functionality."""
    
    def test_circuit_breaker_opens_on_failures(self):
        """Test that circuit breaker opens after threshold failures."""
        from src.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
        
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        
        def failing_function():
            raise Exception("Test failure")
        
        # First 3 failures should pass through
        for i in range(3):
            with pytest.raises(Exception):
                breaker.call(failing_function)
        
        # 4th call should raise CircuitBreakerOpenError
        with pytest.raises(CircuitBreakerOpenError):
            breaker.call(failing_function)
    
    def test_circuit_breaker_recovers(self):
        """Test circuit breaker recovery after timeout."""
        import time
        from src.utils.circuit_breaker import CircuitBreaker
        
        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        
        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: 1/0)
        
        # Wait for recovery
        time.sleep(1.1)
        
        # Should now be in half-open, successful call should close it
        result = breaker.call(lambda: "success")
        assert result == "success"
        assert breaker.is_closed


class TestRetryLogic:
    """Tests for retry logic."""
    
    def test_retry_succeeds_after_failures(self):
        """Test that retry logic eventually succeeds."""
        from src.utils.retry import retry_with_backoff
        
        attempts = {'count': 0}
        
        @retry_with_backoff(max_retries=3, base_delay=0.1, jitter=False)
        def flaky_function():
            attempts['count'] += 1
            if attempts['count'] < 3:
                raise Exception("Temporary failure")
            return "success"
        
        result = flaky_function()
        assert result == "success"
        assert attempts['count'] == 3
    
    def test_retry_exhausts_after_max_attempts(self):
        """Test that retry gives up after max attempts."""
        from src.utils.retry import retry_with_backoff
        
        @retry_with_backoff(max_retries=2, base_delay=0.1)
        def always_failing():
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError):
            always_failing()
