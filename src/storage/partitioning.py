"""
Table partitioning for improved query performance.

Partitions large tables by fiscal year for faster queries.
"""

from typing import List, Optional

from ..storage.database import Database
from ..utils.logger import get_logger

logger = get_logger("finloom.storage.partitioning")


class TablePartitioner:
    """
    Manages table partitioning for large datasets.
    
    Partitions facts and normalized_financials by year for performance.
    """
    
    def __init__(self, db: Database):
        """
        Initialize table partitioner.
        
        Args:
            db: Database instance.
        """
        self.db = db
    
    def create_partitioned_facts_view(self) -> None:
        """
        Create partitioned view for facts table.
        
        Uses DuckDB's efficient filtering on period_end.
        """
        logger.info("Creating partitioned facts view")
        
        # Create view with year extraction
        self.db.connection.execute("""
            CREATE OR REPLACE VIEW facts_by_year AS
            SELECT 
                *,
                EXTRACT(YEAR FROM period_end) as fiscal_year
            FROM facts
            WHERE period_end IS NOT NULL
        """)
        
        logger.info("Created facts_by_year view")
    
    def create_partitioned_normalized_view(self) -> None:
        """Create partitioned view for normalized_financials."""
        logger.info("Creating partitioned normalized financials view")
        
        # Already has fiscal_year column, just create optimized view
        self.db.connection.execute("""
            CREATE OR REPLACE VIEW normalized_by_year AS
            SELECT *
            FROM normalized_financials
            WHERE fiscal_year IS NOT NULL
        """)
        
        logger.info("Created normalized_by_year view")
    
    def create_year_specific_tables(self, years: Optional[List[int]] = None) -> None:
        """
        Create year-specific materialized tables for heavy queries.
        
        Args:
            years: List of years to partition. If None, uses all years in data.
        """
        if years is None:
            # Get all years from data
            result = self.db.connection.execute("""
                SELECT DISTINCT fiscal_year 
                FROM normalized_financials 
                WHERE fiscal_year IS NOT NULL
                ORDER BY fiscal_year
            """).fetchall()
            years = [r[0] for r in result]
        
        logger.info(f"Creating year-specific tables for {len(years)} years")
        
        for year in years:
            # Create facts partition
            table_name = f"facts_{year}"
            self.db.connection.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS
                SELECT * FROM facts
                WHERE EXTRACT(YEAR FROM period_end) = {year}
            """)
            
            # Create index
            self.db.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_accession 
                ON {table_name}(accession_number)
            """)
            
            logger.debug(f"Created partition: {table_name}")
            
            # Create normalized partition
            norm_table = f"normalized_financials_{year}"
            self.db.connection.execute(f"""
                CREATE TABLE IF NOT EXISTS {norm_table} AS
                SELECT * FROM normalized_financials
                WHERE fiscal_year = {year}
            """)
            
            # Create indexes
            self.db.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{norm_table}_ticker 
                ON {norm_table}(company_ticker)
            """)
            
            logger.debug(f"Created partition: {norm_table}")
        
        logger.info(f"Created {len(years)} year-specific table pairs")
    
    def optimize_indexes(self) -> None:
        """Create optimized indexes for partitioned queries."""
        logger.info("Creating optimized indexes")
        
        # Composite index on facts for year-based queries
        try:
            self.db.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_facts_year_concept 
                ON facts(period_end, concept_name)
            """)
            logger.debug("Created idx_facts_year_concept")
        except Exception as e:
            logger.warning(f"Could not create year-concept index: {e}")
        
        # Composite index on normalized for year-ticker queries
        try:
            self.db.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_normalized_year_ticker 
                ON normalized_financials(fiscal_year, company_ticker)
            """)
            logger.debug("Created idx_normalized_year_ticker")
        except Exception as e:
            logger.warning(f"Could not create year-ticker index: {e}")
        
        logger.info("Optimized indexes created")
    
    def analyze_partition_stats(self) -> dict:
        """
        Analyze partitioning statistics.
        
        Returns:
            Dict with partition statistics.
        """
        stats = {}
        
        # Get facts distribution by year
        result = self.db.connection.execute("""
            SELECT 
                EXTRACT(YEAR FROM period_end) as year,
                COUNT(*) as fact_count
            FROM facts
            WHERE period_end IS NOT NULL
            GROUP BY year
            ORDER BY year DESC
        """).fetchall()
        
        stats['facts_by_year'] = {int(r[0]): r[1] for r in result}
        
        # Get normalized distribution by year
        result = self.db.connection.execute("""
            SELECT 
                fiscal_year,
                COUNT(*) as metric_count
            FROM normalized_financials
            WHERE fiscal_year IS NOT NULL
            GROUP BY fiscal_year
            ORDER BY fiscal_year DESC
        """).fetchall()
        
        stats['normalized_by_year'] = {int(r[0]): r[1] for r in result}
        
        # Check if partitioned tables exist
        result = self.db.connection.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_name LIKE 'facts_%' 
               OR table_name LIKE 'normalized_financials_%'
        """).fetchall()
        
        stats['partitioned_tables'] = [r[0] for r in result]
        
        return stats
    
    def recommend_partitioning(self) -> dict:
        """
        Analyze data and recommend partitioning strategy.
        
        Returns:
            Dict with recommendations.
        """
        stats = self.analyze_partition_stats()
        
        total_facts = sum(stats['facts_by_year'].values())
        total_normalized = sum(stats['normalized_by_year'].values())
        
        recommendations = {
            "total_facts": total_facts,
            "total_normalized": total_normalized,
            "should_partition": False,
            "reason": "",
            "recommended_years": []
        }
        
        # Recommend partitioning if > 500K facts
        if total_facts > 500_000:
            recommendations["should_partition"] = True
            recommendations["reason"] = "Large dataset benefits from year-based partitioning"
            
            # Recommend partitioning years with > 20K facts
            heavy_years = [
                year for year, count in stats['facts_by_year'].items()
                if count > 20_000
            ]
            recommendations["recommended_years"] = heavy_years
        
        elif total_facts > 100_000:
            recommendations["should_partition"] = True
            recommendations["reason"] = "Dataset size suggests partitioning for future growth"
            recommendations["recommended_years"] = list(stats['facts_by_year'].keys())
        
        else:
            recommendations["reason"] = "Dataset too small, partitioning not needed yet"
        
        return recommendations


def setup_partitioning(db: Database, force: bool = False) -> dict:
    """
    Set up table partitioning based on data analysis.
    
    Args:
        db: Database instance.
        force: Force partitioning even if not recommended.
    
    Returns:
        Setup results.
    """
    partitioner = TablePartitioner(db)
    
    # Analyze and get recommendations
    recommendations = partitioner.recommend_partitioning()
    
    logger.info(f"Partitioning recommendation: {recommendations['reason']}")
    
    if not recommendations['should_partition'] and not force:
        logger.info("Skipping partitioning - not needed yet")
        return {
            "partitioned": False,
            "reason": recommendations['reason'],
            "stats": recommendations
        }
    
    # Create partitioned views
    partitioner.create_partitioned_facts_view()
    partitioner.create_partitioned_normalized_view()
    
    # Optimize indexes
    partitioner.optimize_indexes()
    
    # Create year-specific tables if recommended
    if recommendations['recommended_years'] and force:
        partitioner.create_year_specific_tables(recommendations['recommended_years'])
    
    logger.info("Partitioning setup complete")
    
    return {
        "partitioned": True,
        "views_created": True,
        "indexes_optimized": True,
        "year_tables_created": force,
        "stats": recommendations
    }


# Query helpers for partitioned tables
def query_facts_by_year(db: Database, year: int, **filters) -> list:
    """
    Query facts for specific year (optimized).
    
    Args:
        db: Database instance.
        year: Fiscal year.
        **filters: Additional filters (concept_name, accession_number, etc.).
    
    Returns:
        List of facts.
    """
    query = """
        SELECT * FROM facts_by_year
        WHERE fiscal_year = ?
    """
    params = [year]
    
    if 'concept_name' in filters:
        query += " AND concept_name = ?"
        params.append(filters['concept_name'])
    
    if 'accession_number' in filters:
        query += " AND accession_number = ?"
        params.append(filters['accession_number'])
    
    return db.connection.execute(query, params).fetchall()


def query_normalized_by_year(
    db: Database,
    year: int,
    ticker: Optional[str] = None
) -> list:
    """
    Query normalized metrics for specific year (optimized).
    
    Args:
        db: Database instance.
        year: Fiscal year.
        ticker: Optional company ticker filter.
    
    Returns:
        List of metrics.
    """
    query = """
        SELECT * FROM normalized_by_year
        WHERE fiscal_year = ?
    """
    params = [year]
    
    if ticker:
        query += " AND company_ticker = ?"
        params.append(ticker)
    
    return db.connection.execute(query, params).fetchall()
