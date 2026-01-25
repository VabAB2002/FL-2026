#!/usr/bin/env python3
"""
Performance testing and benchmarking for FinLoom.

Tests query performance, download speed, and system throughput.
"""

import asyncio
import time
from datetime import datetime
from pathlib import Path
from statistics import mean, median
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.storage.connection_pool import ConnectionPool
from src.caching.redis_cache import get_cache
from src.utils.logger import get_logger

logger = get_logger("finloom.perf_test")


class PerformanceTester:
    """Performance testing suite."""
    
    def __init__(self):
        """Initialize tester."""
        self.db = Database()
        self.results = {}
    
    def test_query_performance(self, iterations=100):
        """Test database query performance."""
        print("\n📊 Testing Query Performance...")
        
        queries = {
            "Simple SELECT": "SELECT COUNT(*) FROM facts",
            "JOIN Query": """
                SELECT f.accession_number, c.ticker, COUNT(fa.id)
                FROM filings f
                JOIN companies c ON f.cik = c.cik
                LEFT JOIN facts fa ON f.accession_number = fa.accession_number
                GROUP BY f.accession_number, c.ticker
                LIMIT 100
            """,
            "Aggregation": """
                SELECT company_ticker, fiscal_year, 
                       AVG(value) as avg_value, COUNT(*) as count
                FROM normalized_financials
                GROUP BY company_ticker, fiscal_year
            """,
        }
        
        results = {}
        
        for query_name, query in queries.items():
            times = []
            
            for _ in range(iterations):
                start = time.time()
                self.db.connection.execute(query).fetchall()
                times.append((time.time() - start) * 1000)
            
            results[query_name] = {
                "mean": mean(times),
                "median": median(times),
                "min": min(times),
                "max": max(times)
            }
            
            print(f"\n{query_name}:")
            print(f"  Mean:   {results[query_name]['mean']:.2f}ms")
            print(f"  Median: {results[query_name]['median']:.2f}ms")
            print(f"  Range:  {results[query_name]['min']:.2f} - {results[query_name]['max']:.2f}ms")
        
        self.results['query_performance'] = results
        return results
    
    def test_connection_pool(self, iterations=50):
        """Test connection pool performance."""
        print("\n📊 Testing Connection Pool...")
        
        db_path = self.db.db_path
        pool = ConnectionPool(db_path, pool_size=5)
        
        # Test with pool
        times_with_pool = []
        for _ in range(iterations):
            start = time.time()
            with pool.get_connection() as conn:
                conn.execute("SELECT COUNT(*) FROM facts").fetchone()
            times_with_pool.append((time.time() - start) * 1000)
        
        # Test without pool (new connection each time)
        times_without_pool = []
        for _ in range(iterations):
            start = time.time()
            conn = self.db.connect()
            conn.execute("SELECT COUNT(*) FROM facts").fetchone()
            conn.close()
            times_without_pool.append((time.time() - start) * 1000)
        
        results = {
            "with_pool": {
                "mean": mean(times_with_pool),
                "median": median(times_with_pool)
            },
            "without_pool": {
                "mean": mean(times_without_pool),
                "median": median(times_without_pool)
            },
            "improvement": (
                (mean(times_without_pool) - mean(times_with_pool)) / 
                mean(times_without_pool) * 100
            )
        }
        
        print(f"\nWith Pool:    {results['with_pool']['mean']:.2f}ms")
        print(f"Without Pool: {results['without_pool']['mean']:.2f}ms")
        print(f"Improvement:  {results['improvement']:.1f}%")
        
        pool.close_all()
        self.results['connection_pool'] = results
        return results
    
    def test_cache_performance(self, iterations=100):
        """Test cache performance."""
        print("\n📊 Testing Cache Performance...")
        
        cache = get_cache()
        if not cache.enabled:
            print("⚠️  Cache disabled, skipping test")
            return None
        
        # Test cache hit
        cache.set('perf_test', 'test_key', {'data': 'test'})
        
        times_hit = []
        for _ in range(iterations):
            start = time.time()
            cache.get('perf_test', 'test_key')
            times_hit.append((time.time() - start) * 1000)
        
        # Test cache miss + database
        times_miss = []
        for _ in range(iterations):
            start = time.time()
            result = cache.get('perf_test', f'miss_{_}')
            if result is None:
                self.db.connection.execute("SELECT COUNT(*) FROM facts").fetchone()
            times_miss.append((time.time() - start) * 1000)
        
        results = {
            "cache_hit": {
                "mean": mean(times_hit),
                "median": median(times_hit)
            },
            "cache_miss": {
                "mean": mean(times_miss),
                "median": median(times_miss)
            },
            "speedup": mean(times_miss) / mean(times_hit)
        }
        
        print(f"\nCache Hit:  {results['cache_hit']['mean']:.2f}ms")
        print(f"Cache Miss: {results['cache_miss']['mean']:.2f}ms")
        print(f"Speedup:    {results['speedup']:.1f}x faster with cache")
        
        self.results['cache_performance'] = results
        return results
    
    def test_throughput(self, duration=10):
        """Test system throughput."""
        print(f"\n📊 Testing Throughput ({duration}s)...")
        
        start = time.time()
        queries_executed = 0
        
        while (time.time() - start) < duration:
            self.db.connection.execute("SELECT COUNT(*) FROM facts").fetchone()
            queries_executed += 1
        
        qps = queries_executed / duration
        
        print(f"\nQueries Executed: {queries_executed}")
        print(f"Queries/Second:   {qps:.1f}")
        
        self.results['throughput'] = {
            "total_queries": queries_executed,
            "queries_per_second": qps
        }
        
        return qps
    
    def generate_report(self):
        """Generate performance report."""
        print("\n" + "="*80)
        print("  PERFORMANCE TEST REPORT")
        print("="*80)
        
        print(f"\nTest Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Database stats
        print("\nDatabase Statistics:")
        stats = {
            "Facts": self.db.connection.execute("SELECT COUNT(*) FROM facts").fetchone()[0],
            "Filings": self.db.connection.execute("SELECT COUNT(*) FROM filings").fetchone()[0],
            "Companies": self.db.connection.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
        }
        for key, value in stats.items():
            print(f"  {key:.<30} {value:>10,}")
        
        # Performance summary
        if 'query_performance' in self.results:
            print("\nQuery Performance Summary:")
            for query, results in self.results['query_performance'].items():
                print(f"  {query}: {results['mean']:.2f}ms avg")
        
        if 'connection_pool' in self.results:
            print(f"\nConnection Pool Improvement: {self.results['connection_pool']['improvement']:.1f}%")
        
        if 'cache_performance' in self.results:
            print(f"Cache Speedup: {self.results['cache_performance']['speedup']:.1f}x")
        
        if 'throughput' in self.results:
            print(f"System Throughput: {self.results['throughput']['queries_per_second']:.1f} QPS")
        
        print("\n" + "="*80 + "\n")
    
    def close(self):
        """Cleanup."""
        self.db.close()


def main():
    """Run performance tests."""
    print("\n🚀 FinLoom Performance Test Suite\n")
    
    tester = PerformanceTester()
    
    try:
        # Run tests
        tester.test_query_performance(iterations=50)
        tester.test_connection_pool(iterations=30)
        tester.test_cache_performance(iterations=50)
        tester.test_throughput(duration=5)
        
        # Generate report
        tester.generate_report()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests cancelled\n")
        return 130
    except Exception as e:
        print(f"\n❌ Test failed: {e}\n")
        return 1
    finally:
        tester.close()


if __name__ == "__main__":
    sys.exit(main())
