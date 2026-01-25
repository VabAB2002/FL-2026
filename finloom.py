#!/usr/bin/env python3
"""
FinLoom Master CLI - Enterprise Operations Command Center

Unified interface for all FinLoom operations.

Usage:
    finloom status              # System health and statistics
    finloom monitor start       # Start monitoring services
    finloom quality check       # Run quality assessment
    finloom backup create       # Create backup
    finloom cache stats         # Cache statistics
    finloom tracing enable      # Enable distributed tracing
"""

import argparse
import json
import os
import sys
from pathlib import Path
from tabulate import tabulate

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.config.env_config import get_env_config
from src.utils.logger import get_logger, setup_logging

logger = get_logger("finloom.cli")


class FinLoomCLI:
    """Master CLI for FinLoom operations."""
    
    def __init__(self):
        """Initialize CLI."""
        self.config = get_env_config()
        self.db = Database()
    
    def cmd_status(self, args):
        """Show system status and statistics."""
        print("\n" + "="*80)
        print("  FINLOOM SYSTEM STATUS")
        print("="*80 + "\n")
        
        # Environment
        print(f"Environment: {self.config.environment.value}")
        print(f"Database: {self.config.get('storage.database_path')}")
        print()
        
        # Database statistics
        print("DATABASE STATISTICS:")
        stats = {
            "Companies": self.db.connection.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
            "Filings": self.db.connection.execute("SELECT COUNT(*) FROM filings WHERE xbrl_processed = TRUE").fetchone()[0],
            "Facts": self.db.connection.execute("SELECT COUNT(*) FROM facts").fetchone()[0],
            "Normalized Metrics": self.db.connection.execute("SELECT COUNT(*) FROM normalized_financials").fetchone()[0],
        }
        
        for key, value in stats.items():
            print(f"  {key:.<30} {value:>10,}")
        print()
        
        # Database size
        try:
            db_path = Path(self.config.get('storage.database_path'))
            if db_path.exists():
                size_mb = db_path.stat().st_size / (1024 * 1024)
                print(f"  Database Size:................. {size_mb:>10,.2f} MB")
        except Exception as e:
            print(f"  Database Size:................. ERROR: {e}")
        print()
        
        # Feature flags
        print("FEATURE FLAGS:")
        flags = self.config.get_feature_flags()
        for feature, enabled in flags.items():
            status = "✅ ENABLED" if enabled else "⚠️  DISABLED"
            print(f"  {feature:.<30} {status}")
        print()
        
        # Service health
        print("SERVICE HEALTH:")
        try:
            from src.monitoring.health import get_health_checker
            checker = get_health_checker()
            
            checks = {
                "Database": checker.check_database(),
                "SEC API": checker.check_sec_api(),
                "Disk Space": checker.check_disk_space(),
            }
            
            for service, status in checks.items():
                icon = "✅" if status['status'] == 'healthy' else "⚠️"
                print(f"  {icon} {service}")
        except Exception as e:
            print(f"  ⚠️  Health checks unavailable: {e}")
        
        print()
    
    def cmd_monitor(self, args):
        """Monitor operations."""
        if args.action == 'start':
            print("\n🚀 Starting monitoring services...")
            print("\nStarting services in background:")
            
            # Start Prometheus metrics
            if not args.no_metrics:
                print("  • Prometheus metrics on http://localhost:9090/metrics")
                os.system("python -c 'from src.monitoring import start_metrics_server; start_metrics_server()' &")
            
            # Start health checks
            if not args.no_health:
                print("  • Health checks on http://localhost:8000/health/detailed")
                os.system("python -m src.monitoring.health &")
            
            print("\n✅ Monitoring services started!")
            print("\nAccess:")
            print("  Metrics:  http://localhost:9090/metrics")
            print("  Health:   http://localhost:8000/health/detailed")
            print()
        
        elif args.action == 'stop':
            print("\n🛑 Stopping monitoring services...")
            os.system("pkill -f 'start_metrics_server'")
            os.system("pkill -f 'src.monitoring.health'")
            print("✅ Services stopped!\n")
        
        elif args.action == 'status':
            print("\n📊 Monitoring Status:")
            # Check if processes are running
            metrics_running = os.system("pgrep -f 'start_metrics_server' > /dev/null") == 0
            health_running = os.system("pgrep -f 'src.monitoring.health' > /dev/null") == 0
            
            print(f"  Metrics Server: {'✅ Running' if metrics_running else '⚠️  Stopped'}")
            print(f"  Health Checks:  {'✅ Running' if health_running else '⚠️  Stopped'}")
            print()
    
    def cmd_quality(self, args):
        """Quality operations."""
        from src.validation.reconciliation import ReconciliationEngine
        from src.validation.quality_scorer import DataQualityScorer
        
        if args.action == 'check':
            print("\n🔍 Running data quality checks...\n")
            
            engine = ReconciliationEngine(self.db)
            results = engine.run_all_checks()
            
            print(f"Total Issues: {results['total_issues']}")
            print(f"  Critical: {results['critical_issues']}")
            print(f"  Errors: {results['error_issues']}")
            print(f"  Warnings: {results['warning_issues']}")
            
            if results['total_issues'] == 0:
                print("\n✅ No issues found - data quality is excellent!\n")
            else:
                print("\n⚠️  Issues detected. Run with --detail for more info.\n")
        
        elif args.action == 'score':
            print("\n📊 Calculating quality scores...\n")
            
            scorer = DataQualityScorer(self.db)
            results = scorer.score_all_companies()
            
            table_data = []
            for result in results:
                if result['filing_count'] > 0:
                    table_data.append([
                        result['ticker'],
                        result['filing_count'],
                        f"{result['average_score']:.1f}",
                        f"{result['min_score']:.1f} - {result['max_score']:.1f}"
                    ])
            
            print(tabulate(
                table_data,
                headers=["Ticker", "Filings", "Avg Score", "Range"],
                tablefmt="grid"
            ))
            print()
    
    def cmd_backup(self, args):
        """Backup operations."""
        print(f"\n💾 Running {args.type} backup...\n")
        
        if args.type == 'full':
            os.system("python scripts/backup_manager.py --full")
        elif args.type == 'incremental':
            os.system("python scripts/backup_manager.py --incremental")
        elif args.type == 'list':
            os.system("python scripts/backup_manager.py --list")
    
    def cmd_cache(self, args):
        """Cache operations."""
        try:
            from src.caching.redis_cache import get_cache
            cache = get_cache()
            
            if not cache.enabled:
                print("\n⚠️  Redis cache is disabled\n")
                return
            
            if args.action == 'stats':
                print("\n📊 Cache Statistics:\n")
                stats = cache.get_stats()
                
                for key, value in stats.items():
                    print(f"  {key:.<30} {value}")
                print()
            
            elif args.action == 'clear':
                print("\n🗑️  Clearing cache...\n")
                from src.caching.redis_cache import QueryCache
                qcache = QueryCache(cache)
                qcache.invalidate_all()
                print("✅ Cache cleared!\n")
        
        except Exception as e:
            print(f"\n⚠️  Cache error: {e}\n")
    
    def cmd_tracing(self, args):
        """Tracing operations."""
        if args.action == 'enable':
            print("\n🔍 Enabling distributed tracing...")
            os.environ['FINLOOM_TRACING_ENABLED'] = 'true'
            print("✅ Tracing enabled!")
            print("\nMake sure Jaeger is running:")
            print("  docker run -d -p 6831:6831/udp -p 16686:16686 jaegertracing/all-in-one")
            print("\nAccess UI: http://localhost:16686\n")
        
        elif args.action == 'disable':
            print("\n🔍 Disabling distributed tracing...")
            os.environ['FINLOOM_TRACING_ENABLED'] = 'false'
            print("✅ Tracing disabled!\n")
    
    def cmd_config(self, args):
        """Configuration operations."""
        if args.action == 'show':
            print("\n⚙️  Current Configuration:\n")
            
            print(f"Environment: {self.config.environment.value}")
            print(f"Database Config:")
            db_config = self.config.get_database_config()
            for key, value in db_config.items():
                print(f"  {key}: {value}")
            
            print(f"\nMonitoring Config:")
            mon_config = self.config.get_monitoring_config()
            for key, value in mon_config.items():
                print(f"  {key}: {value}")
            
            print(f"\nSEC API Config:")
            api_config = self.config.get_sec_api_config()
            for key, value in api_config.items():
                print(f"  {key}: {value}")
            print()
        
        elif args.action == 'validate':
            print("\n✅ Validating configuration...\n")
            errors = self.config.validate_config()
            
            if errors:
                print("⚠️  Configuration errors:")
                for error in errors:
                    print(f"  • {error}")
                print()
            else:
                print("✅ Configuration is valid!\n")
    
    def cmd_perf(self, args):
        """Performance operations."""
        if args.action == 'analyze':
            print("\n📊 Performance Analysis:\n")
            
            # Query performance
            import time
            start = time.time()
            self.db.connection.execute("SELECT COUNT(*) FROM facts").fetchone()
            query_time = (time.time() - start) * 1000
            
            print(f"  Query Latency:................. {query_time:.2f}ms")
            
            # Database size
            total_facts = self.db.connection.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
            print(f"  Total Facts:................... {total_facts:,}")
            
            # Partitioning recommendation
            from src.storage.partitioning import TablePartitioner
            partitioner = TablePartitioner(self.db)
            rec = partitioner.recommend_partitioning()
            
            print(f"\nPartitioning Recommendation:")
            print(f"  Should Partition: {rec['should_partition']}")
            print(f"  Reason: {rec['reason']}")
            print()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="FinLoom Enterprise Operations CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Status command
    subparsers.add_parser('status', help='Show system status')
    
    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor operations')
    monitor_parser.add_argument('action', choices=['start', 'stop', 'status'])
    monitor_parser.add_argument('--no-metrics', action='store_true', help='Skip metrics server')
    monitor_parser.add_argument('--no-health', action='store_true', help='Skip health checks')
    
    # Quality command
    quality_parser = subparsers.add_parser('quality', help='Quality operations')
    quality_parser.add_argument('action', choices=['check', 'score'])
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Backup operations')
    backup_parser.add_argument('type', choices=['full', 'incremental', 'list'])
    
    # Cache command
    cache_parser = subparsers.add_parser('cache', help='Cache operations')
    cache_parser.add_argument('action', choices=['stats', 'clear'])
    
    # Tracing command
    tracing_parser = subparsers.add_parser('tracing', help='Tracing operations')
    tracing_parser.add_argument('action', choices=['enable', 'disable'])
    
    # Config command
    config_parser = subparsers.add_parser('config', help='Configuration operations')
    config_parser.add_argument('action', choices=['show', 'validate'])
    
    # Performance command
    perf_parser = subparsers.add_parser('perf', help='Performance operations')
    perf_parser.add_argument('action', choices=['analyze'])
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Setup logging
    setup_logging()
    
    # Execute command
    cli = FinLoomCLI()
    
    try:
        if args.command == 'status':
            cli.cmd_status(args)
        elif args.command == 'monitor':
            cli.cmd_monitor(args)
        elif args.command == 'quality':
            cli.cmd_quality(args)
        elif args.command == 'backup':
            cli.cmd_backup(args)
        elif args.command == 'cache':
            cli.cmd_cache(args)
        elif args.command == 'tracing':
            cli.cmd_tracing(args)
        elif args.command == 'config':
            cli.cmd_config(args)
        elif args.command == 'perf':
            cli.cmd_perf(args)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user\n")
        return 130
    except Exception as e:
        logger.error(f"Command failed: {e}", exc_info=True)
        print(f"\n❌ Error: {e}\n")
        return 1
    finally:
        cli.db.close()


if __name__ == "__main__":
    sys.exit(main())
