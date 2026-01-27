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
import subprocess
import sys
from pathlib import Path
from tabulate import tabulate

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
        
        # System Verification (if requested)
        if hasattr(args, 'verify_integrity') and args.verify_integrity:
            print("="*80)
            print("  COMPREHENSIVE SYSTEM VERIFICATION")
            print("="*80 + "\n")
            
            from src.monitoring.health_checker import DatabaseHealthChecker
            
            db_path = self.config.get('storage.database_path')
            checker = DatabaseHealthChecker(db_path)
            
            try:
                report = checker.verify_system_integrity()
                
                # Overall status
                status_icon = {"healthy": "✅", "warning": "⚠️", "critical": "❌"}.get(report['status'], "❓")
                print(f"System Status: {status_icon} {report['status'].upper()}\n")
                
                # Schema
                if 'schema' in report:
                    print("1. DATABASE SCHEMA")
                    if report['schema']['missing_tables']:
                        print(f"   ❌ Missing tables: {', '.join(report['schema']['missing_tables'])}")
                    else:
                        print(f"   ✅ All required tables present ({len(report['schema']['required_tables'])} tables)")
                    print()
                
                # Extraction
                if 'extraction' in report:
                    ext = report['extraction']
                    print("2. EXTRACTION PROGRESS")
                    print(f"   Total filings:................ {ext['total_filings']:,}")
                    print(f"   Processed:.................... {ext['processed_filings']:,} ({ext['processing_rate']:.1f}%)")
                    print(f"   With sections:................ {ext['filings_with_sections']:,} ({ext['section_rate']:.1f}%)")
                    print(f"   Total sections:............... {ext['total_sections']:,}")
                    print(f"   Total tables:................. {ext['total_tables']:,}")
                    print(f"   Total footnotes:.............. {ext['total_footnotes']:,}")
                    print(f"   Total chunks:................. {ext['total_chunks']:,}")
                    print()
                
                # Top companies
                if 'top_companies' in report and report['top_companies']:
                    print("3. TOP PROCESSED COMPANIES")
                    for company in report['top_companies'][:5]:
                        print(f"   {company['ticker']:6} {company['name'][:40]:40} {company['processed_filings']:3} filings")
                    print()
                
                # Quality
                if 'quality' in report and report['quality']:
                    q = report['quality']
                    print("4. QUALITY METRICS")
                    print(f"   Average confidence:........... {q['avg_confidence']:.3f}")
                    print(f"   Range:........................ {q['min_confidence']:.3f} - {q['max_confidence']:.3f}")
                    print(f"   Scored sections:.............. {q['scored_sections']:,}")
                    print()
                
                # Features
                if 'features' in report and report['features']:
                    f = report['features']
                    print("5. METADATA FEATURES")
                    print(f"   Sections with part labels:.... {f['sections_with_parts']:,} ({f['parts_rate']:.1f}%)")
                    print(f"   Sections with tables:......... {f['sections_with_tables']:,} ({f['tables_rate']:.1f}%)")
                    print(f"   Sections with lists:.......... {f['sections_with_lists']:,} ({f['lists_rate']:.1f}%)")
                    print()
                
                # Chunking
                if 'chunking' in report and report['chunking']:
                    print("6. HIERARCHICAL CHUNKING")
                    for chunk in report['chunking']:
                        print(f"   Level {chunk['level']} ({chunk['name']:10}): {chunk['count']:,} chunks")
                    print()
                
                # Database
                if 'database' in report:
                    db = report['database']
                    print("7. DATABASE")
                    print(f"   Size:......................... {db['size_mb']:.2f} MB")
                    print(f"   Read-only:.................... {'Yes' if db['read_only'] else 'No'}")
                    print()
                
                # Issues and warnings
                if report.get('issues'):
                    print("❌ ISSUES:")
                    for issue in report['issues']:
                        print(f"   • {issue}")
                    print()
                
                if report.get('warnings'):
                    print("⚠️  WARNINGS:")
                    for warning in report['warnings']:
                        print(f"   • {warning}")
                    print()
                
                # Summary
                print("="*80)
                if report['status'] == 'healthy':
                    print("✅ SYSTEM IS PRODUCTION READY")
                elif report['status'] == 'warning':
                    print("⚠️  SYSTEM HAS WARNINGS - Review above for details")
                else:
                    print("❌ SYSTEM HAS CRITICAL ISSUES - Fix required before production")
                print("="*80)
                print()
                
            except Exception as e:
                print(f"❌ Verification failed: {e}")
                import traceback
                traceback.print_exc()
                print()
    
    def cmd_monitor(self, args):
        """Monitor operations."""
        if args.action == 'start':
            print("\n🚀 Starting monitoring services...")
            print("\nStarting services in background:")
            
            # Start Prometheus metrics
            if not args.no_metrics:
                print("  • Prometheus metrics on http://localhost:9090/metrics")
                try:
                    subprocess.Popen(
                        [sys.executable, "-c", "from src.monitoring import start_metrics_server; start_metrics_server()"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True
                    )
                except Exception as e:
                    print(f"    ⚠️  Failed to start metrics server: {e}")
            
            # Start health checks
            if not args.no_health:
                print("  • Health checks on http://localhost:8000/health/detailed")
                try:
                    subprocess.Popen(
                        [sys.executable, "-m", "src.monitoring.health"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True
                    )
                except Exception as e:
                    print(f"    ⚠️  Failed to start health checks: {e}")
            
            print("\n✅ Monitoring services started!")
            print("\nAccess:")
            print("  Metrics:  http://localhost:9090/metrics")
            print("  Health:   http://localhost:8000/health/detailed")
            print()
        
        elif args.action == 'stop':
            print("\n🛑 Stopping monitoring services...")
            try:
                subprocess.run(["pkill", "-f", "start_metrics_server"], check=False)
                subprocess.run(["pkill", "-f", "src.monitoring.health"], check=False)
                print("✅ Services stopped!\n")
            except Exception as e:
                print(f"⚠️  Error stopping services: {e}\n")
        
        elif args.action == 'status':
            print("\n📊 Monitoring Status:")
            # Check if processes are running
            try:
                result = subprocess.run(
                    ["pgrep", "-f", "start_metrics_server"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                metrics_running = result.returncode == 0
            except Exception:
                metrics_running = False
            
            try:
                result = subprocess.run(
                    ["pgrep", "-f", "src.monitoring.health"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                health_running = result.returncode == 0
            except Exception:
                health_running = False
            
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
        
        try:
            if args.type == 'full':
                subprocess.run([sys.executable, "scripts/backup_manager.py", "--full"], check=True)
            elif args.type == 'incremental':
                subprocess.run([sys.executable, "scripts/backup_manager.py", "--incremental"], check=True)
            elif args.type == 'list':
                subprocess.run([sys.executable, "scripts/backup_manager.py", "--list"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Backup command failed with exit code {e.returncode}")
        except Exception as e:
            print(f"⚠️  Error running backup: {e}")
    
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
    
    def cmd_db(self, args):
        """Database maintenance operations."""
        if args.action == 'detect-duplicates':
            print("\n🔍 Detecting Duplicates\n")
            
            duplicates = self.db.detect_duplicates(args.table)
            
            if not duplicates:
                print("✅ No duplicates found!\n")
                return
            
            print(f"Found {len(duplicates)} duplicate groups:\n")
            
            for i, dup in enumerate(duplicates, 1):
                print(f"{i}. {dup['ticker']} {dup['year']} Q{dup['quarter'] or 'N/A'} {dup['metric']}")
                print(f"   {dup['count']} records:")
                for rec in dup['records']:
                    status = "KEEP" if rec['keep'] else "DELETE"
                    print(f"     - ID {rec['id']}: confidence={rec['confidence']:.2f}, value={rec['value']}, created={rec['created_at']} [{status}]")
                print()
            
            print("="*70)
            print(f"Total duplicate groups: {len(duplicates)}")
            print(f"Total records that can be removed: {sum(d['count'] - 1 for d in duplicates)}")
            print("\nTo remove duplicates, run:")
            print(f"  python finloom.py db clean-duplicates --table {args.table} --execute")
            print("="*70)
            print()
        
        elif args.action == 'clean-duplicates':
            if not args.execute:
                print("\n🔍 Clean Duplicates (DRY RUN)\n")
                print("This is a preview - no data will be deleted.")
                print("Use --execute to actually delete duplicates.\n")
            else:
                print("\n🧹 Clean Duplicates (EXECUTE MODE)\n")
                print("⚠️  This will permanently delete duplicate records!")
                print("Only the best record (highest confidence, most recent) will be kept.\n")
            
            # Run cleanup
            stats = self.db.remove_duplicates(
                table=args.table,
                dry_run=not args.execute
            )
            
            print()
            print("="*70)
            if args.execute:
                print("  CLEANUP COMPLETE")
            else:
                print("  DRY RUN COMPLETE")
            print("="*70)
            print(f"\n  Duplicate groups: {stats['duplicate_groups']}")
            print(f"  Records removed: {stats['records_removed']}")
            print(f"  Records kept: {stats['records_kept']}")
            
            if not args.execute:
                print("\n  No data was actually deleted (dry run)")
                print(f"  Run with --execute to actually clean duplicates:")
                print(f"    python finloom.py db clean-duplicates --table {args.table} --execute")
            else:
                print("\n  Database has been cleaned!")
            
            print("="*70)
            print()
    
    def cmd_recovery(self, args):
        """Recovery operations for failed extractions."""
        from src.processing.unstructured_pipeline import UnstructuredDataPipeline
        
        if args.action == 'reprocess':
            print("\n🔄 Recovery: Reprocessing Failed Extractions\n")
            
            # Query for filings that need reprocessing
            # Case 1: sections_processed=TRUE but COUNT(sections)=0
            orphaned_query = """
                SELECT f.accession_number, f.local_path, c.ticker
                FROM filings f
                JOIN companies c ON f.cik = c.cik
                LEFT JOIN sections s ON f.accession_number = s.accession_number
                WHERE f.sections_processed = TRUE
                GROUP BY f.accession_number, f.local_path, c.ticker
                HAVING COUNT(s.id) = 0
            """
            
            # Case 2: sections_processed=FALSE
            failed_query = """
                SELECT f.accession_number, f.local_path, c.ticker
                FROM filings f
                JOIN companies c ON f.cik = c.cik
                WHERE f.sections_processed = FALSE
                  AND f.local_path IS NOT NULL
            """
            
            # Choose query based on --all flag
            if args.all:
                query = failed_query + " UNION " + orphaned_query
            else:
                query = orphaned_query
            
            # Optional ticker filter
            if args.ticker:
                query = f"SELECT * FROM ({query}) WHERE ticker = '{args.ticker}'"
            
            filings = self.db.connection.execute(query).fetchall()
            
            if not filings:
                print("✅ No filings need reprocessing!")
                print()
                return
            
            print(f"Found {len(filings)} filing(s) to reprocess")
            if args.dry_run:
                print("\n🔍 DRY RUN - would reprocess:")
                for acc, path, ticker in filings[:10]:
                    print(f"  • {ticker}: {acc}")
                if len(filings) > 10:
                    print(f"  ... and {len(filings) - 10} more")
                print()
                return
            
            # Initialize pipeline
            db_path = self.config.get('storage.database_path')
            pipeline = UnstructuredDataPipeline(db_path)
            
            # Reprocess each filing
            success = 0
            failed = 0
            
            print()
            for accession, local_path, ticker in filings:
                print(f"Processing {ticker}: {accession}... ", end="", flush=True)
                
                if not local_path or not Path(local_path).exists():
                    print("❌ Path not found")
                    failed += 1
                    continue
                
                try:
                    result = pipeline.reprocess_filing(
                        accession_number=accession,
                        filing_path=Path(local_path),
                        force=args.force
                    )
                    
                    if result.success:
                        print(f"✅ {result.sections_count} sections")
                        success += 1
                    else:
                        print(f"❌ {result.error_message}")
                        failed += 1
                        
                except Exception as e:
                    print(f"❌ Error: {e}")
                    failed += 1
            
            print()
            print("="*70)
            print(f"✅ Success: {success}")
            print(f"❌ Failed:  {failed}")
            print("="*70)
            print()


def main():
    """Main CLI entry point."""
    # Validate environment variables before doing anything else
    from src.config.env_config import validate_environment_variables
    
    validation_errors = validate_environment_variables()
    if validation_errors:
        print("\n❌ Configuration Errors Detected:")
        print("="*70)
        for i, error in enumerate(validation_errors, 1):
            print(f"\n{i}. {error}")
        print("\n" + "="*70)
        print("\nPlease fix these configuration issues before running FinLoom.")
        print("See .env.example for required environment variables.\n")
        return 1
    
    parser = argparse.ArgumentParser(
        description="FinLoom Enterprise Operations CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    status_parser.add_argument('--verify-integrity', action='store_true', help='Run comprehensive system verification')
    
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
    
    # Recovery command
    recovery_parser = subparsers.add_parser('recovery', help='Recovery operations for failed extractions')
    recovery_parser.add_argument('action', choices=['reprocess'])
    recovery_parser.add_argument('--dry-run', action='store_true', help="Show what would be reprocessed without doing it")
    recovery_parser.add_argument('--ticker', type=str, help="Only reprocess specific ticker")
    recovery_parser.add_argument('--force', action='store_true', help="Reprocess even if sections already exist")
    recovery_parser.add_argument('--all', action='store_true', help="Include sections_processed=FALSE (not just orphaned)")
    
    # Database command
    db_parser = subparsers.add_parser('db', help='Database maintenance operations')
    db_parser.add_argument('action', choices=['clean-duplicates', 'detect-duplicates'])
    db_parser.add_argument('--table', type=str, default='normalized_financials', help="Table to check (default: normalized_financials)")
    db_parser.add_argument('--execute', action='store_true', help="Actually delete duplicates (required for clean-duplicates)")
    
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
        elif args.command == 'recovery':
            cli.cmd_recovery(args)
        elif args.command == 'db':
            cli.cmd_db(args)
        
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
