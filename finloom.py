#!/usr/bin/env python3
"""
FinLoom CLI - Data Ingestion Operations

Core CLI for SEC data ingestion pipeline operations.

Usage:
    finloom status              # System status and statistics
    finloom config show         # Show configuration
    finloom db detect-duplicates # Detect duplicate records
    finloom recovery reprocess  # Reprocess failed extractions
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from src.storage.database import Database
from src.infrastructure.config import get_config
from src.infrastructure.logger import get_logger, setup_logging

logger = get_logger("finloom.cli")


class FinLoomCLI:
    """Core CLI for FinLoom data ingestion operations."""
    
    def __init__(self):
        """Initialize CLI."""
        self.config = get_config()
        self.db = Database()
    
    def cmd_status(self, args):
        """Show system status and statistics."""
        print("\n" + "="*80)
        print("  FINLOOM SYSTEM STATUS")
        print("="*80 + "\n")
        
        # Environment
        print(f"Environment: {self.config.environment.value}")
        print(f"Database: {self.config.database_path}")
        print()
        
        # Database statistics
        print("DATABASE STATISTICS:")
        stats = {
            "Companies": self.db.connection.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
            "Filings": self.db.connection.execute("SELECT COUNT(*) FROM filings WHERE xbrl_processed = TRUE").fetchone()[0],
            "Facts": self.db.connection.execute("SELECT COUNT(*) FROM facts").fetchone()[0],
            "Markdown Files": self.db.connection.execute("SELECT COUNT(*) FROM filings WHERE full_markdown IS NOT NULL").fetchone()[0],
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
        
        print()
    
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
        from src.documents.document_processor import UnstructuredDataPipeline
        
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
        description="FinLoom Data Ingestion CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    
    # Config command
    config_parser = subparsers.add_parser('config', help='Configuration operations')
    config_parser.add_argument('action', choices=['show', 'validate'])
    
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
        elif args.command == 'config':
            cli.cmd_config(args)
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
