#!/usr/bin/env python3
"""
S3 Backup Script

Backs up raw data and database to AWS S3.
Uses incremental sync for efficiency.

Usage:
    python scripts/03_backup_to_s3.py
    python scripts/03_backup_to_s3.py --database-only
    python scripts/03_backup_to_s3.py --raw-only
    
Cron example (run weekly on Sunday at 2 AM):
    0 2 * * 0 cd /path/to/FinLoom-2026 && /path/to/venv/bin/python scripts/03_backup_to_s3.py
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.s3_backup import S3Backup
from src.utils.config import get_settings, load_config
from src.utils.logger import get_logger, setup_logging

logger = get_logger("finloom.backup")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Backup data to S3")
    parser.add_argument(
        "--database-only",
        action="store_true",
        help="Only backup database",
    )
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Only backup raw data",
    )
    parser.add_argument(
        "--create-bucket",
        action="store_true",
        help="Create S3 bucket if it doesn't exist",
    )
    parser.add_argument(
        "--list-backups",
        action="store_true",
        help="List existing database backups",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show storage statistics",
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup
    setup_logging()
    load_config()
    settings = get_settings()
    
    logger.info("=" * 60)
    logger.info("S3 Backup")
    logger.info(f"Started at: {datetime.now()}")
    logger.info(f"Bucket: {settings.storage.s3.bucket_name}")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    backup = S3Backup()
    
    # List backups if requested
    if args.list_backups:
        logger.info("Existing database backups:")
        backups = backup.list_backups()
        for b in backups[:10]:  # Show last 10
            size_mb = b["size"] / 1024 / 1024
            logger.info(f"  {b['key']} ({size_mb:.1f} MB) - {b['last_modified']}")
        return 0
    
    # Show stats if requested
    if args.stats:
        logger.info("Storage statistics:")
        stats = backup.get_storage_stats()
        logger.info(f"  Raw data: {stats['raw_objects']} objects, "
                   f"{stats['raw_size_bytes'] / 1024 / 1024 / 1024:.2f} GB")
        logger.info(f"  Processed: {stats['processed_objects']} objects, "
                   f"{stats['processed_size_bytes'] / 1024 / 1024:.2f} MB")
        logger.info(f"  Database: {stats['database_objects']} backups, "
                   f"{stats['database_size_bytes'] / 1024 / 1024:.2f} MB")
        return 0
    
    # Check bucket exists
    if not backup.bucket_exists():
        if args.create_bucket:
            logger.info("Creating S3 bucket...")
            if not backup.create_bucket():
                logger.error("Failed to create bucket")
                return 1
        else:
            logger.error(
                f"Bucket {settings.storage.s3.bucket_name} does not exist. "
                "Use --create-bucket to create it."
            )
            return 1
    
    results = {
        "raw_sync": None,
        "database_backup": None,
    }
    
    # Backup raw data
    if not args.database_only:
        logger.info("-" * 40)
        logger.info("Syncing raw data to S3...")
        results["raw_sync"] = backup.sync_raw_data(use_aws_cli=True)
        
        if results["raw_sync"]:
            logger.info("Raw data sync completed")
        else:
            logger.warning("Raw data sync failed")
    
    # Backup database
    if not args.raw_only:
        logger.info("-" * 40)
        logger.info("Backing up database...")
        results["database_backup"] = backup.backup_database()
        
        if results["database_backup"]:
            logger.info(f"Database backup completed: {results['database_backup']}")
        else:
            logger.warning("Database backup failed")
    
    elapsed = time.time() - start_time
    
    # Summary
    logger.info("=" * 60)
    logger.info("Backup Complete")
    logger.info(f"Duration: {elapsed:.1f}s")
    
    if results["raw_sync"] is not None:
        logger.info(f"Raw sync: {'Success' if results['raw_sync'] else 'Failed'}")
    if results["database_backup"] is not None:
        logger.info(f"Database: {'Success' if results['database_backup'] else 'Failed'}")
    
    logger.info("=" * 60)
    
    # Return error if any backup failed
    if (results["raw_sync"] is False) or (results["database_backup"] is None and not args.raw_only):
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
