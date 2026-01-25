#!/usr/bin/env python3
"""
Automated Backup Manager for FinLoom.

Handles full and incremental backups to S3 with disaster recovery.

Usage:
    python scripts/backup_manager.py --full
    python scripts/backup_manager.py --incremental
    python scripts/backup_manager.py --restore 20260125
    python scripts/backup_manager.py --list
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.utils.config import get_settings
from src.utils.logger import get_logger, setup_logging

logger = get_logger("finloom.backup")


class BackupManager:
    """
    Automated backup and disaster recovery manager.
    
    Handles full and incremental backups to S3 with retention policies.
    """
    
    def __init__(self, s3_bucket: Optional[str] = None):
        """
        Initialize backup manager.
        
        Args:
            s3_bucket: S3 bucket name. Defaults to config.
        """
        settings = get_settings()
        self.bucket = s3_bucket or settings.storage.s3.bucket_name
        self.db_path = Path(settings.storage.database_path)
        
        try:
            self.s3 = boto3.client('s3')
            # Test connection
            self.s3.head_bucket(Bucket=self.bucket)
            logger.info(f"Backup manager initialized for bucket: {self.bucket}")
        except ClientError as e:
            logger.error(f"S3 connection failed: {e}")
            raise
    
    def create_full_backup(self, compress: bool = True) -> Dict:
        """
        Create full database backup.
        
        Args:
            compress: Whether to compress backup (gzip).
        
        Returns:
            Dict with backup details.
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"finloom_full_{timestamp}.duckdb"
        
        logger.info(f"Creating full backup: {backup_file}")
        
        try:
            # Copy database file
            import shutil
            local_backup = Path("/tmp") / backup_file
            shutil.copy2(self.db_path, local_backup)
            
            # Compress if requested
            if compress:
                logger.info("Compressing backup...")
                import gzip
                compressed_file = f"{local_backup}.gz"
                with open(local_backup, 'rb') as f_in:
                    with gzip.open(compressed_file, 'wb', compresslevel=6) as f_out:
                        shutil.copyfileobj(f_in, f_out)
                local_backup.unlink()  # Remove uncompressed
                local_backup = Path(compressed_file)
                backup_file = f"{backup_file}.gz"
            
            # Get file size
            size_mb = local_backup.stat().st_size / (1024 * 1024)
            logger.info(f"Backup size: {size_mb:.2f} MB")
            
            # Upload to S3
            s3_key = f"backups/full/{backup_file}"
            logger.info(f"Uploading to s3://{self.bucket}/{s3_key}")
            
            self.s3.upload_file(
                str(local_backup),
                self.bucket,
                s3_key,
                ExtraArgs={
                    'ServerSideEncryption': 'AES256',
                    'StorageClass': 'STANDARD_IA',  # Infrequent Access for cost savings
                    'Metadata': {
                        'backup_type': 'full',
                        'timestamp': timestamp,
                        'compressed': str(compress),
                        'size_mb': f"{size_mb:.2f}"
                    }
                }
            )
            
            # Cleanup local file
            local_backup.unlink()
            
            logger.info(f"Full backup complete: s3://{self.bucket}/{s3_key}")
            
            # Cleanup old backups
            self._cleanup_old_backups(days=30)
            
            return {
                "success": True,
                "backup_file": backup_file,
                "s3_key": s3_key,
                "size_mb": size_mb,
                "timestamp": timestamp
            }
            
        except Exception as e:
            logger.error(f"Full backup failed: {e}")
            return {"success": False, "error": str(e)}
    
    def create_incremental_backup(self) -> Dict:
        """
        Create incremental backup (new records since last backup).
        
        Returns:
            Dict with backup details.
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info("Creating incremental backup")
        
        try:
            # Get last backup time
            last_backup_time = self._get_last_backup_time()
            logger.info(f"Last backup: {last_backup_time}")
            
            db = Database()
            
            # Export new records to Parquet
            tables = ['filings', 'facts', 'sections', 'normalized_financials']
            exported_files = []
            
            for table in tables:
                try:
                    df = db.connection.execute(f"""
                        SELECT * FROM {table}
                        WHERE created_at > ?
                    """, [last_backup_time]).fetchdf()
                    
                    if len(df) > 0:
                        filename = f"{table}_{timestamp}.parquet"
                        local_path = Path("/tmp") / filename
                        df.to_parquet(local_path, compression='snappy')
                        
                        # Upload to S3
                        s3_key = f"backups/incremental/{filename}"
                        self.s3.upload_file(
                            str(local_path),
                            self.bucket,
                            s3_key,
                            ExtraArgs={'ServerSideEncryption': 'AES256'}
                        )
                        
                        exported_files.append({
                            "table": table,
                            "records": len(df),
                            "s3_key": s3_key
                        })
                        
                        local_path.unlink()
                        logger.info(f"Exported {len(df)} records from {table}")
                        
                except Exception as e:
                    logger.warning(f"Could not export {table}: {e}")
            
            db.close()
            
            logger.info(f"Incremental backup complete: {len(exported_files)} tables")
            
            return {
                "success": True,
                "timestamp": timestamp,
                "tables_exported": len(exported_files),
                "files": exported_files
            }
            
        except Exception as e:
            logger.error(f"Incremental backup failed: {e}")
            return {"success": False, "error": str(e)}
    
    def restore_from_backup(self, backup_date: str) -> bool:
        """
        Restore database from S3 backup.
        
        Args:
            backup_date: Backup date in YYYYMMDD format.
        
        Returns:
            True if restore successful.
        """
        logger.warning(f"Restoring database from backup: {backup_date}")
        
        try:
            # List backups for that date
            prefix = f"backups/full/finloom_full_{backup_date}"
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response or not response['Contents']:
                logger.error(f"No backup found for date: {backup_date}")
                return False
            
            # Get most recent backup for that date
            backups = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            backup_key = backups[0]['Key']
            
            logger.info(f"Restoring from: s3://{self.bucket}/{backup_key}")
            
            # Download backup
            local_backup = Path("/tmp") / Path(backup_key).name
            self.s3.download_file(self.bucket, backup_key, str(local_backup))
            
            # Decompress if needed
            if local_backup.suffix == '.gz':
                import gzip
                import shutil
                uncompressed = local_backup.with_suffix('')
                with gzip.open(local_backup, 'rb') as f_in:
                    with open(uncompressed, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                local_backup.unlink()
                local_backup = uncompressed
            
            # Backup current database
            current_backup = self.db_path.with_suffix('.duckdb.before_restore')
            if self.db_path.exists():
                import shutil
                shutil.copy2(self.db_path, current_backup)
                logger.info(f"Current database backed up to: {current_backup}")
            
            # Replace database
            import shutil
            shutil.copy2(local_backup, self.db_path)
            local_backup.unlink()
            
            logger.info("Database restored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def list_backups(self, backup_type: str = "full", days: int = 30) -> list:
        """
        List available backups.
        
        Args:
            backup_type: 'full' or 'incremental'.
            days: Show backups from last N days.
        
        Returns:
            List of backup metadata.
        """
        try:
            prefix = f"backups/{backup_type}/"
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            cutoff = datetime.now() - timedelta(days=days)
            backups = []
            
            for obj in response['Contents']:
                if obj['LastModified'].replace(tzinfo=None) > cutoff:
                    size_mb = obj['Size'] / (1024 * 1024)
                    backups.append({
                        "key": obj['Key'],
                        "date": obj['LastModified'],
                        "size_mb": round(size_mb, 2)
                    })
            
            return sorted(backups, key=lambda x: x['date'], reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
            return []
    
    def _get_last_backup_time(self) -> datetime:
        """Get timestamp of last backup."""
        # Check S3 for last backup
        try:
            backups = self.list_backups(backup_type="full", days=7)
            if backups:
                return backups[0]['date']
        except Exception:
            pass
        
        # Default to 24 hours ago
        return datetime.now() - timedelta(days=1)
    
    def _cleanup_old_backups(self, days: int = 30) -> int:
        """
        Delete backups older than specified days.
        
        Args:
            days: Keep backups from last N days.
        
        Returns:
            Number of backups deleted.
        """
        logger.info(f"Cleaning up backups older than {days} days")
        
        try:
            cutoff = datetime.now() - timedelta(days=days)
            deleted = 0
            
            for backup_type in ['full', 'incremental']:
                response = self.s3.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=f"backups/{backup_type}/"
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['LastModified'].replace(tzinfo=None) < cutoff:
                            self.s3.delete_object(
                                Bucket=self.bucket,
                                Key=obj['Key']
                            )
                            deleted += 1
                            logger.debug(f"Deleted old backup: {obj['Key']}")
            
            logger.info(f"Deleted {deleted} old backups")
            return deleted
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="FinLoom Backup Manager")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Create full backup"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Create incremental backup"
    )
    parser.add_argument(
        "--restore",
        type=str,
        metavar="YYYYMMDD",
        help="Restore from backup date"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available backups"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Cleanup old backups (>30 days)"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        help="Override S3 bucket name"
    )
    
    args = parser.parse_args()
    
    setup_logging()
    
    try:
        manager = BackupManager(s3_bucket=args.bucket)
        
        if args.full:
            print("\n🔄 Creating full backup...")
            result = manager.create_full_backup()
            if result['success']:
                print(f"✅ Backup created: {result['backup_file']}")
                print(f"   Size: {result['size_mb']:.2f} MB")
                print(f"   Location: s3://{manager.bucket}/{result['s3_key']}\n")
            else:
                print(f"❌ Backup failed: {result['error']}\n")
                return 1
        
        elif args.incremental:
            print("\n🔄 Creating incremental backup...")
            result = manager.create_incremental_backup()
            if result['success']:
                print(f"✅ Incremental backup created")
                print(f"   Tables exported: {result['tables_exported']}")
                for file_info in result['files']:
                    print(f"   - {file_info['table']}: {file_info['records']} records")
                print()
            else:
                print(f"❌ Backup failed: {result['error']}\n")
                return 1
        
        elif args.restore:
            print(f"\n⚠️  WARNING: This will replace your current database!")
            confirm = input("Type 'yes' to continue: ")
            if confirm.lower() == 'yes':
                print("\n🔄 Restoring database...")
                success = manager.restore_from_backup(args.restore)
                if success:
                    print("✅ Database restored successfully\n")
                else:
                    print("❌ Restore failed\n")
                    return 1
            else:
                print("Restore cancelled\n")
        
        elif args.list:
            print("\n📋 Available Backups:\n")
            
            for backup_type in ['full', 'incremental']:
                backups = manager.list_backups(backup_type=backup_type, days=30)
                if backups:
                    print(f"{backup_type.upper()} BACKUPS:")
                    for backup in backups:
                        print(f"  {backup['date'].strftime('%Y-%m-%d %H:%M:%S')} - "
                              f"{Path(backup['key']).name} ({backup['size_mb']} MB)")
                    print()
        
        elif args.cleanup:
            print("\n🧹 Cleaning up old backups...")
            deleted = manager._cleanup_old_backups(days=30)
            print(f"✅ Deleted {deleted} old backups\n")
        
        else:
            parser.print_help()
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Backup operation failed: {e}", exc_info=True)
        print(f"\n❌ Error: {e}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
