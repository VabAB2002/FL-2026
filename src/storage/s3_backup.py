"""
S3 backup utilities for FinLoom SEC Data Pipeline.

Provides backup and restore functionality for raw data and database.
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from ..utils.config import get_absolute_path, get_env_settings, get_settings
from ..utils.logger import get_logger

logger = get_logger("finloom.storage.s3_backup")


class S3Backup:
    """
    S3 backup manager for SEC filing data.
    
    Handles incremental backups of raw files and database snapshots.
    """
    
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        aws_region: Optional[str] = None,
    ) -> None:
        """
        Initialize S3 backup manager.
        
        Args:
            bucket_name: S3 bucket name. Defaults to config value.
            aws_region: AWS region. Defaults to environment variable.
        """
        settings = get_settings()
        env = get_env_settings()
        
        self.bucket_name = bucket_name or settings.storage.s3.bucket_name
        self.aws_region = aws_region or env.aws_default_region
        
        self.raw_prefix = settings.storage.s3.raw_prefix
        self.processed_prefix = settings.storage.s3.processed_prefix
        self.database_prefix = settings.storage.s3.database_prefix
        
        self.raw_data_path = get_absolute_path(settings.storage.raw_data_path)
        self.processed_data_path = get_absolute_path(settings.storage.processed_data_path)
        self.database_path = get_absolute_path(settings.storage.database_path)
        
        # Initialize S3 client
        self.s3_client = boto3.client("s3", region_name=self.aws_region)
        
        logger.info(f"S3 backup initialized: bucket={self.bucket_name}")
    
    def bucket_exists(self) -> bool:
        """Check if the S3 bucket exists and is accessible."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                return False
            elif error_code == "403":
                logger.error(f"Access denied to bucket {self.bucket_name}")
                return False
            raise
    
    def create_bucket(self) -> bool:
        """
        Create the S3 bucket if it doesn't exist.
        
        Returns:
            True if bucket was created or already exists.
        """
        if self.bucket_exists():
            logger.info(f"Bucket {self.bucket_name} already exists")
            return True
        
        try:
            # Create bucket with location constraint for non-us-east-1
            if self.aws_region == "us-east-1":
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={
                        "LocationConstraint": self.aws_region
                    }
                )
            
            logger.info(f"Created bucket {self.bucket_name}")
            
            # Set lifecycle policy for cost optimization
            self._set_lifecycle_policy()
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create bucket: {e}")
            return False
    
    def _set_lifecycle_policy(self) -> None:
        """Set S3 lifecycle policy for cost optimization."""
        lifecycle_policy = {
            "Rules": [
                {
                    "ID": "TransitionToGlacier",
                    "Status": "Enabled",
                    "Filter": {
                        "Prefix": self.raw_prefix
                    },
                    "Transitions": [
                        {
                            "Days": 90,
                            "StorageClass": "GLACIER"
                        }
                    ]
                },
                {
                    "ID": "ExpireOldDatabaseBackups",
                    "Status": "Enabled",
                    "Filter": {
                        "Prefix": self.database_prefix
                    },
                    "Expiration": {
                        "Days": 365
                    },
                    "NoncurrentVersionExpiration": {
                        "NoncurrentDays": 30
                    }
                }
            ]
        }
        
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=lifecycle_policy
            )
            logger.info("Set S3 lifecycle policy")
        except ClientError as e:
            logger.warning(f"Failed to set lifecycle policy: {e}")
    
    def upload_file(
        self,
        local_path: Path,
        s3_key: str,
        metadata: Optional[dict] = None,
    ) -> bool:
        """
        Upload a single file to S3.
        
        Args:
            local_path: Local file path.
            s3_key: S3 object key.
            metadata: Optional metadata dict.
        
        Returns:
            True if upload succeeded.
        """
        try:
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata
            
            self.s3_client.upload_file(
                str(local_path),
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args if extra_args else None,
            )
            logger.debug(f"Uploaded {local_path} to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False
    
    def sync_raw_data(self, use_aws_cli: bool = True) -> bool:
        """
        Sync raw data directory to S3.
        
        Uses AWS CLI sync for efficiency if available.
        
        Args:
            use_aws_cli: Whether to use AWS CLI (recommended for large syncs).
        
        Returns:
            True if sync succeeded.
        """
        if not self.raw_data_path.exists():
            logger.warning(f"Raw data path does not exist: {self.raw_data_path}")
            return False
        
        s3_uri = f"s3://{self.bucket_name}/{self.raw_prefix}"
        
        if use_aws_cli:
            try:
                result = subprocess.run(
                    [
                        "aws", "s3", "sync",
                        str(self.raw_data_path),
                        s3_uri,
                        "--only-show-errors"
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                logger.info(f"Synced raw data to {s3_uri}")
                return True
            except subprocess.CalledProcessError as e:
                logger.error(f"AWS CLI sync failed: {e.stderr}")
                return False
            except FileNotFoundError:
                logger.warning("AWS CLI not found, falling back to boto3")
                use_aws_cli = False
        
        if not use_aws_cli:
            return self._sync_directory_boto3(self.raw_data_path, self.raw_prefix)
        
        return False
    
    def _sync_directory_boto3(self, local_dir: Path, s3_prefix: str) -> bool:
        """Sync directory to S3 using boto3 (slower but no CLI needed)."""
        success = True
        file_count = 0
        
        for local_path in local_dir.rglob("*"):
            if local_path.is_file():
                relative_path = local_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}{relative_path}"
                
                if not self.upload_file(local_path, s3_key):
                    success = False
                else:
                    file_count += 1
        
        logger.info(f"Uploaded {file_count} files to S3")
        return success
    
    def backup_database(self) -> Optional[str]:
        """
        Backup the DuckDB database to S3.
        
        Creates a timestamped backup file.
        
        Returns:
            S3 key of the backup, or None if failed.
        """
        if not self.database_path.exists():
            logger.warning(f"Database not found: {self.database_path}")
            return None
        
        # Create timestamped backup key
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        db_name = self.database_path.name
        s3_key = f"{self.database_prefix}{timestamp}_{db_name}"
        
        if self.upload_file(
            self.database_path,
            s3_key,
            metadata={"backup_timestamp": timestamp}
        ):
            logger.info(f"Database backed up to s3://{self.bucket_name}/{s3_key}")
            return s3_key
        
        return None
    
    def download_file(self, s3_key: str, local_path: Path) -> bool:
        """
        Download a file from S3.
        
        Args:
            s3_key: S3 object key.
            local_path: Local destination path.
        
        Returns:
            True if download succeeded.
        """
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                str(local_path)
            )
            logger.debug(f"Downloaded s3://{self.bucket_name}/{s3_key} to {local_path}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            return False
    
    def restore_database(self, backup_key: Optional[str] = None) -> bool:
        """
        Restore database from S3 backup.
        
        Args:
            backup_key: Specific backup key. If None, uses latest.
        
        Returns:
            True if restore succeeded.
        """
        if backup_key is None:
            backup_key = self._get_latest_database_backup()
            if not backup_key:
                logger.error("No database backup found in S3")
                return False
        
        logger.info(f"Restoring database from {backup_key}")
        return self.download_file(backup_key, self.database_path)
    
    def _get_latest_database_backup(self) -> Optional[str]:
        """Get the key of the latest database backup."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.database_prefix,
            )
            
            objects = response.get("Contents", [])
            if not objects:
                return None
            
            # Sort by last modified and get latest
            objects.sort(key=lambda x: x["LastModified"], reverse=True)
            return objects[0]["Key"]
            
        except ClientError as e:
            logger.error(f"Failed to list backups: {e}")
            return None
    
    def list_backups(self) -> list[dict]:
        """List all database backups."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.database_prefix,
            )
            
            backups = []
            for obj in response.get("Contents", []):
                backups.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                })
            
            backups.sort(key=lambda x: x["last_modified"], reverse=True)
            return backups
            
        except ClientError as e:
            logger.error(f"Failed to list backups: {e}")
            return []
    
    def get_storage_stats(self) -> dict:
        """Get storage statistics for the bucket."""
        stats = {
            "raw_objects": 0,
            "raw_size_bytes": 0,
            "processed_objects": 0,
            "processed_size_bytes": 0,
            "database_objects": 0,
            "database_size_bytes": 0,
        }
        
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            
            for prefix, count_key, size_key in [
                (self.raw_prefix, "raw_objects", "raw_size_bytes"),
                (self.processed_prefix, "processed_objects", "processed_size_bytes"),
                (self.database_prefix, "database_objects", "database_size_bytes"),
            ]:
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        stats[count_key] += 1
                        stats[size_key] += obj["Size"]
            
        except ClientError as e:
            logger.error(f"Failed to get storage stats: {e}")
        
        return stats
