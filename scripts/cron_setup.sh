#!/bin/bash
# Cron Setup Script for FinLoom SEC Data Pipeline
# ================================================
#
# This script shows how to set up cron jobs for automation.
# Modify the paths to match your installation.
#
# Usage:
#   1. Edit the paths below
#   2. Run: crontab -e
#   3. Add the cron entries shown by this script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$PROJECT_DIR/venv/bin/python"
LOG_DIR="$PROJECT_DIR/logs"

echo "FinLoom Cron Setup"
echo "=================="
echo ""
echo "Project directory: $PROJECT_DIR"
echo "Python executable: $VENV_PYTHON"
echo "Log directory: $LOG_DIR"
echo ""
echo "Add the following lines to your crontab (run 'crontab -e'):"
echo ""
echo "# ==========================================="
echo "# FinLoom SEC Data Pipeline Automation"
echo "# ==========================================="
echo ""
echo "# Daily update - Check for new 10-K filings"
echo "# Runs every day at 9:00 AM"
echo "0 9 * * * cd $PROJECT_DIR && $VENV_PYTHON scripts/02_daily_update.py >> $LOG_DIR/cron_daily.log 2>&1"
echo ""
echo "# Weekly backup - Sync data to S3"
echo "# Runs every Sunday at 2:00 AM"
echo "0 2 * * 0 cd $PROJECT_DIR && $VENV_PYTHON scripts/03_backup_to_s3.py >> $LOG_DIR/cron_backup.log 2>&1"
echo ""
echo "# Monthly cleanup - Remove old log files"
echo "# Runs on the 1st of each month at 3:00 AM"
echo "0 3 1 * * find $LOG_DIR -name '*.log' -mtime +30 -delete"
echo ""
echo "# ==========================================="
echo ""
echo "To verify cron is working, check the log files after the scheduled times."
