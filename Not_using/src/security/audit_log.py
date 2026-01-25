"""
Audit logging for compliance and security.

Tracks all data modifications for regulatory compliance.
"""

import json
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Dict, Optional

from ..storage.database import Database
from ..utils.logger import get_correlation_id, get_logger

logger = get_logger("finloom.security.audit")

# Context for audit logging
current_user: ContextVar[str] = ContextVar('current_user', default='system')
current_ip: ContextVar[str] = ContextVar('current_ip', default=None)


def set_audit_context(user_id: str, ip_address: Optional[str] = None):
    """
    Set audit context for current operation.
    
    Args:
        user_id: User identifier.
        ip_address: IP address of requester.
    """
    current_user.set(user_id)
    if ip_address:
        current_ip.set(ip_address)


class AuditLogger:
    """
    Audit logger for compliance tracking.
    
    Logs all data modifications to immutable audit_log table.
    """
    
    def __init__(self, db: Database):
        """
        Initialize audit logger.
        
        Args:
            db: Database instance.
        """
        self.db = db
    
    def log_action(
        self,
        action: str,
        table_name: str,
        record_id: Optional[str] = None,
        old_value: Optional[Dict] = None,
        new_value: Optional[Dict] = None,
        query_text: Optional[str] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """
        Log a data modification action.
        
        Args:
            action: Action type (SELECT, INSERT, UPDATE, DELETE).
            table_name: Table being modified.
            record_id: Record identifier.
            old_value: Old record value (for UPDATE/DELETE).
            new_value: New record value (for INSERT/UPDATE).
            query_text: SQL query executed.
            success: Whether action succeeded.
            error_message: Error message if failed.
        """
        try:
            audit_id = self.db.connection.execute(
                "SELECT nextval('audit_log_id_seq')"
            ).fetchone()[0]
            
            self.db.connection.execute("""
                INSERT INTO audit_log (
                    id, timestamp, user_id, service_name, correlation_id,
                    action, table_name, record_id,
                    old_value, new_value,
                    ip_address, query_text,
                    success, error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            """, [
                audit_id,
                datetime.now(),
                current_user.get(),
                'finloom',
                get_correlation_id() or 'none',
                action.upper(),
                table_name,
                record_id,
                json.dumps(old_value) if old_value else None,
                json.dumps(new_value) if new_value else None,
                current_ip.get(),
                query_text,
                success,
                error_message
            ])
            
            logger.debug(
                f"Audit log: {action} on {table_name} "
                f"by {current_user.get()} "
                f"[{get_correlation_id()}]"
            )
            
        except Exception as e:
            # Never fail the main operation due to audit logging
            logger.error(f"Failed to write audit log: {e}")
    
    def log_select(
        self,
        table_name: str,
        record_count: int,
        query_text: Optional[str] = None
    ):
        """Log SELECT operation."""
        self.log_action(
            action='SELECT',
            table_name=table_name,
            query_text=query_text,
            new_value={'record_count': record_count}
        )
    
    def log_insert(
        self,
        table_name: str,
        record_id: str,
        new_value: Dict,
        query_text: Optional[str] = None
    ):
        """Log INSERT operation."""
        self.log_action(
            action='INSERT',
            table_name=table_name,
            record_id=record_id,
            new_value=new_value,
            query_text=query_text
        )
    
    def log_update(
        self,
        table_name: str,
        record_id: str,
        old_value: Dict,
        new_value: Dict,
        query_text: Optional[str] = None
    ):
        """Log UPDATE operation."""
        self.log_action(
            action='UPDATE',
            table_name=table_name,
            record_id=record_id,
            old_value=old_value,
            new_value=new_value,
            query_text=query_text
        )
    
    def log_delete(
        self,
        table_name: str,
        record_id: str,
        old_value: Dict,
        query_text: Optional[str] = None
    ):
        """Log DELETE operation."""
        self.log_action(
            action='DELETE',
            table_name=table_name,
            record_id=record_id,
            old_value=old_value,
            query_text=query_text
        )
    
    def get_audit_trail(
        self,
        table_name: Optional[str] = None,
        record_id: Optional[str] = None,
        user_id: Optional[str] = None,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get audit trail.
        
        Args:
            table_name: Filter by table.
            record_id: Filter by record ID.
            user_id: Filter by user.
            hours: Show last N hours.
        
        Returns:
            List of audit log entries.
        """
        query = """
            SELECT 
                timestamp, user_id, action, table_name, record_id,
                correlation_id, success, error_message
            FROM audit_log
            WHERE timestamp > now() - INTERVAL ? HOUR
        """
        params = [hours]
        
        if table_name:
            query += " AND table_name = ?"
            params.append(table_name)
        
        if record_id:
            query += " AND record_id = ?"
            params.append(record_id)
        
        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        
        query += " ORDER BY timestamp DESC LIMIT 1000"
        
        results = self.db.connection.execute(query, params).fetchall()
        
        return [
            {
                "timestamp": r[0],
                "user_id": r[1],
                "action": r[2],
                "table_name": r[3],
                "record_id": r[4],
                "correlation_id": r[5],
                "success": r[6],
                "error_message": r[7]
            }
            for r in results
        ]


def audit_decorator(action: str, table_name: str):
    """
    Decorator for automatic audit logging.
    
    Usage:
        @audit_decorator('INSERT', 'filings')
        def insert_filing(db, filing):
            # Your code
            pass
    """
    from functools import wraps
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            db = None
            # Try to find Database instance in args
            for arg in args:
                if isinstance(arg, Database):
                    db = arg
                    break
            
            if not db:
                # No database instance, skip audit logging
                return func(*args, **kwargs)
            
            auditor = AuditLogger(db)
            
            try:
                result = func(*args, **kwargs)
                
                # Log successful action
                auditor.log_action(
                    action=action,
                    table_name=table_name,
                    success=True
                )
                
                return result
                
            except Exception as e:
                # Log failed action
                auditor.log_action(
                    action=action,
                    table_name=table_name,
                    success=False,
                    error_message=str(e)
                )
                raise
        
        return wrapper
    return decorator


# Global audit logger instance
_audit_logger: Optional[AuditLogger] = None


def get_audit_logger() -> AuditLogger:
    """Get or create global audit logger."""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger(Database())
    return _audit_logger
