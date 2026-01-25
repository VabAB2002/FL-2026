"""
Alerting system for FinLoom.

Sends alerts to Slack, PagerDuty, or email for critical issues.
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

import requests

from ..utils.config import get_settings
from ..utils.logger import get_logger

logger = get_logger("finloom.monitoring.alerts")


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Alert message."""
    severity: AlertSeverity
    title: str
    message: str
    context: Optional[Dict] = None
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class AlertManager:
    """
    Manages alerts across multiple channels.
    
    Supports Slack, PagerDuty, and email notifications.
    """
    
    def __init__(
        self,
        slack_webhook_url: Optional[str] = None,
        pagerduty_key: Optional[str] = None,
        email_enabled: bool = False,
    ):
        """
        Initialize alert manager.
        
        Args:
            slack_webhook_url: Slack incoming webhook URL.
            pagerduty_key: PagerDuty integration key.
            email_enabled: Enable email alerts.
        """
        self.slack_webhook = slack_webhook_url
        self.pagerduty_key = pagerduty_key
        self.email_enabled = email_enabled
        
        self._alert_history = []
        self._rate_limit_window = 300  # 5 minutes
        self._rate_limit_count = {}
        
        logger.info(
            f"Alert manager initialized. "
            f"Slack: {'enabled' if slack_webhook_url else 'disabled'}, "
            f"PagerDuty: {'enabled' if pagerduty_key else 'disabled'}"
        )
    
    def send_alert(
        self,
        severity: str | AlertSeverity,
        title: str,
        message: str,
        context: Optional[Dict] = None
    ) -> bool:
        """
        Send an alert.
        
        Args:
            severity: Alert severity (info, warning, error, critical).
            title: Alert title.
            message: Alert message.
            context: Additional context dict.
        
        Returns:
            True if alert sent successfully.
        """
        # Convert string to enum
        if isinstance(severity, str):
            severity = AlertSeverity(severity.lower())
        
        # Create alert object
        alert = Alert(
            severity=severity,
            title=title,
            message=message,
            context=context
        )
        
        # Check rate limiting
        if not self._check_rate_limit(alert):
            logger.warning(f"Alert rate limited: {title}")
            return False
        
        # Store in history
        self._alert_history.append(alert)
        
        # Send to appropriate channels based on severity
        success = True
        
        if severity in [AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
            # Send to Slack for errors and critical
            if self.slack_webhook:
                success &= self._send_slack(alert)
            
            # Send to PagerDuty only for critical
            if severity == AlertSeverity.CRITICAL and self.pagerduty_key:
                success &= self._trigger_pagerduty(alert)
        
        elif severity == AlertSeverity.WARNING:
            # Send warnings only to Slack
            if self.slack_webhook:
                success &= self._send_slack(alert)
        
        # INFO alerts are only logged
        logger.info(f"Alert sent: [{severity.value.upper()}] {title}")
        
        return success
    
    def _check_rate_limit(self, alert: Alert) -> bool:
        """Check if alert should be rate limited."""
        key = f"{alert.severity.value}:{alert.title}"
        now = time.time()
        
        # Clean old entries
        self._rate_limit_count = {
            k: v for k, v in self._rate_limit_count.items()
            if now - v < self._rate_limit_window
        }
        
        # Check rate limit (max 3 identical alerts per 5 minutes)
        if key in self._rate_limit_count:
            count = sum(1 for k, v in self._rate_limit_count.items() if k == key)
            if count >= 3:
                return False
        
        self._rate_limit_count[key] = now
        return True
    
    def _send_slack(self, alert: Alert) -> bool:
        """Send alert to Slack."""
        if not self.slack_webhook:
            return False
        
        try:
            # Color based on severity
            colors = {
                AlertSeverity.INFO: "#36a64f",
                AlertSeverity.WARNING: "#ff9900",
                AlertSeverity.ERROR: "#ff0000",
                AlertSeverity.CRITICAL: "#8B0000"
            }
            
            # Build fields from context
            fields = []
            if alert.context:
                for key, value in alert.context.items():
                    fields.append({
                        "title": key.replace('_', ' ').title(),
                        "value": str(value),
                        "short": True
                    })
            
            payload = {
                "attachments": [{
                    "color": colors.get(alert.severity, "#808080"),
                    "title": f"[{alert.severity.value.upper()}] {alert.title}",
                    "text": alert.message,
                    "fields": fields,
                    "footer": "FinLoom Data Pipeline",
                    "ts": int(alert.timestamp.timestamp())
                }]
            }
            
            response = requests.post(
                self.slack_webhook,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.debug(f"Slack alert sent: {alert.title}")
                return True
            else:
                logger.error(f"Slack alert failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def _trigger_pagerduty(self, alert: Alert) -> bool:
        """Trigger PagerDuty incident."""
        if not self.pagerduty_key:
            return False
        
        try:
            payload = {
                "routing_key": self.pagerduty_key,
                "event_action": "trigger",
                "payload": {
                    "summary": alert.title,
                    "severity": alert.severity.value,
                    "source": "finloom",
                    "timestamp": alert.timestamp.isoformat(),
                    "custom_details": {
                        "message": alert.message,
                        **(alert.context or {})
                    }
                }
            }
            
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10
            )
            
            if response.status_code == 202:
                logger.info(f"PagerDuty incident created: {alert.title}")
                return True
            else:
                logger.error(f"PagerDuty alert failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to trigger PagerDuty: {e}")
            return False
    
    def get_recent_alerts(self, minutes: int = 60) -> list:
        """Get alerts from last N minutes."""
        cutoff = datetime.now().timestamp() - (minutes * 60)
        return [
            a for a in self._alert_history
            if a.timestamp.timestamp() > cutoff
        ]


# Global alert manager instance
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get or create global alert manager."""
    global _alert_manager
    
    if _alert_manager is None:
        settings = get_settings()
        
        # Try to get webhook URLs from environment or config
        import os
        slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
        pagerduty_key = os.getenv('PAGERDUTY_INTEGRATION_KEY')
        
        _alert_manager = AlertManager(
            slack_webhook_url=slack_webhook,
            pagerduty_key=pagerduty_key
        )
    
    return _alert_manager


def send_alert(
    severity: str,
    title: str,
    message: str,
    context: Optional[Dict] = None
) -> bool:
    """
    Convenience function to send an alert.
    
    Args:
        severity: info, warning, error, critical.
        title: Alert title.
        message: Alert message.
        context: Additional context.
    
    Returns:
        True if sent successfully.
    """
    manager = get_alert_manager()
    return manager.send_alert(severity, title, message, context)
