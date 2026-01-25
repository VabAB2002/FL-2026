"""
Graceful degradation for processing pipeline.

Allows partial failures without stopping the entire pipeline.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from ..utils.logger import get_logger
from ..monitoring.alerts import send_alert

logger = get_logger("finloom.reliability.degradation")


class ServiceLevel(Enum):
    """Service level definitions."""
    FULL = "full"              # All features working
    DEGRADED = "degraded"      # Core features only
    MINIMAL = "minimal"        # Essential features only
    UNAVAILABLE = "unavailable"  # Service down


@dataclass
class DegradationRule:
    """Rule for graceful degradation."""
    service_name: str
    failure_threshold: int
    degraded_level: ServiceLevel
    fallback_action: Optional[Callable] = None
    skip_optional: bool = True


class GracefulDegradationManager:
    """
    Manages graceful degradation across the system.
    
    Allows partial failures without complete system shutdown.
    """
    
    def __init__(self):
        """Initialize degradation manager."""
        self.current_level = ServiceLevel.FULL
        self.service_failures: Dict[str, int] = {}
        self.degradation_rules: List[DegradationRule] = []
        self._setup_default_rules()
        
        logger.info("Graceful degradation manager initialized")
    
    def _setup_default_rules(self):
        """Set up default degradation rules."""
        # SEC API degradation
        self.add_rule(DegradationRule(
            service_name='sec_api',
            failure_threshold=5,
            degraded_level=ServiceLevel.DEGRADED,
            skip_optional=True
        ))
        
        # XBRL parsing degradation
        self.add_rule(DegradationRule(
            service_name='xbrl_parser',
            failure_threshold=3,
            degraded_level=ServiceLevel.DEGRADED,
            skip_optional=False
        ))
        
        # Section extraction (optional feature)
        self.add_rule(DegradationRule(
            service_name='section_extraction',
            failure_threshold=2,
            degraded_level=ServiceLevel.MINIMAL,
            skip_optional=True
        ))
    
    def add_rule(self, rule: DegradationRule):
        """Add degradation rule."""
        self.degradation_rules.append(rule)
        logger.debug(f"Added degradation rule for {rule.service_name}")
    
    def record_failure(self, service_name: str) -> ServiceLevel:
        """
        Record a service failure and check degradation rules.
        
        Args:
            service_name: Name of failing service.
        
        Returns:
            Current service level.
        """
        # Increment failure count
        self.service_failures[service_name] = self.service_failures.get(service_name, 0) + 1
        failure_count = self.service_failures[service_name]
        
        # Check degradation rules
        for rule in self.degradation_rules:
            if rule.service_name == service_name:
                if failure_count >= rule.failure_threshold:
                    old_level = self.current_level
                    self.current_level = rule.degraded_level
                    
                    if old_level != self.current_level:
                        logger.warning(
                            f"Service degraded: {service_name} - "
                            f"{old_level.value} -> {self.current_level.value}"
                        )
                        
                        # Send alert
                        send_alert(
                            severity='warning',
                            title=f'Service Degradation: {service_name}',
                            message=f'Service level changed to {self.current_level.value}',
                            context={
                                'service': service_name,
                                'failure_count': failure_count,
                                'old_level': old_level.value,
                                'new_level': self.current_level.value
                            }
                        )
                    
                    break
        
        return self.current_level
    
    def record_success(self, service_name: str):
        """Record a service success (reset failure count)."""
        if service_name in self.service_failures:
            old_count = self.service_failures[service_name]
            self.service_failures[service_name] = max(0, old_count - 1)
            
            # If all services recovered, restore to FULL
            if all(count == 0 for count in self.service_failures.values()):
                if self.current_level != ServiceLevel.FULL:
                    logger.info("All services recovered - restoring to FULL level")
                    self.current_level = ServiceLevel.FULL
    
    def should_skip_optional(self, feature: str) -> bool:
        """
        Check if optional feature should be skipped.
        
        Args:
            feature: Feature name.
        
        Returns:
            True if feature should be skipped.
        """
        if self.current_level == ServiceLevel.FULL:
            return False
        
        if self.current_level == ServiceLevel.MINIMAL:
            # In minimal mode, skip all optional features
            return True
        
        if self.current_level == ServiceLevel.DEGRADED:
            # Check specific rules
            for rule in self.degradation_rules:
                if rule.service_name == feature and rule.skip_optional:
                    return True
        
        return False
    
    def get_service_level(self) -> ServiceLevel:
        """Get current service level."""
        return self.current_level
    
    def is_feature_enabled(self, feature: str) -> bool:
        """
        Check if a feature is enabled at current service level.
        
        Args:
            feature: Feature name.
        
        Returns:
            True if feature is enabled.
        """
        if self.current_level == ServiceLevel.UNAVAILABLE:
            return False
        
        if self.current_level == ServiceLevel.MINIMAL:
            # Only core features in minimal mode
            core_features = ['download', 'xbrl_parse', 'store']
            return feature in core_features
        
        if self.current_level == ServiceLevel.DEGRADED:
            # Most features except optional ones
            optional_features = ['section_extraction', 'table_extraction', 'sentiment_analysis']
            return feature not in optional_features
        
        return True  # FULL level
    
    def get_status_report(self) -> Dict:
        """Get degradation status report."""
        return {
            "current_level": self.current_level.value,
            "service_failures": self.service_failures.copy(),
            "rules": [
                {
                    "service": rule.service_name,
                    "threshold": rule.failure_threshold,
                    "degraded_level": rule.degraded_level.value
                }
                for rule in self.degradation_rules
            ]
        }


# Global instance
_degradation_manager: Optional[GracefulDegradationManager] = None


def get_degradation_manager() -> GracefulDegradationManager:
    """Get or create global degradation manager."""
    global _degradation_manager
    if _degradation_manager is None:
        _degradation_manager = GracefulDegradationManager()
    return _degradation_manager


def with_degradation(service_name: str, optional: bool = False):
    """
    Decorator for graceful degradation.
    
    Usage:
        @with_degradation('xbrl_parser', optional=False)
        def parse_xbrl(filing):
            # Your code
            pass
    """
    from functools import wraps
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            manager = get_degradation_manager()
            
            # Check if feature should be skipped
            if optional and manager.should_skip_optional(service_name):
                logger.info(f"Skipping optional feature: {service_name} (degraded mode)")
                return None
            
            try:
                result = func(*args, **kwargs)
                manager.record_success(service_name)
                return result
                
            except Exception as e:
                level = manager.record_failure(service_name)
                
                if optional and level in [ServiceLevel.DEGRADED, ServiceLevel.MINIMAL]:
                    # Fail gracefully for optional features
                    logger.warning(f"Optional feature failed gracefully: {service_name} - {e}")
                    return None
                else:
                    # Re-raise for required features
                    raise
        
        return wrapper
    return decorator


class PipelineExecutor:
    """
    Pipeline executor with graceful degradation.
    
    Executes pipeline stages and continues on partial failures.
    """
    
    def __init__(self):
        """Initialize pipeline executor."""
        self.manager = get_degradation_manager()
    
    def execute_pipeline(
        self,
        stages: List[Dict[str, Any]],
        context: Dict[str, Any]
    ) -> Dict:
        """
        Execute pipeline with graceful degradation.
        
        Args:
            stages: List of pipeline stages with config.
            context: Execution context.
        
        Returns:
            Execution results.
        """
        results = {
            "success": [],
            "failed": [],
            "skipped": [],
            "context": context
        }
        
        for stage in stages:
            stage_name = stage['name']
            stage_func = stage['function']
            optional = stage.get('optional', False)
            
            # Check if stage is enabled
            if not self.manager.is_feature_enabled(stage_name):
                logger.info(f"Stage disabled at current service level: {stage_name}")
                results['skipped'].append({
                    "stage": stage_name,
                    "reason": "service_level"
                })
                continue
            
            # Check if optional stage should be skipped
            if optional and self.manager.should_skip_optional(stage_name):
                logger.info(f"Skipping optional stage: {stage_name}")
                results['skipped'].append({
                    "stage": stage_name,
                    "reason": "degraded_mode"
                })
                continue
            
            # Execute stage
            try:
                logger.info(f"Executing stage: {stage_name}")
                stage_result = stage_func(context)
                
                self.manager.record_success(stage_name)
                results['success'].append({
                    "stage": stage_name,
                    "result": stage_result
                })
                
                # Update context with stage results
                if stage_result:
                    context.update(stage_result)
                
            except Exception as e:
                logger.error(f"Stage failed: {stage_name} - {e}")
                
                self.manager.record_failure(stage_name)
                results['failed'].append({
                    "stage": stage_name,
                    "error": str(e),
                    "optional": optional
                })
                
                # Stop pipeline if required stage fails
                if not optional:
                    logger.error(f"Required stage failed, stopping pipeline: {stage_name}")
                    break
        
        # Log summary
        logger.info(
            f"Pipeline complete: {len(results['success'])} success, "
            f"{len(results['failed'])} failed, {len(results['skipped'])} skipped"
        )
        
        return results
