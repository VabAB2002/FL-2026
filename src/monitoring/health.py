"""
Health check endpoints for FinLoom.

Provides Kubernetes-style liveness and readiness probes.
"""

import json
import shutil
from datetime import datetime
from typing import Dict, Optional

import requests
from fastapi import FastAPI, Response, status

from ..storage.database import Database
from ..utils.logger import get_logger

logger = get_logger("finloom.monitoring.health")

app = FastAPI(title="FinLoom Health Checks")


class HealthChecker:
    """
    Health check implementation.
    
    Performs various health checks on system components.
    """
    
    def __init__(self, db: Optional[Database] = None):
        """
        Initialize health checker.
        
        Args:
            db: Database instance for health checks.
        """
        self.db = db or Database()
        self.start_time = datetime.now()
    
    def check_database(self) -> Dict:
        """Check database connectivity and health."""
        try:
            start = datetime.now()
            result = self.db.connection.execute("SELECT 1").fetchone()
            latency_ms = (datetime.now() - start).total_seconds() * 1000
            
            # Check database size
            db_size = self.db.connection.execute(
                "SELECT SUM(total_blocks * block_size) FROM pragma_database_size()"
            ).fetchone()
            
            return {
                "status": "healthy",
                "latency_ms": round(latency_ms, 2),
                "size_bytes": db_size[0] if db_size else 0,
                "connection_count": len(self.db._connection.__dict__) if self.db._connection else 0
            }
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def check_sec_api(self) -> Dict:
        """Check SEC API availability."""
        try:
            start = datetime.now()
            response = requests.head(
                "https://www.sec.gov",
                timeout=5,
                headers={"User-Agent": "FinLoom Health Check"}
            )
            latency_ms = (datetime.now() - start).total_seconds() * 1000
            
            return {
                "status": "healthy" if response.status_code < 500 else "degraded",
                "status_code": response.status_code,
                "latency_ms": round(latency_ms, 2)
            }
        except requests.Timeout:
            return {
                "status": "unhealthy",
                "error": "timeout"
            }
        except Exception as e:
            logger.error(f"SEC API health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def check_disk_space(self) -> Dict:
        """Check available disk space."""
        try:
            disk = shutil.disk_usage("/")
            free_percent = (disk.free / disk.total) * 100
            free_gb = disk.free / (1024**3)
            
            # Consider unhealthy if less than 10% or 5GB free
            is_healthy = free_percent > 10 and free_gb > 5
            
            return {
                "status": "healthy" if is_healthy else "unhealthy",
                "free_percent": round(free_percent, 2),
                "free_gb": round(free_gb, 2),
                "total_gb": round(disk.total / (1024**3), 2),
                "warning": None if is_healthy else "Low disk space"
            }
        except Exception as e:
            logger.error(f"Disk space check failed: {e}")
            return {
                "status": "unknown",
                "error": str(e)
            }
    
    def check_memory(self) -> Dict:
        """Check memory usage (basic check)."""
        try:
            import psutil
            memory = psutil.virtual_memory()
            
            return {
                "status": "healthy" if memory.percent < 90 else "degraded",
                "used_percent": memory.percent,
                "available_gb": round(memory.available / (1024**3), 2)
            }
        except ImportError:
            # psutil not available
            return {
                "status": "unknown",
                "error": "psutil not installed"
            }
        except Exception as e:
            return {
                "status": "unknown",
                "error": str(e)
            }
    
    def get_uptime(self) -> Dict:
        """Get system uptime."""
        uptime = datetime.now() - self.start_time
        return {
            "uptime_seconds": int(uptime.total_seconds()),
            "uptime_human": str(uptime).split('.')[0]
        }


# Global health checker instance
_health_checker: Optional[HealthChecker] = None


def get_health_checker() -> HealthChecker:
    """Get or create global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


# =============================================================================
# Health Check Endpoints
# =============================================================================

@app.get("/health/live")
def liveness_probe():
    """
    Kubernetes liveness probe.
    
    Returns 200 if the application is running.
    This is a simple check that the process is alive.
    """
    return {
        "status": "alive",
        "timestamp": datetime.now().isoformat(),
        "service": "finloom"
    }


@app.get("/health/ready")
def readiness_probe():
    """
    Kubernetes readiness probe.
    
    Returns 200 if the application is ready to accept traffic.
    Checks all critical dependencies.
    """
    checker = get_health_checker()
    
    checks = {
        "database": checker.check_database(),
        "sec_api": checker.check_sec_api(),
        "disk": checker.check_disk_space()
    }
    
    # System is ready if all critical checks are healthy
    critical_checks = ["database", "disk"]
    is_ready = all(
        checks[name]["status"] == "healthy"
        for name in critical_checks
    )
    
    # SEC API degradation is acceptable, just warning
    if checks["sec_api"]["status"] != "healthy":
        logger.warning("SEC API health check degraded")
    
    response_status = status.HTTP_200_OK if is_ready else status.HTTP_503_SERVICE_UNAVAILABLE
    
    return Response(
        content=json.dumps({
            "status": "ready" if is_ready else "not_ready",
            "timestamp": datetime.now().isoformat(),
            "checks": checks
        }, indent=2),
        status_code=response_status,
        media_type="application/json"
    )


@app.get("/health/startup")
def startup_probe():
    """
    Kubernetes startup probe.
    
    Returns 200 once the application has completed initialization.
    """
    checker = get_health_checker()
    
    # Check if database is initialized
    db_check = checker.check_database()
    
    is_started = db_check["status"] == "healthy"
    response_status = status.HTTP_200_OK if is_started else status.HTTP_503_SERVICE_UNAVAILABLE
    
    return Response(
        content=json.dumps({
            "status": "started" if is_started else "starting",
            "timestamp": datetime.now().isoformat(),
            "database": db_check
        }, indent=2),
        status_code=response_status,
        media_type="application/json"
    )


@app.get("/health/detailed")
def detailed_health():
    """
    Detailed health check with all component status.
    
    Useful for monitoring dashboards.
    """
    checker = get_health_checker()
    
    checks = {
        "database": checker.check_database(),
        "sec_api": checker.check_sec_api(),
        "disk": checker.check_disk_space(),
        "memory": checker.check_memory(),
        "uptime": checker.get_uptime()
    }
    
    # Overall health status
    statuses = [
        check.get("status", "unknown")
        for check in checks.values()
        if isinstance(check, dict) and "status" in check
    ]
    
    if "unhealthy" in statuses:
        overall_status = "unhealthy"
    elif "degraded" in statuses:
        overall_status = "degraded"
    else:
        overall_status = "healthy"
    
    return {
        "status": overall_status,
        "timestamp": datetime.now().isoformat(),
        "service": "finloom",
        "version": "1.0.0",
        "checks": checks
    }


@app.get("/")
def root():
    """Root endpoint with service information."""
    return {
        "service": "finloom",
        "version": "1.0.0",
        "health_endpoints": {
            "liveness": "/health/live",
            "readiness": "/health/ready",
            "startup": "/health/startup",
            "detailed": "/health/detailed"
        }
    }


# =============================================================================
# Standalone Server
# =============================================================================

def start_health_server(port: int = 8000, host: str = "0.0.0.0"):
    """
    Start health check server.
    
    Args:
        port: Port to listen on.
        host: Host to bind to.
    """
    import uvicorn
    
    logger.info(f"Starting health check server on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    start_health_server()
