"""
Database connection pool for DuckDB.

Provides thread-safe connection pooling to prevent contention and deadlocks.
"""

import queue
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

import duckdb

from ..utils.logger import get_logger

logger = get_logger("finloom.storage.pool")


class ConnectionPool:
    """
    Thread-safe connection pool for DuckDB.
    
    Manages a pool of database connections to prevent contention
    and improve performance under concurrent access.
    
    Usage:
        pool = ConnectionPool(db_path, pool_size=5)
        with pool.get_connection() as conn:
            result = conn.execute("SELECT * FROM table").fetchall()
    """
    
    def __init__(
        self,
        db_path: str | Path,
        pool_size: int = 5,
        timeout: float = 30.0,
        max_overflow: int = 2,
    ):
        """
        Initialize connection pool.
        
        Args:
            db_path: Path to DuckDB database file.
            pool_size: Number of connections to maintain.
            timeout: Seconds to wait for available connection.
            max_overflow: Additional connections allowed beyond pool_size.
        """
        self.db_path = Path(db_path)
        self.pool_size = pool_size
        self.timeout = timeout
        self.max_overflow = max_overflow
        
        # Connection pool (FIFO queue)
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size + max_overflow)
        self._all_connections = []
        self._lock = threading.Lock()
        self._overflow_count = 0
        
        # Statistics
        self._stats = {
            "connections_created": 0,
            "connections_acquired": 0,
            "connections_released": 0,
            "timeouts": 0,
            "overflow_created": 0,
        }
        
        # Initialize pool
        self._initialize_pool()
        
        logger.info(
            f"Connection pool initialized: "
            f"size={pool_size}, max_overflow={max_overflow}, "
            f"db={self.db_path.name}"
        )
    
    def _initialize_pool(self) -> None:
        """Create initial pool of connections."""
        for _ in range(self.pool_size):
            conn = self._create_connection()
            self._pool.put(conn)
    
    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new database connection."""
        try:
            conn = duckdb.connect(str(self.db_path), read_only=False)
            
            # Configure connection
            conn.execute("PRAGMA threads=4")
            conn.execute("PRAGMA memory_limit='2GB'")
            
            with self._lock:
                self._all_connections.append(conn)
                self._stats["connections_created"] += 1
            
            logger.debug(f"Created new connection (total: {len(self._all_connections)})")
            return conn
        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            raise
    
    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a connection from the pool.
        
        Yields:
            Database connection.
        
        Raises:
            TimeoutError: If no connection available within timeout.
        """
        conn = None
        start_time = time.time()
        
        try:
            # Try to get connection from pool
            try:
                conn = self._pool.get(block=True, timeout=self.timeout)
                with self._lock:
                    self._stats["connections_acquired"] += 1
                
            except queue.Empty:
                # Pool is empty, check if we can create overflow connection
                with self._lock:
                    if self._overflow_count < self.max_overflow:
                        self._overflow_count += 1
                        self._stats["overflow_created"] += 1
                        conn = self._create_connection()
                        logger.warning(
                            f"Created overflow connection "
                            f"({self._overflow_count}/{self.max_overflow})"
                        )
                    else:
                        self._stats["timeouts"] += 1
                        elapsed = time.time() - start_time
                        raise TimeoutError(
                            f"Failed to acquire connection within {elapsed:.2f}s. "
                            f"Pool size: {self.pool_size}, "
                            f"Overflow: {self._overflow_count}/{self.max_overflow}"
                        )
            
            # Verify connection is healthy
            if not self._is_connection_healthy(conn):
                logger.warning("Connection unhealthy, creating new one")
                conn.close()
                conn = self._create_connection()
            
            yield conn
            
        finally:
            # Return connection to pool
            if conn is not None:
                try:
                    # Check if this is an overflow connection
                    with self._lock:
                        is_overflow = self._overflow_count > 0
                    
                    if is_overflow:
                        # Close overflow connections instead of returning to pool
                        conn.close()
                        with self._lock:
                            self._overflow_count -= 1
                            if conn in self._all_connections:
                                self._all_connections.remove(conn)
                        logger.debug("Closed overflow connection")
                    else:
                        # Return to pool
                        self._pool.put(conn, block=False)
                        with self._lock:
                            self._stats["connections_released"] += 1
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
    
    def _is_connection_healthy(self, conn: duckdb.DuckDBPyConnection) -> bool:
        """Check if connection is healthy."""
        try:
            conn.execute("SELECT 1").fetchone()
            return True
        except Exception as e:
            logger.warning(f"Connection health check failed: {e}")
            return False
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            logger.info(f"Closing {len(self._all_connections)} connections")
            
            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")
            
            self._all_connections.clear()
            
            # Clear the queue
            while not self._pool.empty():
                try:
                    self._pool.get_nowait()
                except queue.Empty:
                    break
    
    def get_stats(self) -> dict:
        """Get connection pool statistics."""
        with self._lock:
            return {
                **self._stats,
                "pool_size": self.pool_size,
                "total_connections": len(self._all_connections),
                "available_connections": self._pool.qsize(),
                "overflow_active": self._overflow_count,
            }
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_all()
