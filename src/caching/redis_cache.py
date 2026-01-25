"""
Redis-based query result caching.

Dramatically speeds up repeated queries.
"""

import hashlib
import json
import pickle
from typing import Any, Callable, Optional
from functools import wraps

import redis

from ..utils.logger import get_logger
from ..config.env_config import get_env_config

logger = get_logger("finloom.caching.redis_cache")


class RedisCache:
    """
    Redis-based caching layer for query results.
    
    Provides TTL-based caching with automatic invalidation.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        default_ttl: int = 3600,
        enabled: bool = True
    ):
        """
        Initialize Redis cache.
        
        Args:
            host: Redis host.
            port: Redis port.
            db: Redis database number.
            password: Redis password.
            default_ttl: Default TTL in seconds (1 hour).
            enabled: Enable caching.
        """
        self.enabled = enabled
        self.default_ttl = default_ttl
        
        if not enabled:
            logger.info("Redis cache disabled")
            self.client = None
            return
        
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=False,  # We'll handle encoding
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            self.client.ping()
            logger.info(f"Redis cache connected: {host}:{port}/{db}")
            
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis connection failed: {e}. Caching disabled.")
            self.client = None
            self.enabled = False
    
    def _make_key(self, namespace: str, key: str) -> str:
        """Create cache key."""
        return f"finloom:{namespace}:{key}"
    
    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage."""
        return pickle.dumps(value)
    
    def _deserialize(self, data: bytes) -> Any:
        """Deserialize stored value."""
        return pickle.loads(data)
    
    def get(self, namespace: str, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            namespace: Cache namespace.
            key: Cache key.
        
        Returns:
            Cached value or None if not found.
        """
        if not self.enabled or not self.client:
            return None
        
        try:
            cache_key = self._make_key(namespace, key)
            data = self.client.get(cache_key)
            
            if data:
                logger.debug(f"Cache hit: {namespace}:{key}")
                return self._deserialize(data)
            else:
                logger.debug(f"Cache miss: {namespace}:{key}")
                return None
                
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
    
    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set value in cache.
        
        Args:
            namespace: Cache namespace.
            key: Cache key.
            value: Value to cache.
            ttl: Time-to-live in seconds.
        
        Returns:
            True if successful.
        """
        if not self.enabled or not self.client:
            return False
        
        try:
            cache_key = self._make_key(namespace, key)
            data = self._serialize(value)
            ttl = ttl or self.default_ttl
            
            self.client.setex(cache_key, ttl, data)
            logger.debug(f"Cache set: {namespace}:{key} (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False
    
    def delete(self, namespace: str, key: str) -> bool:
        """
        Delete value from cache.
        
        Args:
            namespace: Cache namespace.
            key: Cache key.
        
        Returns:
            True if successful.
        """
        if not self.enabled or not self.client:
            return False
        
        try:
            cache_key = self._make_key(namespace, key)
            self.client.delete(cache_key)
            logger.debug(f"Cache delete: {namespace}:{key}")
            return True
            
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False
    
    def invalidate_namespace(self, namespace: str) -> int:
        """
        Invalidate all keys in namespace.
        
        Args:
            namespace: Cache namespace to invalidate.
        
        Returns:
            Number of keys deleted.
        """
        if not self.enabled or not self.client:
            return 0
        
        try:
            pattern = f"finloom:{namespace}:*"
            keys = self.client.keys(pattern)
            
            if keys:
                deleted = self.client.delete(*keys)
                logger.info(f"Invalidated {deleted} keys in namespace: {namespace}")
                return deleted
            
            return 0
            
        except Exception as e:
            logger.error(f"Cache invalidate error: {e}")
            return 0
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        if not self.enabled or not self.client:
            return {"enabled": False}
        
        try:
            info = self.client.info('stats')
            return {
                "enabled": True,
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "total_commands": info.get('total_commands_processed', 0),
            }
        except Exception as e:
            logger.error(f"Cache stats error: {e}")
            return {"enabled": True, "error": str(e)}


# Global cache instance
_cache: Optional[RedisCache] = None


def get_cache() -> RedisCache:
    """Get or create global Redis cache."""
    global _cache
    
    if _cache is None:
        config = get_env_config()
        _cache = RedisCache(
            host=config.get('redis.host', 'localhost'),
            port=config.get('redis.port', 6379),
            db=config.get('redis.db', 0),
            password=config.get('redis.password'),
            default_ttl=config.get('redis.default_ttl', 3600),
            enabled=config.get('features.caching_enabled', False)
        )
    
    return _cache


def cached(
    namespace: str,
    ttl: Optional[int] = None,
    key_func: Optional[Callable] = None
):
    """
    Decorator for caching function results.
    
    Usage:
        @cached('filings', ttl=3600)
        def get_filing(cik, accession):
            # Expensive query
            return filing
    
    Args:
        namespace: Cache namespace.
        ttl: Time-to-live in seconds.
        key_func: Function to generate cache key from args.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = get_cache()
            
            if not cache.enabled:
                return func(*args, **kwargs)
            
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default: hash of args + kwargs
                key_data = (args, tuple(sorted(kwargs.items())))
                key_hash = hashlib.md5(
                    str(key_data).encode()
                ).hexdigest()
                cache_key = f"{func.__name__}:{key_hash}"
            
            # Try cache
            result = cache.get(namespace, cache_key)
            if result is not None:
                return result
            
            # Cache miss - execute function
            result = func(*args, **kwargs)
            
            # Store in cache
            cache.set(namespace, cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator


class QueryCache:
    """
    High-level query cache for database results.
    
    Provides convenient caching for common query patterns.
    """
    
    def __init__(self, cache: Optional[RedisCache] = None):
        """
        Initialize query cache.
        
        Args:
            cache: Redis cache instance.
        """
        self.cache = cache or get_cache()
    
    def get_filing(self, accession_number: str) -> Optional[dict]:
        """Get cached filing."""
        return self.cache.get('filings', accession_number)
    
    def set_filing(self, accession_number: str, filing: dict, ttl: int = 3600):
        """Cache filing data."""
        self.cache.set('filings', accession_number, filing, ttl)
    
    def get_company_filings(self, cik: str) -> Optional[list]:
        """Get cached company filings list."""
        return self.cache.get('company_filings', cik)
    
    def set_company_filings(self, cik: str, filings: list, ttl: int = 1800):
        """Cache company filings list."""
        self.cache.set('company_filings', cik, filings, ttl)
    
    def get_normalized_metrics(
        self,
        ticker: str,
        fiscal_year: int
    ) -> Optional[list]:
        """Get cached normalized metrics."""
        key = f"{ticker}:{fiscal_year}"
        return self.cache.get('normalized_metrics', key)
    
    def set_normalized_metrics(
        self,
        ticker: str,
        fiscal_year: int,
        metrics: list,
        ttl: int = 7200
    ):
        """Cache normalized metrics."""
        key = f"{ticker}:{fiscal_year}"
        self.cache.set('normalized_metrics', key, metrics, ttl)
    
    def invalidate_company(self, cik: str):
        """Invalidate all cached data for a company."""
        self.cache.delete('company_filings', cik)
        logger.info(f"Invalidated cache for company: {cik}")
    
    def invalidate_all(self):
        """Invalidate all cached query results."""
        for namespace in ['filings', 'company_filings', 'normalized_metrics']:
            count = self.cache.invalidate_namespace(namespace)
            logger.info(f"Invalidated {count} keys in {namespace}")


# Example usage
@cached('filings', ttl=3600)
def get_filing_cached(db, accession_number: str):
    """Example: Get filing with caching."""
    return db.connection.execute(
        "SELECT * FROM filings WHERE accession_number = ?",
        [accession_number]
    ).fetchone()


@cached('metrics', ttl=7200, key_func=lambda db, ticker, year: f"{ticker}:{year}")
def get_metrics_cached(db, ticker: str, year: int):
    """Example: Get metrics with custom cache key."""
    return db.connection.execute(
        "SELECT * FROM normalized_financials WHERE company_ticker = ? AND fiscal_year = ?",
        [ticker, year]
    ).fetchall()
