# Repository Pattern in FinLoom

## Overview

FinLoom uses the Repository Pattern for domain data access, providing a clean separation between business logic and data persistence.

## When to Use Each Approach

### Use Repository Pattern (Abstraction)

Use repositories when working with **domain entities**:

- `CompanyRepository` - Company data operations
- `FilingRepository` - Filing data operations
- `FactRepository` - XBRL fact data operations
- `SectionRepository` - Document section operations
- `NormalizedMetricsRepository` - Normalized financial metrics

**Examples:**
```python
from src.storage.repositories import get_filing_repository

filing_repo = get_filing_repository()
filing = filing_repo.get_filing(accession_number)
```

**Benefits:**
- Type-safe interfaces via Protocol classes
- Easy to mock for testing
- Abstracts database implementation details
- Supports dependency injection

### Use Direct Database Access

Use `Database` class directly for:

1. **System Utilities** - Health checks, monitoring, metrics collection
2. **Data Validation** - Quality scoring, reconciliation, data integrity checks
3. **Cross-table Analysis** - Complex queries spanning multiple tables
4. **Custom Reporting** - Ad-hoc queries and analytics

**Examples:**
```python
from src.storage.database import Database

db = Database()
result = db.connection.execute("""
    SELECT COUNT(*) FROM facts 
    WHERE accession_number = ?
""", [accession_number]).fetchone()
```

**When direct access is appropriate:**
- `src/validation/quality_scorer.py` - Custom quality metrics
- `src/validation/reconciliation.py` - Cross-table reconciliation
- `src/monitoring/health.py` - System health checks
- Scripts in `scripts/` directory - One-off operations

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                   Application Layer                      │
│  (Business Logic, API Endpoints, CLI Commands)          │
└────────────────┬───────────────────┬────────────────────┘
                 │                   │
         ┌───────▼────────┐  ┌──────▼──────────┐
         │  Repositories  │  │  Direct Access  │
         │  (Domain Data) │  │  (Utilities)    │
         └───────┬────────┘  └──────┬──────────┘
                 │                   │
         ┌───────▼───────────────────▼────────┐
         │       Database Connection           │
         │          (DuckDB)                   │
         └─────────────────────────────────────┘
```

## Repository Protocol Definitions

All repository protocols are defined in `src/core/repository.py`:

```python
from typing import Protocol

class FilingRepository(Protocol):
    """Repository for filing data access."""
    
    def get_filing(self, accession_number: str) -> Optional[dict]:
        """Get filing by accession number."""
        ...
    
    def save_filing(self, filing: dict) -> int:
        """Save a filing and return its ID."""
        ...
```

## Implementation

Concrete implementations are in `src/storage/repositories.py`:

```python
class DuckDBFilingRepository:
    """DuckDB implementation of FilingRepository."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_filing(self, accession_number: str) -> Optional[dict]:
        # Implementation details...
        pass
```

## Singleton Pattern

Repositories use singleton pattern for efficiency:

```python
from src.storage.repositories import (
    get_filing_repository,
    get_fact_repository,
    get_company_repository
)

# Get singleton instances (reuses connection)
filing_repo = get_filing_repository()
fact_repo = get_fact_repository()
```

## Dependency Injection

For testing and flexibility, repositories support dependency injection:

```python
class ConceptMapper:
    def __init__(
        self,
        fact_repo: Optional[FactRepository] = None,
        mapping_repo: Optional[MappingRepository] = None,
    ):
        self.fact_repo = fact_repo or get_fact_repository()
        self.mapping_repo = mapping_repo or get_mapping_repository()
```

## Testing

Repositories can be easily mocked for unit tests:

```python
from unittest.mock import Mock

def test_concept_mapper():
    # Mock repositories
    mock_fact_repo = Mock(spec=FactRepository)
    mock_fact_repo.get_facts_by_filing.return_value = [...]
    
    # Inject mocks
    mapper = ConceptMapper(fact_repo=mock_fact_repo)
    
    # Test business logic without database
    result = mapper.normalize_filing("test-accession")
    assert result is not None
```

## Best Practices

### DO

✅ Use repositories for domain entity operations  
✅ Use direct database access for utilities and analytics  
✅ Depend on Protocol types, not concrete implementations  
✅ Use dependency injection for testability  
✅ Keep repository methods focused and single-purpose  

### DON'T

❌ Don't use repositories for complex cross-table queries  
❌ Don't add business logic to repository methods  
❌ Don't bypass repositories for domain entity CRUD  
❌ Don't create repositories for non-domain entities  
❌ Don't use direct SQL in business logic (use repositories)  

## Migration Guide

If you need to refactor code to use repositories:

### Before (Direct Database Access)
```python
def process_filing(accession_number: str):
    db = Database()
    filing = db.connection.execute("""
        SELECT * FROM filings 
        WHERE accession_number = ?
    """, [accession_number]).fetchone()
    # Process filing...
```

### After (Repository Pattern)
```python
from src.storage.repositories import get_filing_repository

def process_filing(accession_number: str):
    filing_repo = get_filing_repository()
    filing = filing_repo.get_filing(accession_number)
    # Process filing...
```

## Summary

- **Domain Operations** → Use Repositories
- **Utilities & Analytics** → Direct Database Access
- **Testing** → Mock Repositories
- **Type Safety** → Protocol Interfaces

This pattern provides the right balance between abstraction and practicality, making the codebase maintainable while allowing flexibility where needed.
