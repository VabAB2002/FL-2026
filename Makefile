.PHONY: help install install-dev test test-cov lint format type-check clean setup-hooks run-daily run-backfill status neo4j-up neo4j-down neo4j-init neo4j-status neo4j-shell

# Default target
help:
	@echo "FinLoom - SEC Financial Data Pipeline"
	@echo ""
	@echo "Setup:"
	@echo "  make install       Install production dependencies"
	@echo "  make install-dev   Install development dependencies"
	@echo "  make setup-hooks   Install pre-commit hooks"
	@echo ""
	@echo "Development:"
	@echo "  make test          Run tests"
	@echo "  make test-cov      Run tests with coverage"
	@echo "  make lint          Run linters (ruff)"
	@echo "  make format        Format code (black + ruff fix)"
	@echo "  make type-check    Run type checker (mypy)"
	@echo "  make clean         Clean build artifacts"
	@echo ""
	@echo "Operations:"
	@echo "  make status        Show system status"
	@echo "  make run-backfill  Run data backfill from SEC"
	@echo ""
	@echo "Neo4j Graph Database:"
	@echo "  make neo4j-up      Start Neo4j in Docker"
	@echo "  make neo4j-down    Stop Neo4j"
	@echo "  make neo4j-init    Initialize Neo4j schema (run after neo4j-up)"
	@echo "  make neo4j-status  Check Neo4j connection status"
	@echo "  make neo4j-shell   Open Cypher shell"

# Setup targets
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

setup-hooks:
	pre-commit install
	@echo "Pre-commit hooks installed!"

# Development targets
test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

lint:
	ruff check src/ tests/ scripts/

format:
	black src/ tests/ scripts/
	ruff check --fix src/ tests/ scripts/

type-check:
	mypy src/

# Cleaning
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	@echo "Cleaned!"

# Operations targets
status:
	python finloom.py status

run-backfill:
	python scripts/backfill_data.py

# Quick quality check (format + lint + test)
check: format lint test
	@echo "All checks passed!"

# Neo4j targets
neo4j-up:
	@echo "Starting Neo4j..."
	docker-compose up -d neo4j
	@echo "Waiting for Neo4j to be ready (this may take 30 seconds)..."
	@sleep 5
	@echo "Neo4j is starting. Check status with: make neo4j-status"
	@echo "Browser UI: http://localhost:7474"
	@echo "Run 'make neo4j-init' to initialize schema"

neo4j-down:
	docker-compose down neo4j
	@echo "Neo4j stopped"

neo4j-init:
	@echo "Initializing Neo4j schema..."
	python scripts/init_neo4j_schema.py

neo4j-status:
	@echo "Checking Neo4j status..."
	@docker-compose ps neo4j
	@echo ""
	@echo "Testing connection..."
	@python -c "from src.graph.neo4j_client import Neo4jClient; client = Neo4jClient(); print('✓ Connected' if client.verify_connection() else '✗ Connection failed'); client.close()" 2>/dev/null || echo "✗ Cannot connect (is Neo4j running?)"

neo4j-shell:
	docker-compose exec neo4j cypher-shell -u neo4j -p $${NEO4J_PASSWORD:-finloom123}
