.PHONY: help install install-dev test test-cov lint format type-check clean setup-hooks run-daily run-backfill status

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
	@echo "  make run-daily     Run daily update pipeline"
	@echo "  make run-backfill  Run historical backfill"

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

run-daily:
	python scripts/02_daily_update.py

run-backfill:
	python scripts/01_backfill_historical.py

# Quick quality check (format + lint + test)
check: format lint test
	@echo "All checks passed!"
