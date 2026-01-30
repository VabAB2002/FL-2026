.PHONY: help install install-dev test test-cov lint format type-check clean setup-hooks run-daily run-backfill status neo4j-up neo4j-down neo4j-init neo4j-status neo4j-shell chunk build-graph communities extract extract-llm backfill

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
	@echo "Pipelines:"
	@echo "  make backfill      Backfill SEC filings (download + parse)"
	@echo "  make extract       Extract entities (SpaCy NER)"
	@echo "  make extract-llm   Augment entities with LLM extraction"
	@echo "  make chunk         Chunk filings for RAG"
	@echo "  make build-graph   Build Neo4j knowledge graph"
	@echo "  make communities   Detect communities + summarize"
	@echo "  make embed         Generate embeddings and upload to Qdrant"
	@echo "  make embed-test    Test embeddings with 100 chunks"
	@echo ""
	@echo "Neo4j Graph Database:"
	@echo "  make neo4j-up      Start Neo4j in Docker"
	@echo "  make neo4j-down    Stop Neo4j"
	@echo "  make neo4j-init    Initialize Neo4j schema (run after neo4j-up)"
	@echo "  make neo4j-status  Check Neo4j connection status"
	@echo "  make neo4j-shell   Open Cypher shell"
	@echo ""
	@echo "Qdrant Vector Database:"
	@echo "  make qdrant-up     Start Qdrant in Docker"
	@echo "  make qdrant-down   Stop Qdrant"
	@echo "  make qdrant-status Check Qdrant connection status"
	@echo ""
	@echo "Meilisearch Keyword Search:"
	@echo "  make meili-up      Start Meilisearch in Docker"
	@echo "  make meili-down    Stop Meilisearch"
	@echo "  make meili-status  Check Meilisearch connection status"
	@echo "  make meili-index   Index chunks in Meilisearch"

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
	@echo "No tests directory - skipping tests"

test-cov:
	@echo "No tests directory - skipping coverage"

lint:
	ruff check src/ scripts/

format:
	black src/ scripts/
	ruff check --fix src/ scripts/

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
	python -m src.downloads

# Pipeline targets
backfill:
	python -m src.downloads

extract:
	python -m src.readers

extract-llm:
	python -m src.readers.cli_llm

chunk:
	python -m src.splitter

build-graph:
	python -m src.graph

communities:
	python -m src.graph.cli_communities

embed:
	python -m src.vectors

embed-test:
	python -m src.vectors --limit 100

# Quick quality check (format + lint + test)
check: format lint test
	@echo "All checks passed!"

# Qdrant targets
qdrant-up:
	@echo "Starting Qdrant..."
	docker-compose up -d qdrant
	@echo "Waiting for Qdrant to be ready..."
	@sleep 3
	@echo "Qdrant is running. Web UI: http://localhost:6333/dashboard"

qdrant-down:
	docker-compose down qdrant
	@echo "Qdrant stopped"

qdrant-status:
	@echo "Checking Qdrant status..."
	@docker-compose ps qdrant
	@echo ""
	@echo "Testing connection..."
	@curl -s http://localhost:6333/healthz && echo "✓ Connected" || echo "✗ Connection failed"

# Meilisearch targets
meili-up:
	@echo "Starting Meilisearch..."
	docker-compose up -d meilisearch

meili-down:
	@echo "Stopping Meilisearch..."
	docker-compose down meilisearch

meili-status:
	@echo "Checking Meilisearch status..."
	@docker-compose ps meilisearch
	@echo ""
	@echo "Testing connection..."
	@curl -s http://localhost:7700/health && echo "✓ Connected" || echo "✗ Connection failed"

meili-index:
	@echo "Indexing chunks in Meilisearch..."
	python3 -m src.retrieval.index_meilisearch

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
	@python -c "from src.graph.graph_connector import Neo4jClient; client = Neo4jClient(); print('✓ Connected' if client.verify_connection() else '✗ Connection failed'); client.close()" 2>/dev/null || echo "✗ Cannot connect (is Neo4j running?)"

neo4j-shell:
	docker-compose exec neo4j cypher-shell -u neo4j -p $${NEO4J_PASSWORD:-finloom123}
