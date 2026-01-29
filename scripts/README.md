# Scripts Directory

Production scripts for FinLoom data pipeline.

## Directory Structure

### 📦 `pipelines/`
Main production data pipelines that run regularly:
- `backfill_data.py` - Download & process SEC filings from EDGAR
- `extract_entities_from_filings.py` - Extract entities using SpaCy NER
- `add_llm_extraction_parallel.py` - Augment with LLM extraction (people, risks)
- `build_knowledge_graph.py` - Build Neo4j knowledge graph from entities

### 🔧 `utilities/`
One-off utilities and backfill scripts:
- `backfill_filing_sections.py` - Fix incomplete section extraction

### ⚙️ `setup/`
Initialization and setup scripts:
- `init_neo4j_schema.py` - Initialize Neo4j schema with indexes/constraints

## Usage

### Running Main Pipeline

```bash
# 1. Download and process filings
python scripts/pipelines/backfill_data.py

# 2. Extract entities
python scripts/pipelines/extract_entities_from_filings.py

# 3. Augment with LLM extraction
python scripts/pipelines/add_llm_extraction_parallel.py

# 4. Build knowledge graph
python scripts/pipelines/build_knowledge_graph.py --workers 4
```

### Setup

```bash
# Initialize Neo4j
python scripts/setup/init_neo4j_schema.py
```
