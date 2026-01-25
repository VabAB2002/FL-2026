# RAG Integration Guide

## Overview

This guide shows how to integrate the FinLoom unstructured data system with a RAG (Retrieval Augmented Generation) pipeline for intelligent Q&A over SEC filings.

## Architecture

```
User Question
     │
     ▼
┌─────────────────────────────────────────────┐
│  1. Question → Embedding                     │
│     (Sentence Transformers / OpenAI)         │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│  2. Vector Search                            │
│     Query DuckDB chunks by similarity        │
│     Return top-k chunks                      │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│  3. Context Loading                          │
│     - Parent section                         │
│     - Related tables                         │
│     - Cross-referenced content               │
│     - Metadata (company, date, section)      │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│  4. LLM Query                                │
│     GPT-4 / Claude with context              │
│     Generate answer with citations           │
└────────────────┬────────────────────────────┘
                 │
                 ▼
         Final Answer + Sources
```

## Step 1: Install Dependencies

```bash
pip install sentence-transformers openai duckdb pandas numpy
```

## Step 2: Generate Embeddings

### Using Sentence Transformers (Local)

```python
from sentence_transformers import SentenceTransformer
import duckdb
from tqdm import tqdm

# Load model
model = SentenceTransformer('all-MiniLM-L6-v2')  # Fast, good quality
# Or: model = SentenceTransformer('all-mpnet-base-v2')  # Slower, better quality

# Connect to database
conn = duckdb.connect('data/database/finloom.duckdb')

# Get all level-2 chunks (optimized for RAG)
chunks = conn.execute("""
    SELECT chunk_id, chunk_text
    FROM chunks
    WHERE chunk_level = 2
    ORDER BY chunk_id
""").fetchall()

print(f"Generating embeddings for {len(chunks)} chunks...")

# Generate embeddings in batches
batch_size = 100
for i in tqdm(range(0, len(chunks), batch_size)):
    batch = chunks[i:i+batch_size]
    
    # Extract texts
    texts = [chunk[1] for chunk in batch]
    
    # Generate embeddings
    embeddings = model.encode(texts, show_progress_bar=False)
    
    # Update database
    for j, (chunk_id, _) in enumerate(batch):
        embedding_list = embeddings[j].tolist()
        conn.execute("""
            UPDATE chunks
            SET embedding_vector = ?
            WHERE chunk_id = ?
        """, [embedding_list, chunk_id])
    
    conn.commit()

print("✅ Embeddings generated successfully")
conn.close()
```

### Using OpenAI Embeddings (Cloud)

```python
from openai import OpenAI
import duckdb
from tqdm import tqdm

client = OpenAI(api_key="your-api-key")

conn = duckdb.connect('data/database/finloom.duckdb')

chunks = conn.execute("""
    SELECT chunk_id, chunk_text
    FROM chunks
    WHERE chunk_level = 2
""").fetchall()

print(f"Generating OpenAI embeddings for {len(chunks)} chunks...")

for chunk_id, text in tqdm(chunks):
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    
    embedding = response.data[0].embedding
    
    conn.execute("""
        UPDATE chunks
        SET embedding_vector = ?
        WHERE chunk_id = ?
    """, [embedding, chunk_id])

conn.commit()
conn.close()
```

## Step 3: Build RAG Query Interface

```python
from sentence_transformers import SentenceTransformer
from openai import OpenAI
import duckdb
import numpy as np

class FinLoomRAG:
    """RAG interface for FinLoom SEC filings."""
    
    def __init__(
        self,
        db_path: str = "data/database/finloom.duckdb",
        embedding_model: str = "all-MiniLM-L6-v2",
        llm_model: str = "gpt-4-turbo-preview",
    ):
        self.db_path = db_path
        self.embedding_model = SentenceTransformer(embedding_model)
        self.llm_client = OpenAI()
        self.llm_model = llm_model
    
    def get_relevant_chunks(
        self,
        query: str,
        top_k: int = 5,
        ticker: str = None,
        section_types: list = None,
    ):
        """Get relevant chunks for a query."""
        # Generate query embedding
        query_embedding = self.embedding_model.encode(query)
        
        # Connect to database
        conn = duckdb.connect(self.db_path, read_only=True)
        
        # Build SQL query
        sql = """
            SELECT 
                c.chunk_id,
                c.chunk_text,
                c.heading,
                c.section_type,
                s.section_title,
                s.accession_number,
                comp.ticker,
                comp.company_name,
                f.filing_date,
                array_cosine_similarity(c.embedding_vector, ?) as similarity
            FROM chunks c
            JOIN sections s ON c.section_id = s.id
            JOIN filings f ON c.accession_number = f.accession_number
            JOIN companies comp ON f.cik = comp.cik
            WHERE c.chunk_level = 2
              AND c.embedding_vector IS NOT NULL
        """
        
        params = [query_embedding.tolist()]
        
        if ticker:
            sql += " AND comp.ticker = ?"
            params.append(ticker)
        
        if section_types:
            placeholders = ','.join(['?' for _ in section_types])
            sql += f" AND c.section_type IN ({placeholders})"
            params.extend(section_types)
        
        sql += " ORDER BY similarity DESC LIMIT ?"
        params.append(top_k)
        
        results = conn.execute(sql, params).fetchall()
        conn.close()
        
        return [
            {
                "chunk_id": r[0],
                "text": r[1],
                "heading": r[2],
                "section_type": r[3],
                "section_title": r[4],
                "accession_number": r[5],
                "ticker": r[6],
                "company_name": r[7],
                "filing_date": r[8],
                "similarity": r[9],
            }
            for r in results
        ]
    
    def get_chunk_context(self, chunk_id: str):
        """Get full context for a chunk (parent section, tables, etc.)."""
        conn = duckdb.connect(self.db_path, read_only=True)
        
        # Get chunk with section
        result = conn.execute("""
            SELECT 
                c.chunk_text,
                s.content_text as section_text,
                s.section_title,
                c.section_type
            FROM chunks c
            JOIN sections s ON c.section_id = s.id
            WHERE c.chunk_id = ?
        """, [chunk_id]).fetchone()
        
        if not result:
            return None
        
        chunk_text, section_text, section_title, section_type = result
        
        # Get related tables
        tables = conn.execute("""
            SELECT table_name, table_markdown
            FROM tables
            WHERE section_id = (
                SELECT section_id FROM chunks WHERE chunk_id = ?
            )
            LIMIT 3
        """, [chunk_id]).fetchall()
        
        conn.close()
        
        return {
            "chunk_text": chunk_text,
            "section_text": section_text,
            "section_title": section_title,
            "section_type": section_type,
            "tables": [{"name": t[0], "markdown": t[1]} for t in tables],
        }
    
    def ask(
        self,
        question: str,
        ticker: str = None,
        top_k: int = 5,
        include_context: bool = True,
    ):
        """Ask a question and get an answer with sources."""
        
        # 1. Get relevant chunks
        print(f"🔍 Searching for relevant information...")
        chunks = self.get_relevant_chunks(question, top_k=top_k, ticker=ticker)
        
        if not chunks:
            return {
                "answer": "No relevant information found.",
                "sources": [],
            }
        
        print(f"✅ Found {len(chunks)} relevant chunks")
        
        # 2. Build context
        context_parts = []
        sources = []
        
        for i, chunk in enumerate(chunks, 1):
            context_parts.append(f"""
[Source {i}] {chunk['company_name']} ({chunk['ticker']}) - {chunk['filing_date']}
Section: {chunk['section_title']}
{'-' * 60}
{chunk['text']}
""")
            
            sources.append({
                "id": i,
                "company": chunk['company_name'],
                "ticker": chunk['ticker'],
                "filing_date": str(chunk['filing_date']),
                "section": chunk['section_title'],
                "similarity": round(chunk['similarity'], 3),
            })
        
        context = "\n\n".join(context_parts)
        
        # 3. Query LLM
        print(f"💭 Generating answer...")
        
        messages = [
            {
                "role": "system",
                "content": """You are a financial analyst assistant. Answer questions based on SEC 10-K filing content provided.

Rules:
- Use ONLY information from the provided context
- Cite sources using [Source N] format
- If information isn't in context, say so
- Be precise with numbers and dates
- Explain financial concepts clearly"""
            },
            {
                "role": "user",
                "content": f"""Context from SEC filings:

{context}

Question: {question}

Please provide a comprehensive answer citing the sources."""
            }
        ]
        
        response = self.llm_client.chat.completions.create(
            model=self.llm_model,
            messages=messages,
            temperature=0.3,
        )
        
        answer = response.choices[0].message.content
        
        return {
            "answer": answer,
            "sources": sources,
            "chunks_used": len(chunks),
        }


# Example usage
if __name__ == "__main__":
    rag = FinLoomRAG()
    
    # Ask a question
    result = rag.ask(
        "What were Apple's main risk factors in 2024?",
        ticker="AAPL",
        top_k=5
    )
    
    print("\n" + "=" * 60)
    print("ANSWER:")
    print("=" * 60)
    print(result["answer"])
    print("\n" + "=" * 60)
    print("SOURCES:")
    print("=" * 60)
    for source in result["sources"]:
        print(f"[{source['id']}] {source['company']} ({source['ticker']}) - {source['filing_date']}")
        print(f"    Section: {source['section']}")
        print(f"    Similarity: {source['similarity']}")
```

## Step 4: Advanced Features

### Multi-Company Comparison

```python
def compare_companies(question: str, tickers: list[str]):
    """Compare multiple companies."""
    results = {}
    
    for ticker in tickers:
        result = rag.ask(question, ticker=ticker, top_k=3)
        results[ticker] = result
    
    return results

# Example
comparison = compare_companies(
    "What are the primary revenue sources?",
    tickers=["AAPL", "MSFT", "GOOGL"]
)
```

### Time-Series Analysis

```python
def analyze_over_time(question: str, ticker: str, years: list[int]):
    """Analyze across multiple years."""
    conn = duckdb.connect(rag.db_path, read_only=True)
    
    results = {}
    for year in years:
        # Filter chunks by filing year
        chunks = conn.execute("""
            SELECT c.chunk_id, c.chunk_text
            FROM chunks c
            JOIN filings f ON c.accession_number = f.accession_number
            JOIN companies comp ON f.cik = comp.cik
            WHERE comp.ticker = ?
              AND EXTRACT(YEAR FROM f.filing_date) = ?
              AND c.chunk_level = 2
        """, [ticker, year]).fetchall()
        
        # Process for this year
        # ... (similar to ask() method)
        
        results[year] = answer
    
    return results
```

### Hybrid Search (Keyword + Semantic)

```python
def hybrid_search(question: str, keywords: list[str], top_k: int = 10):
    """Combine keyword search with semantic search."""
    
    # 1. Keyword search
    keyword_chunks = []
    for keyword in keywords:
        chunks = conn.execute("""
            SELECT chunk_id, chunk_text
            FROM chunks
            WHERE chunk_text LIKE ?
              AND chunk_level = 2
            LIMIT 20
        """, [f"%{keyword}%"]).fetchall()
        keyword_chunks.extend(chunks)
    
    # 2. Semantic search
    semantic_chunks = rag.get_relevant_chunks(question, top_k=top_k)
    
    # 3. Merge and deduplicate
    # ... (combine results)
    
    return merged_results
```

## Step 5: Build Web API

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="FinLoom RAG API")
rag = FinLoomRAG()

class Question(BaseModel):
    question: str
    ticker: str = None
    top_k: int = 5

@app.post("/ask")
async def ask_question(q: Question):
    """Ask a question about SEC filings."""
    try:
        result = rag.ask(
            question=q.question,
            ticker=q.ticker,
            top_k=q.top_k
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "healthy"}

# Run with: uvicorn rag_api:app --reload
```

## Performance Optimization

### 1. Cache Embeddings

```python
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_query_embedding(query: str):
    return model.encode(query)
```

### 2. Batch Processing

```python
# Process multiple questions in batch
questions = ["Q1", "Q2", "Q3"]
query_embeddings = model.encode(questions)
```

### 3. Index Optimization

```sql
-- Create index on embedding similarity
CREATE INDEX IF NOT EXISTS idx_chunks_embedding 
ON chunks USING HNSW (embedding_vector);
```

## Monitoring & Evaluation

### Track RAG Metrics

```python
from prometheus_client import Counter, Histogram

rag_queries = Counter('rag_queries_total', 'Total RAG queries')
rag_latency = Histogram('rag_query_latency_seconds', 'RAG query latency')
```

### Evaluate Quality

```python
def evaluate_rag(test_questions: list[dict]):
    """Evaluate RAG quality."""
    results = []
    
    for test in test_questions:
        question = test['question']
        expected_answer = test['expected']
        
        result = rag.ask(question)
        
        # Calculate similarity between expected and actual
        similarity = calculate_similarity(expected_answer, result['answer'])
        
        results.append({
            "question": question,
            "similarity": similarity,
            "sources_used": result['chunks_used']
        })
    
    return results
```

## Best Practices

1. **Chunk Size**: 500-1000 tokens (already optimized)
2. **Top-K**: 3-7 chunks for best balance
3. **Temperature**: 0.2-0.4 for factual answers
4. **Citation**: Always include source references
5. **Fallback**: Handle "not found" gracefully
6. **Caching**: Cache frequent queries
7. **Monitoring**: Track latency and quality

## Next Steps

1. ✅ Generate embeddings for all chunks
2. ✅ Build RAG query interface
3. ⏭️ Create web UI (Streamlit/Gradio)
4. ⏭️ Add user feedback loop
5. ⏭️ Implement advanced features (hybrid search, time-series)

## Troubleshooting

**Issue:** Slow query performance
- Solution: Add embedding index, batch queries

**Issue:** Irrelevant results
- Solution: Adjust top_k, try hybrid search, fine-tune embedding model

**Issue:** LLM hallucinations
- Solution: Lower temperature, add stricter prompts, include more context

## Summary

With the FinLoom unstructured data system + this RAG integration, you can:
- ✅ Ask natural language questions about SEC filings
- ✅ Get accurate answers with source citations
- ✅ Compare companies and trends over time
- ✅ Build production-grade financial analysis tools

**Ready for deployment!**
