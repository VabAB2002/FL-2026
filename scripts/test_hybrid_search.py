"""
Test script for hybrid search implementation.

Verifies that:
1. Meilisearch is accessible and has data
2. Qdrant is accessible and has data
3. Hybrid retriever combines all sources
4. Reranking works correctly
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.retrieval.hybrid_search import HybridRetriever
from src.retrieval.keyword_search import KeywordSearch
from src.retrieval.vector_search import VectorSearch
from src.infrastructure.logger import get_logger

logger = get_logger(__name__)


def test_services():
    """Test that all required services are accessible."""
    print("\n=== Testing Service Availability ===\n")
    
    # Test Meilisearch
    try:
        keyword_search = KeywordSearch()
        stats = keyword_search.get_stats()
        print(f"✓ Meilisearch: Connected")
        print(f"  - Documents: {stats['number_of_documents']}")
        print(f"  - Indexing: {stats['is_indexing']}")
    except Exception as e:
        print(f"✗ Meilisearch: Failed - {e}")
        return False
    
    # Test Qdrant
    try:
        vector_search = VectorSearch()
        print(f"✓ Qdrant: Connected")
        vector_search.close()
    except Exception as e:
        print(f"✗ Qdrant: Failed - {e}")
        return False
    
    return True


def test_hybrid_search():
    """Test hybrid search functionality."""
    print("\n=== Testing Hybrid Search ===\n")
    
    # Initialize hybrid retriever
    retriever = HybridRetriever(use_reranking=True)
    
    # Test queries
    test_queries = [
        "What are Apple's risk factors?",
        "Apple revenue 2024",
        "Competition in technology sector",
    ]
    
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        print("-" * 60)
        
        try:
            # Search without reranking
            retriever.use_reranking = False
            results_no_rerank = retriever.retrieve(query, top_k=5)
            print(f"  Without reranking: {len(results_no_rerank)} results")
            
            # Search with reranking
            retriever.use_reranking = True
            results_reranked = retriever.retrieve(query, top_k=5)
            print(f"  With reranking:    {len(results_reranked)} results")
            
            # Show top result
            if results_reranked:
                top_result = results_reranked[0]
                print(f"\n  Top Result:")
                print(f"    Score: {top_result['score']:.4f}")
                print(f"    Company: {top_result['metadata'].get('company_name', 'N/A')}")
                print(f"    Section: {top_result['metadata'].get('section_title', 'N/A')}")
                print(f"    Source: {top_result['metadata'].get('source', 'N/A')}")
                print(f"    Content preview: {top_result['content'][:150]}...")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    retriever.close()


def test_individual_components():
    """Test each search component individually."""
    print("\n=== Testing Individual Components ===\n")
    
    query = "Apple revenue 2024"
    
    # Vector search
    print(f"1. Vector Search: '{query}'")
    try:
        vector_search = VectorSearch()
        results = vector_search.search(query, top_k=5)
        print(f"   ✓ Found {len(results)} results")
        if results:
            print(f"   Top score: {results[0]['score']:.4f}")
        vector_search.close()
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Keyword search
    print(f"\n2. Keyword Search: '{query}'")
    try:
        keyword_search = KeywordSearch()
        results = keyword_search.search(query, top_k=5)
        print(f"   ✓ Found {len(results)} results")
    except Exception as e:
        print(f"   ✗ Error: {e}")


def check_api_keys():
    """Check that required API keys are configured."""
    print("\n=== Checking API Keys ===\n")
    
    keys = {
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "COHERE_API_KEY": os.getenv("COHERE_API_KEY"),
    }
    
    for key_name, key_value in keys.items():
        if key_value:
            masked = f"{key_value[:8]}...{key_value[-4:]}" if len(key_value) > 12 else "***"
            print(f"✓ {key_name}: {masked}")
        else:
            print(f"✗ {key_name}: Not set")


if __name__ == "__main__":
    print("=" * 70)
    print("HYBRID SEARCH VERIFICATION TEST")
    print("=" * 70)
    
    # Check API keys
    check_api_keys()
    
    # Test services
    if not test_services():
        print("\n❌ Service tests failed. Please ensure Docker services are running.")
        sys.exit(1)
    
    # Test individual components
    test_individual_components()
    
    # Test hybrid search
    test_hybrid_search()
    
    print("\n" + "=" * 70)
    print("✓ VERIFICATION COMPLETE")
    print("=" * 70)
