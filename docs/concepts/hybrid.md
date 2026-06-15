# Hybrid Search

Hybrid search combines vector (semantic) and BM25 (keyword) search for better retrieval quality. It captures both conceptual similarity and exact term matching.

## Overview

```
Query: "golang error handling best practices"
        │
        ├──────────────────┬──────────────────┐
        ▼                  ▼                  │
   Vector Search      BM25 Search            │
   (Semantic)         (Keywords)             │
        │                  │                  │
        ▼                  ▼                  │
   [doc3, doc1,       [doc1, doc2,           │
    doc5, doc2]        doc4, doc3]           │
        │                  │                  │
        └────────┬─────────┘                  │
                 ▼                            │
           Score Fusion                       │
                 │                            │
                 ▼                            │
           [doc1, doc3,                       │
            doc2, doc5]                       │
```

## Quick Start

```go
import (
    "github.com/plexusone/omniretrieve/hybrid"
    "github.com/plexusone/omniretrieve/vector"
    "github.com/plexusone/omniretrieve/bm25"
)

// Create indices
vectorIndex := vector.NewIndex(vector.Config{Dimensions: 384})
bm25Index := bm25.NewIndex()

// Create hybrid searcher
searcher := hybrid.NewSearcher(hybrid.Config{
    VectorIndex: vectorIndex,
    BM25Index:   bm25Index,
    Alpha:       0.5, // Equal weight
})

// Add documents
searcher.Add(ctx, "doc1", "Error handling in Go", embedding1)
searcher.Add(ctx, "doc2", "Go best practices guide", embedding2)

// Search
results, err := searcher.Search(ctx, "error handling", queryEmb, 10)
```

## Configuration

```go
searcher := hybrid.NewSearcher(hybrid.Config{
    VectorIndex: vectorIndex,
    BM25Index:   bm25Index,
    Alpha:       0.5,                     // Vector weight (0-1)
    Fusion:      hybrid.FusionRRF,        // Fusion strategy
    RRFConstant: 60,                      // RRF k parameter
    Normalize:   true,                    // Normalize scores
})
```

### Alpha Parameter

Controls the balance between vector and BM25:

| Alpha | Vector Weight | BM25 Weight | Best For |
|-------|---------------|-------------|----------|
| 0.0 | 0% | 100% | Keyword-only |
| 0.3 | 30% | 70% | Keyword-heavy |
| 0.5 | 50% | 50% | Balanced |
| 0.7 | 70% | 30% | Semantic-heavy |
| 1.0 | 100% | 0% | Semantic-only |

### Tuning Alpha

```go
// For technical documentation (exact terms matter)
searcher := hybrid.NewSearcher(hybrid.Config{
    Alpha: 0.3, // More keyword weight
})

// For conversational queries
searcher := hybrid.NewSearcher(hybrid.Config{
    Alpha: 0.7, // More semantic weight
})
```

## Fusion Strategies

### Reciprocal Rank Fusion (RRF)

Combines rankings rather than scores:

```
RRF(d) = Σ 1 / (k + rank(d))
```

```go
searcher := hybrid.NewSearcher(hybrid.Config{
    Fusion:      hybrid.FusionRRF,
    RRFConstant: 60, // k parameter
})
```

**Advantages:**

- Robust to score distribution differences
- No normalization needed
- Works well with diverse retrieval methods

### Weighted Sum

Combines normalized scores:

```
score(d) = α × vector_score(d) + (1-α) × bm25_score(d)
```

```go
searcher := hybrid.NewSearcher(hybrid.Config{
    Fusion:    hybrid.FusionWeightedSum,
    Alpha:     0.5,
    Normalize: true, // Required for weighted sum
})
```

**Advantages:**

- Intuitive weighting
- Preserves score magnitudes
- Good when scores are comparable

### Choosing a Strategy

| Scenario | Recommended |
|----------|-------------|
| Different retrieval methods | RRF |
| Same embedding space | Weighted Sum |
| Unknown score distributions | RRF |
| Fine-grained control | Weighted Sum |

## Operations

### Adding Documents

```go
// Add with content and embedding
searcher.Add(ctx, "doc1", "Document content", embedding)

// The content goes to BM25, embedding to vector index
```

### Searching

```go
// Basic search
results, err := searcher.Search(ctx, "query text", queryEmbedding, 10)

// Search with options
results, err := searcher.SearchWithOptions(ctx, "query", emb, hybrid.SearchOptions{
    TopK:       10,
    MinScore:   0.5,
    VectorOnly: false,
    BM25Only:   false,
})
```

### Removing Documents

```go
searcher.Remove("doc1") // Removes from both indices
```

## Advanced Usage

### Custom Embedder Integration

```go
type HybridSystem struct {
    searcher *hybrid.Searcher
    embedder Embedder
}

func (h *HybridSystem) Add(ctx context.Context, id, content string) error {
    // Generate embedding
    embeddings, err := h.embedder.Embed(ctx, []string{content})
    if err != nil {
        return err
    }

    // Add to hybrid index
    return h.searcher.Add(ctx, id, content, embeddings[0])
}

func (h *HybridSystem) Search(ctx context.Context, query string, k int) ([]hybrid.Result, error) {
    // Generate query embedding
    embeddings, err := h.embedder.Embed(ctx, []string{query})
    if err != nil {
        return nil, err
    }

    return h.searcher.Search(ctx, query, embeddings[0], k)
}
```

### Dynamic Alpha

Adjust alpha based on query characteristics:

```go
func dynamicAlpha(query string) float64 {
    // Short queries -> more keyword
    if len(strings.Fields(query)) <= 2 {
        return 0.3
    }

    // Question queries -> more semantic
    if strings.HasSuffix(query, "?") {
        return 0.7
    }

    // Default balanced
    return 0.5
}

results, _ := searcher.SearchWithOptions(ctx, query, emb, hybrid.SearchOptions{
    Alpha: dynamicAlpha(query),
})
```

### Two-Stage Retrieval

First retrieve, then rerank:

```go
import "github.com/plexusone/omniretrieve/rerank"

// Stage 1: Hybrid retrieval
candidates, _ := searcher.Search(ctx, query, emb, 100)

// Stage 2: Neural reranking
reranker := rerank.NewCrossEncoder(config)
final, _ := reranker.Rerank(ctx, query, candidates[:20])
```

## Performance

### Latency

| Component | Typical Latency |
|-----------|----------------|
| BM25 search | 1-5 ms |
| Vector search | 10-50 ms |
| Score fusion | < 1 ms |
| Total | 15-60 ms |

### Memory

Both indices are maintained, so memory usage is:

```
Total = BM25 memory + Vector memory
     ~= 500 bytes/doc + 3 KB/doc (384d)
     ~= 3.5 KB per document
```

## When to Use Hybrid

| Scenario | Pure Vector | Pure BM25 | Hybrid |
|----------|-------------|-----------|--------|
| Semantic similarity | ✓ | | |
| Exact term matching | | ✓ | ✓ |
| Typo tolerance | ✓ | | ✓ |
| Unknown query type | | | ✓ |
| Best overall recall | | | ✓ |

## See Also

- [Vector Search](vector.md) - Semantic similarity
- [BM25 Search](bm25.md) - Keyword matching
- [Reranking](rerank.md) - Result quality improvement
- [Memory Manager](memory.md) - Collection-based storage
