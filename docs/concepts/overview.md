# Concepts Overview

OmniRetrieve provides multiple retrieval strategies that can be used independently or combined.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Memory Manager                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │ Collection  │  │ Collection  │  │ Collection  │     │
│  │  "notes"    │  │  "docs"     │  │  "history"  │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                   Hybrid Searcher                        │
│  ┌──────────────────┐    ┌──────────────────┐          │
│  │   Vector Index   │    │    BM25 Index    │          │
│  │  (Embeddings)    │    │   (Keywords)     │          │
│  └──────────────────┘    └──────────────────┘          │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                      Reranker                            │
│  Cross-encoder model for result quality improvement      │
└─────────────────────────────────────────────────────────┘
```

## Index Types

### Vector Index

Stores document embeddings for semantic similarity search.

| Feature | Description |
|---------|-------------|
| **Search Type** | Semantic similarity |
| **Best For** | Finding conceptually similar content |
| **Requires** | Embedding model |
| **Complexity** | O(n) brute force, O(log n) with ANN |

### BM25 Index

Traditional keyword-based search using TF-IDF scoring.

| Feature | Description |
|---------|-------------|
| **Search Type** | Keyword matching |
| **Best For** | Exact term matching, known terminology |
| **Requires** | Nothing (pure Go) |
| **Complexity** | O(n) worst case |

### Hybrid Index

Combines vector and BM25 scores using reciprocal rank fusion or weighted sum.

| Feature | Description |
|---------|-------------|
| **Search Type** | Combined semantic + keyword |
| **Best For** | General-purpose retrieval |
| **Requires** | Embedding model |
| **Trade-off** | Higher latency, better recall |

## Core Interfaces

### Embedder

Converts text to vector embeddings:

```go
type Embedder interface {
    Embed(ctx context.Context, texts []string) ([][]float64, error)
    Dimensions() int
}
```

### Index

Base interface for all index types:

```go
type Index interface {
    Add(id string, content string, embedding []float64) error
    Remove(id string) error
    Search(query string, embedding []float64, k int) ([]Result, error)
}
```

### Document

Standard document structure:

```go
type Document struct {
    ID        string            `json:"id"`
    Content   string            `json:"content"`
    Embedding []float64         `json:"embedding,omitempty"`
    Metadata  map[string]string `json:"metadata,omitempty"`
    CreatedAt time.Time         `json:"created_at"`
}
```

## Similarity Metrics

### Cosine Similarity

Default for most use cases:

```go
similarity := vector.CosineSimilarity(a, b)
```

### Euclidean Distance

For absolute distance comparisons:

```go
distance := vector.EuclideanDistance(a, b)
```

### Dot Product

For normalized embeddings:

```go
score := vector.DotProduct(a, b)
```

## Fusion Strategies

### Reciprocal Rank Fusion (RRF)

Combines rankings from multiple sources:

```
RRF(d) = Σ 1 / (k + rank(d))
```

Best for combining diverse ranking sources.

### Weighted Sum

Simple weighted combination of scores:

```
score(d) = α * vector_score(d) + (1-α) * bm25_score(d)
```

Best when scores are normalized.

## When to Use What

| Scenario | Recommended Approach |
|----------|---------------------|
| RAG context retrieval | Memory Manager + Hybrid |
| Document search | BM25 + Reranking |
| Semantic similarity | Vector Index |
| Question answering | Hybrid + Reranking |
| Keyword lookup | BM25 only |

## Performance Considerations

### Memory Usage

| Component | Memory Per Document |
|-----------|-------------------|
| BM25 Index | ~100-500 bytes |
| Vector Index (384d) | ~3 KB |
| Vector Index (1536d) | ~12 KB |
| Metadata | Variable |

### Latency

| Operation | Typical Latency |
|-----------|----------------|
| BM25 search (10K docs) | 1-5 ms |
| Vector search (10K docs) | 10-50 ms |
| Embedding generation | 50-200 ms |
| Reranking (20 docs) | 100-500 ms |

## Next Steps

- [Vector Search](vector.md) - Deep dive into vector search
- [BM25 Search](bm25.md) - Keyword-based search
- [Hybrid Search](hybrid.md) - Combined approach
- [Memory Manager](memory.md) - Collection-based storage
