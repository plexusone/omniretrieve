# Vector Search

Vector search finds documents based on semantic similarity using embeddings. It's the foundation of modern retrieval systems and powers RAG applications.

## Overview

Vector search works by:

1. Converting text to high-dimensional vectors (embeddings)
2. Storing vectors in an index
3. Finding nearest neighbors using similarity metrics

```
Query: "machine learning frameworks"
    │
    ▼ (Embedding)
[0.12, -0.45, 0.78, ...]
    │
    ▼ (Similarity Search)
┌─────────────────────────────┐
│ doc1: 0.92 "PyTorch intro"  │
│ doc2: 0.87 "TensorFlow..."  │
│ doc3: 0.71 "Keras guide"    │
└─────────────────────────────┘
```

## Quick Start

```go
import "github.com/plexusone/omniretrieve/vector"

// Create index
index := vector.NewIndex(vector.Config{
    Dimensions: 384, // Must match embedder
})

// Add documents with embeddings
index.Add("doc1", embedding1)
index.Add("doc2", embedding2)

// Search
results := index.Search(queryEmbedding, 5)

for _, r := range results {
    fmt.Printf("%.3f: %s\n", r.Score, r.ID)
}
```

## Configuration

```go
index := vector.NewIndex(vector.Config{
    Dimensions: 384,                    // Required: embedding dimensions
    Metric:     vector.MetricCosine,    // Similarity metric
    Normalize:  true,                   // Normalize vectors
})
```

### Similarity Metrics

| Metric | Use Case | Range |
|--------|----------|-------|
| `MetricCosine` | Most text embeddings | [-1, 1] |
| `MetricEuclidean` | Absolute distances | [0, ∞) |
| `MetricDotProduct` | Normalized embeddings | [-∞, ∞] |

### Choosing a Metric

```go
// Cosine similarity (default, recommended)
// Best for: text embeddings, sentence transformers
index := vector.NewIndex(vector.Config{
    Metric: vector.MetricCosine,
})

// Euclidean distance
// Best for: image embeddings, when magnitude matters
index := vector.NewIndex(vector.Config{
    Metric: vector.MetricEuclidean,
})

// Dot product
// Best for: pre-normalized embeddings (e.g., OpenAI)
index := vector.NewIndex(vector.Config{
    Metric: vector.MetricDotProduct,
})
```

## Operations

### Adding Vectors

```go
// Add single vector
index.Add("doc1", embedding)

// Add with metadata
index.AddWithMeta("doc1", embedding, map[string]any{
    "title": "Document Title",
    "date":  "2026-06-10",
})

// Batch add
vectors := map[string][]float64{
    "doc1": embedding1,
    "doc2": embedding2,
    "doc3": embedding3,
}
index.AddBatch(vectors)
```

### Searching

```go
// Basic search
results := index.Search(queryEmbedding, 10)

// Search with filter
results := index.SearchWithFilter(queryEmbedding, 10, func(id string, meta map[string]any) bool {
    return meta["type"] == "article"
})
```

### Updating Vectors

```go
// Update replaces the existing vector
index.Update("doc1", newEmbedding)
```

### Removing Vectors

```go
index.Remove("doc1")
```

## Embeddings

### Embedding Dimensions

Common embedding model dimensions:

| Model | Dimensions |
|-------|------------|
| OpenAI text-embedding-3-small | 1536 |
| OpenAI text-embedding-3-large | 3072 |
| Cohere embed-v3 | 1024 |
| Sentence Transformers | 384-768 |

### Using omnillm

```go
import "github.com/plexusone/omnillm"

client := omnillm.NewClient()
embedder := omnillm.NewEmbedder(client, "text-embedding-3-small")

// Embed text
embeddings, err := embedder.Embed(ctx, []string{
    "First document",
    "Second document",
})

// Add to index
for i, emb := range embeddings {
    index.Add(fmt.Sprintf("doc%d", i), emb)
}
```

### Batch Embedding

```go
// Embed in batches for efficiency
texts := []string{"doc1", "doc2", "doc3", ...}
batchSize := 100

for i := 0; i < len(texts); i += batchSize {
    end := min(i+batchSize, len(texts))
    batch := texts[i:end]

    embeddings, err := embedder.Embed(ctx, batch)
    if err != nil {
        return err
    }

    for j, emb := range embeddings {
        index.Add(fmt.Sprintf("doc%d", i+j), emb)
    }
}
```

## Similarity Functions

### Cosine Similarity

```go
// Range: [-1, 1], higher is more similar
score := vector.CosineSimilarity(a, b)
```

### Euclidean Distance

```go
// Range: [0, ∞), lower is more similar
distance := vector.EuclideanDistance(a, b)
```

### Dot Product

```go
// Range: unbounded, higher is more similar
score := vector.DotProduct(a, b)
```

### Normalization

```go
// Normalize vector to unit length
normalized := vector.Normalize(embedding)

// Check if normalized
isUnit := vector.IsNormalized(embedding)
```

## Performance

### Complexity

| Operation | Brute Force | With HNSW* |
|-----------|-------------|------------|
| Add | O(1) | O(log n) |
| Search | O(n × d) | O(log n × d) |
| Remove | O(1) | O(log n) |

*HNSW available via pgvector provider

### Memory Usage

```go
// Memory per vector:
// dimensions × 8 bytes (float64) + overhead
//
// 384 dimensions: ~3.1 KB per vector
// 1536 dimensions: ~12.3 KB per vector
```

### Optimization Tips

1. **Batch operations** - Add/embed in batches
2. **Dimension reduction** - Use smaller embedding models when possible
3. **Filter early** - Apply metadata filters before similarity search
4. **Use ANN** - For large datasets, use pgvector with HNSW index

## Production Considerations

### Index Persistence

For production, use pgvector:

```go
import "github.com/plexusone/omniretrieve/providers/pgvector"

manager, err := pgvector.NewManager(pgvector.Config{
    ConnectionString: "postgres://...",
    Dimensions:       384,
    IndexType:        "hnsw", // Approximate nearest neighbor
})
```

### Scaling

| Documents | Recommendation |
|-----------|----------------|
| < 10,000 | In-memory index |
| 10,000 - 1M | pgvector with HNSW |
| > 1M | Dedicated vector DB (Pinecone, Weaviate) |

## See Also

- [Memory Manager](memory.md) - Collection-based storage
- [Hybrid Search](hybrid.md) - Combine with BM25
- [Reranking](rerank.md) - Improve result quality
- [pgvector Provider](../providers/pgvector.md) - PostgreSQL storage
