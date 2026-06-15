# Reranking

Reranking improves retrieval quality by re-scoring initial results using a more sophisticated model. It's the final stage in a retrieval pipeline.

## Overview

```
Initial Retrieval (fast, broad)
        │
        ▼
   [100 candidates]
        │
        ▼
    Reranking (slow, precise)
        │
        ▼
   [Top 10 results]
```

The two-stage approach:

1. **Retrieve**: Fast retrieval of many candidates (BM25, vector, hybrid)
2. **Rerank**: Precise scoring of top candidates using cross-encoder

## Quick Start

```go
import "github.com/plexusone/omniretrieve/rerank"

// Create reranker
reranker := rerank.NewCrossEncoder(rerank.Config{
    Model: "cross-encoder/ms-marco-MiniLM-L-6-v2",
})

// Initial retrieval
candidates := index.Search(query, 100)

// Rerank top candidates
reranked, err := reranker.Rerank(ctx, query, candidates[:20])

// Use top results
for _, r := range reranked[:5] {
    fmt.Printf("%.3f: %s\n", r.Score, r.ID)
}
```

## Reranker Types

### Cross-Encoder

Processes query-document pairs together for precise relevance scoring:

```go
reranker := rerank.NewCrossEncoder(rerank.Config{
    Model:      "cross-encoder/ms-marco-MiniLM-L-6-v2",
    MaxLength:  512,
    BatchSize:  32,
})
```

**Advantages:**

- Most accurate
- Captures query-document interactions
- State-of-the-art quality

**Disadvantages:**

- Slow (O(n) model calls)
- Higher compute cost

### LLM Reranker

Uses an LLM to score relevance:

```go
reranker := rerank.NewLLMReranker(rerank.LLMConfig{
    Client: llmClient,
    Model:  "gpt-4o-mini",
    Prompt: "Rate relevance of document to query from 0-10",
})
```

**Advantages:**

- Uses existing LLM infrastructure
- Flexible scoring criteria
- Can explain rankings

**Disadvantages:**

- Highest latency
- Most expensive
- May be overkill for simple tasks

### Cohere Reranker

Uses Cohere's rerank API:

```go
reranker := rerank.NewCohereReranker(rerank.CohereConfig{
    APIKey: os.Getenv("COHERE_API_KEY"),
    Model:  "rerank-english-v3.0",
})
```

## Configuration

```go
type Config struct {
    // Model identifier
    Model string

    // Maximum sequence length
    MaxLength int

    // Batch size for inference
    BatchSize int

    // Minimum score threshold
    MinScore float64

    // Whether to return scores
    ReturnScores bool
}
```

### Tuning Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MaxLength` | 512 | Maximum tokens (query + doc) |
| `BatchSize` | 32 | Documents per batch |
| `MinScore` | 0.0 | Filter low-scoring results |

## Usage Patterns

### Basic Reranking

```go
// Retrieve candidates
candidates := searcher.Search(ctx, query, queryEmb, 100)

// Rerank
reranked, err := reranker.Rerank(ctx, query, candidates)
```

### With Score Threshold

```go
reranker := rerank.NewCrossEncoder(rerank.Config{
    MinScore: 0.5, // Filter low scores
})

reranked, _ := reranker.Rerank(ctx, query, candidates)
// Only results with score >= 0.5
```

### Truncated Reranking

Rerank only top-N candidates for efficiency:

```go
// Retrieve many
candidates := searcher.Search(ctx, query, emb, 100)

// Rerank top 20
reranked, _ := reranker.Rerank(ctx, query, candidates[:20])
```

## Integration Examples

### With Hybrid Search

```go
// Stage 1: Hybrid retrieval
candidates, _ := hybrid.Search(ctx, query, emb, 100)

// Stage 2: Rerank
reranked, _ := reranker.Rerank(ctx, query, candidates[:30])

// Use top results for RAG
context := buildContext(reranked[:5])
```

### With Memory Manager

```go
// Search memory
results, _ := memory.Search(ctx, "notes", query, memory.SearchOptions{
    TopK: 50,
})

// Convert to rerank format
candidates := make([]rerank.Document, len(results))
for i, r := range results {
    candidates[i] = rerank.Document{
        ID:      r.Document.ID,
        Content: r.Document.Content,
    }
}

// Rerank
reranked, _ := reranker.Rerank(ctx, query, candidates[:20])
```

## Rerank Interface

```go
type Reranker interface {
    Rerank(ctx context.Context, query string, docs []Document) ([]ScoredDocument, error)
}

type Document struct {
    ID      string
    Content string
}

type ScoredDocument struct {
    Document
    Score float64
}
```

### Custom Reranker

```go
type MyReranker struct {
    scorer func(query, doc string) float64
}

func (r *MyReranker) Rerank(ctx context.Context, query string, docs []Document) ([]ScoredDocument, error) {
    scored := make([]ScoredDocument, len(docs))
    for i, doc := range docs {
        scored[i] = ScoredDocument{
            Document: doc,
            Score:    r.scorer(query, doc.Content),
        }
    }

    // Sort by score descending
    sort.Slice(scored, func(i, j int) bool {
        return scored[i].Score > scored[j].Score
    })

    return scored, nil
}
```

## Performance

### Latency

| Reranker | 20 docs | 50 docs | 100 docs |
|----------|---------|---------|----------|
| Cross-Encoder | 100 ms | 250 ms | 500 ms |
| LLM | 500 ms | 1.2 s | 2.5 s |
| Cohere | 150 ms | 300 ms | 600 ms |

### Cost Considerations

| Reranker | Cost Model |
|----------|------------|
| Cross-Encoder | Compute only |
| LLM | Per token |
| Cohere | Per query |

### Optimization Tips

1. **Limit candidates** - Rerank 20-50, not 100+
2. **Batch efficiently** - Use appropriate batch sizes
3. **Cache results** - Cache for repeated queries
4. **Truncate content** - Only send relevant portions

## When to Use Reranking

| Scenario | Use Reranking? |
|----------|----------------|
| High-stakes queries | Yes |
| RAG applications | Yes |
| Simple keyword search | Usually no |
| Real-time autocomplete | No |
| Batch processing | Yes |

## Best Practices

### Candidate Selection

```go
// Good: Diverse candidates from multiple sources
candidates := append(
    vectorResults[:30],
    bm25Results[:30]...,
)
reranked, _ := reranker.Rerank(ctx, query, candidates)

// Less good: Only one source
candidates := vectorResults[:60]
```

### Content Preparation

```go
// Truncate long documents
func prepareForRerank(content string, maxLen int) string {
    if len(content) <= maxLen {
        return content
    }
    return content[:maxLen] + "..."
}
```

### Score Calibration

```go
// Normalize scores to [0, 1] if needed
func normalizeScores(results []ScoredDocument) {
    if len(results) == 0 {
        return
    }

    max := results[0].Score
    min := results[len(results)-1].Score
    range_ := max - min

    for i := range results {
        results[i].Score = (results[i].Score - min) / range_
    }
}
```

## See Also

- [Hybrid Search](hybrid.md) - Two-stage retrieval
- [Vector Search](vector.md) - Initial retrieval
- [BM25 Search](bm25.md) - Keyword-based retrieval
