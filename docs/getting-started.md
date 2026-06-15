# Getting Started

This guide walks through setting up OmniRetrieve for common use cases.

## Installation

```bash
go get github.com/plexusone/omniretrieve@latest
```

## Basic Usage

### Memory Manager (Recommended)

The memory manager is the easiest way to get started:

```go
package main

import (
    "context"
    "fmt"

    "github.com/plexusone/omniretrieve/memory"
)

func main() {
    ctx := context.Background()

    // Create manager with embedder
    mgr := memory.NewManager(memory.ManagerConfig{
        // Use hash embedder for testing
        // In production, use a real embedding model
        Embedder: memory.NewHashEmbedder(384),
    })

    // Create a collection
    _, _ = mgr.GetOrCreateCollection(ctx, "docs", "Documentation")

    // Store a document
    err := mgr.Store(ctx, "docs", "intro", &memory.Document{
        ID:      "intro",
        Content: "OmniRetrieve is a retrieval library for Go",
        Metadata: map[string]string{
            "type": "overview",
        },
    })
    if err != nil {
        panic(err)
    }

    // Search
    results, err := mgr.Search(ctx, "docs", "Go retrieval library", memory.SearchOptions{
        TopK:            5,
        IncludeMetadata: true,
    })
    if err != nil {
        panic(err)
    }

    for _, r := range results {
        fmt.Printf("Score: %.2f, ID: %s\n", r.Score, r.Document.ID)
        fmt.Printf("Content: %s\n", r.Document.Content)
    }
}
```

### Using a Real Embedder

For production, use a real embedding model via omnillm:

```go
import (
    "github.com/plexusone/omnillm"
    "github.com/plexusone/omniretrieve/memory"
)

// Create embedder using omnillm
client := omnillm.NewClient()
embedder := omnillm.NewEmbedder(client, "text-embedding-3-small")

mgr := memory.NewManager(memory.ManagerConfig{
    Embedder: embedder,
})
```

## BM25 Search

For keyword-based search:

```go
import "github.com/plexusone/omniretrieve/bm25"

// Create index
index := bm25.NewIndex()

// Add documents
index.Add("doc1", "The quick brown fox jumps over the lazy dog")
index.Add("doc2", "A fast red fox leaps across the sleepy hound")
index.Add("doc3", "The dog barks at the cat")

// Search
results := index.Search("quick fox", 5)

for _, r := range results {
    fmt.Printf("%.3f: %s\n", r.Score, r.ID)
}
```

## Hybrid Search

Combine vector and BM25 for best results:

```go
import (
    "github.com/plexusone/omniretrieve/hybrid"
    "github.com/plexusone/omniretrieve/vector"
    "github.com/plexusone/omniretrieve/bm25"
)

// Create indices
vectorIndex := vector.NewIndex(vector.Config{
    Dimensions: 384,
})
bm25Index := bm25.NewIndex()

// Create hybrid searcher
searcher := hybrid.NewSearcher(hybrid.Config{
    VectorIndex: vectorIndex,
    BM25Index:   bm25Index,
    Alpha:       0.5, // Weight: 0 = all BM25, 1 = all vector
})

// Add documents (adds to both indices)
searcher.Add(ctx, "doc1", "Machine learning basics", embedding1)
searcher.Add(ctx, "doc2", "Deep learning fundamentals", embedding2)

// Search (uses both indices)
results, err := searcher.Search(ctx, "ML fundamentals", queryEmbedding, 10)
```

## Reranking

Improve result quality with neural reranking:

```go
import "github.com/plexusone/omniretrieve/rerank"

// Create reranker (uses LLM for cross-encoding)
reranker := rerank.NewCrossEncoder(rerank.Config{
    Model: "cross-encoder/ms-marco-MiniLM-L-6-v2",
})

// Initial search
initialResults := index.Search(ctx, query, 100)

// Rerank top results
reranked, err := reranker.Rerank(ctx, query, initialResults[:20])
```

## Production Setup

### PostgreSQL with pgvector

For production deployments, use PostgreSQL with pgvector:

```go
import "github.com/plexusone/omniretrieve/providers/pgvector"

// Connect to PostgreSQL
manager, err := pgvector.NewManager(pgvector.Config{
    ConnectionString: "postgres://user:pass@localhost/db",
    Dimensions:       384,
})
if err != nil {
    panic(err)
}
defer manager.Close()

// Use like memory manager
err = manager.Store(ctx, "collection", "key", document)
results, err := manager.Search(ctx, "collection", query, options)
```

### Environment Variables

```bash
# PostgreSQL connection
export PGVECTOR_URL="postgres://user:pass@localhost/db"

# Embedding model (if using omnillm)
export OPENAI_API_KEY="sk-..."
```

## Next Steps

- [Concepts Overview](concepts/overview.md) - Understand the architecture
- [Vector Search](concepts/vector.md) - Deep dive into vector search
- [Memory Manager](concepts/memory.md) - Collection-based storage
- [pgvector Provider](providers/pgvector.md) - Production PostgreSQL setup
