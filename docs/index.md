# OmniRetrieve

**Vector search, BM25, and semantic memory for Go.**

OmniRetrieve provides a unified interface for retrieval operations in AI applications:

- **Vector Search** - Semantic similarity using embeddings
- **BM25 Search** - Traditional keyword-based text search
- **Hybrid Search** - Combine vector and BM25 for best results
- **Memory Manager** - Collection-based document storage with semantic search
- **Reranking** - Re-score results using cross-encoders

## Key Features

- **Multiple Index Types** - Vector, BM25, and hybrid indices
- **Pluggable Embedders** - Use any embedding model
- **Collection Support** - Organize documents into named collections
- **Metadata Filtering** - Filter results by metadata fields
- **PostgreSQL Support** - pgvector integration for production deployments
- **Observability** - Built-in tracing and metrics

## Quick Example

```go
package main

import (
    "context"
    "fmt"

    "github.com/plexusone/omniretrieve/memory"
)

func main() {
    ctx := context.Background()

    // Create memory manager with hash embedder (for testing)
    mgr := memory.NewManager(memory.ManagerConfig{
        Embedder: memory.NewHashEmbedder(384),
    })

    // Create a collection
    coll, _ := mgr.GetOrCreateCollection(ctx, "notes", "My notes collection")

    // Store documents
    mgr.Store(ctx, "notes", "doc1", &memory.Document{
        ID:      "doc1",
        Content: "Go is a statically typed, compiled language",
    })
    mgr.Store(ctx, "notes", "doc2", &memory.Document{
        ID:      "doc2",
        Content: "Python is dynamically typed and interpreted",
    })

    // Semantic search
    results, _ := mgr.Search(ctx, "notes", "compiled programming languages", memory.SearchOptions{
        TopK: 5,
    })

    for _, r := range results {
        fmt.Printf("%.2f: %s\n", r.Score, r.Document.Content)
    }
}
```

## Package Structure

```
github.com/plexusone/omniretrieve
├── vector/        # Vector index and similarity search
├── bm25/          # BM25 text search index
├── hybrid/        # Hybrid vector + BM25 search
├── memory/        # Memory manager with collections
├── rerank/        # Result reranking
├── graph/         # Graph-based retrieval
├── retrieve/      # Core retrieval interface
├── observe/       # Observability integration
└── providers/
    └── pgvector/  # PostgreSQL pgvector provider
```

## Installation

```bash
go get github.com/plexusone/omniretrieve@latest
```

## Use Cases

| Use Case | Package | Description |
|----------|---------|-------------|
| RAG Applications | `memory` | Store and retrieve context for LLM prompts |
| Document Search | `hybrid` | Find relevant documents using keywords + semantics |
| Semantic Similarity | `vector` | Find similar items based on embeddings |
| Knowledge Base | `memory` | Build searchable knowledge collections |
| Research Tools | `bm25` + `rerank` | Keyword search with neural reranking |

## Getting Started

- [Getting Started Guide](getting-started.md)
- [Concepts Overview](concepts/overview.md)

## Learn More

- [Vector Search](concepts/vector.md) - Semantic similarity search
- [BM25 Search](concepts/bm25.md) - Keyword-based text search
- [Hybrid Search](concepts/hybrid.md) - Combined approach
- [Memory Manager](concepts/memory.md) - Collection-based storage
- [Reranking](concepts/rerank.md) - Result re-scoring
