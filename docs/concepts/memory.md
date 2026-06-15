# Memory Manager

The Memory Manager provides collection-based document storage with semantic search. It's designed for AI applications that need to store and retrieve contextual information.

## Overview

The memory manager organizes documents into named collections, each with its own vector index for semantic search.

```
Memory Manager
├── Collection: "notes"
│   ├── doc1: "Meeting notes from Monday..."
│   ├── doc2: "Project requirements..."
│   └── doc3: "Team discussion..."
├── Collection: "history"
│   ├── conv1: "User asked about weather..."
│   └── conv2: "User requested summary..."
└── Collection: "preferences"
    ├── pref1: "User prefers dark mode..."
    └── pref2: "Notification settings..."
```

## Quick Start

```go
import "github.com/plexusone/omniretrieve/memory"

ctx := context.Background()

// Create manager with embedder
mgr := memory.NewManager(memory.ManagerConfig{
    Embedder: memory.NewHashEmbedder(384), // Use real embedder in production
})

// Create collection
_, _ = mgr.GetOrCreateCollection(ctx, "notes", "My notes")

// Store documents
mgr.Store(ctx, "notes", "meeting-2026-06-10", &memory.Document{
    ID:      "meeting-2026-06-10",
    Content: "Discussed Q3 roadmap, agreed on priorities",
    Metadata: map[string]string{
        "type": "meeting",
        "date": "2026-06-10",
    },
})

// Search
results, _ := mgr.Search(ctx, "notes", "roadmap priorities", memory.SearchOptions{
    TopK:            5,
    IncludeMetadata: true,
})
```

## Configuration

```go
mgr := memory.NewManager(memory.ManagerConfig{
    // Required: embedding model
    Embedder: embedder,

    // Optional: default collection settings
    DefaultTopK: 10,
})
```

### Using Real Embedders

For production, use a real embedding model:

```go
import (
    "github.com/plexusone/omnillm"
    "github.com/plexusone/omniretrieve/memory"
)

// OpenAI embeddings
client := omnillm.NewClient()
embedder := omnillm.NewEmbedder(client, "text-embedding-3-small")

mgr := memory.NewManager(memory.ManagerConfig{
    Embedder: embedder,
})
```

## Collections

### Creating Collections

```go
// Get or create (idempotent)
coll, created := mgr.GetOrCreateCollection(ctx, "notes", "My notes collection")
if created {
    fmt.Println("Created new collection")
}

// Get existing (returns error if not found)
coll, err := mgr.GetCollection(ctx, "notes")
```

### Listing Collections

```go
collections := mgr.ListCollections()
for _, c := range collections {
    fmt.Printf("%s: %s\n", c.Name, c.Description)
}
```

### Deleting Collections

```go
err := mgr.DeleteCollection(ctx, "notes")
```

## Documents

### Document Structure

```go
type Document struct {
    ID        string            `json:"id"`
    Content   string            `json:"content"`
    Embedding []float64         `json:"embedding,omitempty"`
    Metadata  map[string]string `json:"metadata,omitempty"`
    CreatedAt time.Time         `json:"created_at"`
}
```

### Storing Documents

```go
// Basic storage
mgr.Store(ctx, "notes", "doc1", &memory.Document{
    ID:      "doc1",
    Content: "Document content here",
})

// With metadata
mgr.Store(ctx, "notes", "doc2", &memory.Document{
    ID:      "doc2",
    Content: "Another document",
    Metadata: map[string]string{
        "author": "alice",
        "type":   "note",
    },
})

// Auto-generate key
key := fmt.Sprintf("mem_%d", time.Now().UnixNano())
mgr.Store(ctx, "notes", key, doc)
```

### Retrieving Documents

```go
// Get by key
doc, err := mgr.Get(ctx, "notes", "doc1")

// List all in collection
docs, err := mgr.List(ctx, "notes", 100, 0)
```

### Deleting Documents

```go
err := mgr.Delete(ctx, "notes", "doc1")
```

## Search

### Basic Search

```go
results, err := mgr.Search(ctx, "notes", "search query", memory.SearchOptions{
    TopK: 5,
})
```

### Search Options

```go
type SearchOptions struct {
    TopK            int    // Maximum results (default: 5)
    IncludeMetadata bool   // Include metadata in results
    MinScore        float64 // Minimum similarity score (0-1)
}
```

### Search Results

```go
type SearchResult struct {
    Document *Document
    Score    float64 // Similarity score (0-1)
}

// Process results
for _, r := range results {
    fmt.Printf("%.2f: %s\n", r.Score, r.Document.Content)
    if r.Document.Metadata != nil {
        fmt.Printf("  Metadata: %v\n", r.Document.Metadata)
    }
}
```

### Filtering Results

```go
// Filter by minimum score
results, _ := mgr.Search(ctx, "notes", query, memory.SearchOptions{
    TopK:     10,
    MinScore: 0.7, // Only results with score >= 0.7
})

// Post-filter by metadata
var filtered []memory.SearchResult
for _, r := range results {
    if r.Document.Metadata["type"] == "meeting" {
        filtered = append(filtered, r)
    }
}
```

## Embedder Interface

Implement custom embedders:

```go
type Embedder interface {
    Embed(ctx context.Context, texts []string) ([][]float64, error)
    Dimensions() int
}
```

### Hash Embedder (Testing)

For testing without an embedding service:

```go
embedder := memory.NewHashEmbedder(384)
```

!!! warning
    The hash embedder produces deterministic but non-semantic embeddings. Use only for testing.

### Custom Embedder

```go
type MyEmbedder struct {
    model  string
    client *api.Client
}

func (e *MyEmbedder) Embed(ctx context.Context, texts []string) ([][]float64, error) {
    // Call your embedding API
    return embeddings, nil
}

func (e *MyEmbedder) Dimensions() int {
    return 384
}
```

## Persistence

### In-Memory (Default)

The default manager stores everything in memory:

```go
mgr := memory.NewManager(config)
// Data is lost when process exits
```

### With Storage Backend

For persistence, use the pgvector provider:

```go
import "github.com/plexusone/omniretrieve/providers/pgvector"

manager, err := pgvector.NewManager(pgvector.Config{
    ConnectionString: "postgres://...",
    Dimensions:       384,
})
```

## Best Practices

### Key Naming

Use descriptive, unique keys:

```go
// Good
key := fmt.Sprintf("meeting_%s", date)
key := fmt.Sprintf("user_%s_pref", userID)

// Bad
key := "doc1"
key := fmt.Sprintf("%d", time.Now().Unix())
```

### Collection Organization

| Collection | Use Case |
|------------|----------|
| `conversations` | Chat history |
| `preferences` | User preferences |
| `knowledge` | Domain knowledge |
| `context` | Temporary context |

### Content Optimization

```go
// Good: clear, complete content
doc := &memory.Document{
    Content: "User prefers dark mode for the IDE, compact sidebar layout, and vim keybindings",
}

// Bad: too brief
doc := &memory.Document{
    Content: "dark mode",
}
```

### Metadata Usage

```go
doc := &memory.Document{
    Content: "Meeting notes...",
    Metadata: map[string]string{
        "type":        "meeting",
        "date":        "2026-06-10",
        "participants": "alice,bob",
        "priority":    "high",
    },
}
```

## See Also

- [Vector Search](vector.md) - Underlying vector index
- [Hybrid Search](hybrid.md) - Combined keyword + semantic search
- [pgvector Provider](../providers/pgvector.md) - PostgreSQL persistence
