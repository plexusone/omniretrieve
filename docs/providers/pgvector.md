# pgvector Provider

The pgvector provider enables persistent vector storage using PostgreSQL with the pgvector extension. It's recommended for production deployments.

## Overview

pgvector provides:

- **Persistent storage** - Vectors survive restarts
- **HNSW indexing** - Fast approximate nearest neighbor search
- **SQL integration** - Combine with existing PostgreSQL data
- **Scalability** - Handle millions of vectors

## Prerequisites

### PostgreSQL Setup

```bash
# Install PostgreSQL with pgvector
# Ubuntu/Debian
sudo apt install postgresql-16-pgvector

# macOS with Homebrew
brew install pgvector

# Docker
docker run -d \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  ankane/pgvector
```

### Enable Extension

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

## Quick Start

```go
import "github.com/plexusone/omniretrieve/providers/pgvector"

// Connect
manager, err := pgvector.NewManager(pgvector.Config{
    ConnectionString: "postgres://user:pass@localhost:5432/db",
    Dimensions:       384,
})
if err != nil {
    log.Fatal(err)
}
defer manager.Close()

// Create collection
_, _ = manager.GetOrCreateCollection(ctx, "docs", "Documentation")

// Store document
err = manager.Store(ctx, "docs", "doc1", &pgvector.Document{
    ID:        "doc1",
    Content:   "Document content",
    Embedding: embedding,
    Metadata:  map[string]string{"type": "guide"},
})

// Search
results, err := manager.Search(ctx, "docs", queryEmbedding, pgvector.SearchOptions{
    TopK: 10,
})
```

## Configuration

```go
manager, err := pgvector.NewManager(pgvector.Config{
    // Required
    ConnectionString: "postgres://...",
    Dimensions:       384,

    // Optional
    TablePrefix:      "omniretrieve_",
    IndexType:        "hnsw",      // or "ivfflat"
    DistanceFunction: "cosine",    // or "l2", "inner_product"
    MaxConnections:   10,
    CreateTables:     true,
})
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `ConnectionString` | Required | PostgreSQL connection URL |
| `Dimensions` | Required | Embedding dimensions |
| `TablePrefix` | `"omni_"` | Prefix for table names |
| `IndexType` | `"hnsw"` | Index type: "hnsw" or "ivfflat" |
| `DistanceFunction` | `"cosine"` | Distance: "cosine", "l2", "inner_product" |
| `MaxConnections` | `10` | Connection pool size |
| `CreateTables` | `true` | Auto-create tables |

## Index Types

### HNSW (Recommended)

Hierarchical Navigable Small World - best for most use cases:

```go
manager, _ := pgvector.NewManager(pgvector.Config{
    IndexType: "hnsw",
    HNSWConfig: &pgvector.HNSWConfig{
        M:              16,  // Connections per layer
        EFConstruction: 64,  // Build quality
    },
})
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `M` | 16 | Max connections per layer |
| `EFConstruction` | 64 | Build-time quality factor |

### IVFFlat

Inverted File with Flat compression - good for very large datasets:

```go
manager, _ := pgvector.NewManager(pgvector.Config{
    IndexType: "ivfflat",
    IVFFlatConfig: &pgvector.IVFFlatConfig{
        Lists: 100, // Number of clusters
    },
})
```

### Index Comparison

| Aspect | HNSW | IVFFlat |
|--------|------|---------|
| Build time | Slower | Faster |
| Query speed | Faster | Slower |
| Memory | Higher | Lower |
| Accuracy | Higher | Lower |
| Best for | < 1M vectors | > 1M vectors |

## Operations

### Collections

```go
// Create
coll, created := manager.GetOrCreateCollection(ctx, "docs", "Description")

// List
collections := manager.ListCollections(ctx)

// Delete
err := manager.DeleteCollection(ctx, "docs")
```

### Documents

```go
// Store
err := manager.Store(ctx, "docs", "key", document)

// Get
doc, err := manager.Get(ctx, "docs", "key")

// Delete
err := manager.Delete(ctx, "docs", "key")

// List
docs, err := manager.List(ctx, "docs", 100, 0)
```

### Search

```go
// Basic search
results, err := manager.Search(ctx, "docs", embedding, pgvector.SearchOptions{
    TopK: 10,
})

// With filters
results, err := manager.Search(ctx, "docs", embedding, pgvector.SearchOptions{
    TopK:     10,
    MinScore: 0.7,
    Where:    "metadata->>'type' = 'guide'",
})
```

## Batch Operations

```go
// Batch insert
batch := manager.NewBatch("docs")
for _, doc := range documents {
    batch.Add(doc)
}
err := batch.Execute(ctx)

// Batch with progress
err := batch.ExecuteWithProgress(ctx, func(done, total int) {
    fmt.Printf("Progress: %d/%d\n", done, total)
})
```

## Schema

The provider creates these tables:

```sql
-- Collections
CREATE TABLE omni_collections (
    id UUID PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    dimensions INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Documents
CREATE TABLE omni_documents (
    id UUID PRIMARY KEY,
    collection_id UUID REFERENCES omni_collections(id),
    key TEXT NOT NULL,
    content TEXT,
    embedding vector(384),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(collection_id, key)
);

-- HNSW index
CREATE INDEX ON omni_documents
USING hnsw (embedding vector_cosine_ops);
```

## Performance Tuning

### Connection Pool

```go
manager, _ := pgvector.NewManager(pgvector.Config{
    MaxConnections: 20, // Increase for high concurrency
})
```

### Index Tuning

```sql
-- Adjust HNSW search quality (higher = better quality, slower)
SET hnsw.ef_search = 100;

-- Adjust IVFFlat probes (higher = better quality, slower)
SET ivfflat.probes = 10;
```

### Query Optimization

```go
// Use EXPLAIN to analyze queries
rows, _ := db.Query(`
    EXPLAIN ANALYZE
    SELECT * FROM omni_documents
    ORDER BY embedding <=> $1
    LIMIT 10
`, queryEmbedding)
```

## Monitoring

### Index Statistics

```sql
-- Check index size
SELECT pg_size_pretty(pg_relation_size('omni_documents_embedding_idx'));

-- Check table statistics
SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname = 'omni_documents';
```

### Connection Pool Stats

```go
stats := manager.Stats()
fmt.Printf("Active: %d, Idle: %d\n", stats.ActiveConns, stats.IdleConns)
```

## Migration

### From In-Memory

```go
// Export from memory manager
docs, _ := memoryMgr.List(ctx, "collection", 10000, 0)

// Import to pgvector
batch := pgManager.NewBatch("collection")
for _, doc := range docs {
    batch.Add(doc)
}
batch.Execute(ctx)
```

### Schema Migrations

```go
// Run migrations
err := manager.Migrate(ctx)
```

## Best Practices

### Connection Management

```go
// Use single manager instance
var manager *pgvector.Manager

func init() {
    var err error
    manager, err = pgvector.NewManager(config)
    if err != nil {
        log.Fatal(err)
    }
}

// Graceful shutdown
func shutdown() {
    manager.Close()
}
```

### Error Handling

```go
result, err := manager.Search(ctx, coll, emb, opts)
if err != nil {
    if errors.Is(err, pgvector.ErrCollectionNotFound) {
        // Handle missing collection
    }
    if errors.Is(err, pgvector.ErrConnectionFailed) {
        // Handle connection issue
    }
    return err
}
```

### Backup and Restore

```bash
# Backup
pg_dump -t 'omni_*' mydb > omniretrieve_backup.sql

# Restore
psql mydb < omniretrieve_backup.sql
```

## See Also

- [Memory Manager](../concepts/memory.md) - In-memory alternative
- [Vector Search](../concepts/vector.md) - Vector search concepts
- [Hybrid Search](../concepts/hybrid.md) - Combined retrieval
