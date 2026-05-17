# OmniRetrieve - Technical Requirements Document

## System Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Layer                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────┐    ┌───────────┐    ┌───────────┐                │
│  │  Vector   │    │   Graph   │    │  Hybrid   │                │
│  │ Retriever │    │ Retriever │    │ Retriever │                │
│  └─────┬─────┘    └─────┬─────┘    └─────┬─────┘                │
│        │                │                │                      │
│        └────────────────┼────────────────┘                      │
│                         │                                       │
│                    ┌────▼────┐                                  │
│                    │ Reranker│                                  │
│                    └────┬────┘                                  │
│                         │                                       │
├─────────────────────────┼───────────────────────────────────────┤
│                    ┌────▼────┐                                  │
│                    │Observer │  ──────► Exporters               │
│                    └────┬────┘          (Phoenix, Opik, etc.)   │
│                         │                                       │
├─────────────────────────┼───────────────────────────────────────┤
│                   Provider Layer                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ pgvector │  │ Pinecone │  │  Neo4j   │  │ Neptune  │         │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

### Package Structure

```
omniretrieve/
├── retrieve/          # Core interfaces
│   └── retrieve.go    # Retriever, Query, Result, ContextItem
│
├── vector/            # Vector retrieval
│   ├── vector.go      # Index, BatchIndex, IndexManager interfaces
│   ├── retriever.go   # Vector retriever implementation
│   └── vector_test.go
│
├── graph/             # Graph retrieval
│   ├── graph.go       # KnowledgeGraph, GraphManager interfaces
│   ├── retriever.go   # Graph retriever implementation
│   └── graph_test.go
│
├── hybrid/            # Hybrid retrieval
│   ├── hybrid.go      # Hybrid retriever with policies
│   └── hybrid_test.go
│
├── observe/           # Observability
│   ├── observe.go     # Observer, Span, Exporter interfaces
│   └── observe_test.go
│
├── rerank/            # Reranking
│   ├── rerank.go      # Reranker interface and implementations
│   └── rerank_test.go
│
├── memory/            # In-memory implementations
│   ├── vector.go      # In-memory vector index
│   ├── graph.go       # In-memory knowledge graph
│   └── embedder.go    # Hash-based embedder for testing
│
└── providers/
    └── pgvector/      # PostgreSQL pgvector provider
        ├── go.mod     # Separate module
        ├── pgvector.go
        ├── batch.go
        └── manager.go
```

## Core Interfaces

### Retriever Interface

```go
// Retriever is the primary interface for all retrieval strategies.
type Retriever interface {
    Retrieve(ctx context.Context, query Query) (*Result, error)
}

// Query represents a retrieval request.
type Query struct {
    Text       string            // Natural language query
    Embedding  []float32         // Pre-computed embedding (optional)
    Filters    map[string]string // Metadata filters
    TopK       int               // Maximum results
    MinScore   float64           // Minimum relevance threshold
}

// Result contains retrieved context items.
type Result struct {
    Items    []ContextItem
    Metadata map[string]any
}

// ContextItem represents a single retrieved piece of context.
type ContextItem struct {
    ID       string
    Content  string
    Source   string
    Score    float64
    Metadata map[string]string
}
```

### Vector Index Interface

```go
// Index provides vector storage and search.
type Index interface {
    Search(ctx context.Context, embedding []float32, k int, filters map[string]string) ([]SearchResult, error)
    Insert(ctx context.Context, node Node) error
    Upsert(ctx context.Context, node Node) error
    Delete(ctx context.Context, id string) error
    Name() string
}

// BatchIndex extends Index with batch operations.
type BatchIndex interface {
    Index
    InsertBatch(ctx context.Context, nodes []Node) error
    UpsertBatch(ctx context.Context, nodes []Node) error
    DeleteBatch(ctx context.Context, ids []string) error
}

// IndexManager handles index lifecycle.
type IndexManager interface {
    CreateIndex(ctx context.Context, cfg IndexConfig) error
    DropIndex(ctx context.Context, name string) error
    IndexExists(ctx context.Context, name string) (bool, error)
    IndexStats(ctx context.Context, name string) (*IndexStats, error)
    ListIndexes(ctx context.Context) ([]string, error)
}
```

### Knowledge Graph Interface

```go
// KnowledgeGraph provides entity-relationship storage and traversal.
type KnowledgeGraph interface {
    Traverse(ctx context.Context, query TraversalQuery) ([]TraversalResult, error)
    AddEntity(ctx context.Context, entity Entity) error
    AddRelation(ctx context.Context, relation Relation) error
    DeleteEntity(ctx context.Context, id string) error
    DeleteRelation(ctx context.Context, id string) error
}

// TraversalQuery defines graph traversal parameters.
type TraversalQuery struct {
    StartEntityID string
    MaxDepth      int
    RelationTypes []string
    Direction     Direction
    Limit         int
}
```

## Design Decisions

### D1: Separate Modules for Providers

**Decision**: Each provider (pgvector, pinecone, neo4j) lives in its own Go module under `providers/`.

**Rationale**:
- Avoids pulling unnecessary dependencies (e.g., pgvector users don't need Neo4j driver)
- Follows AWS SDK v2 pattern
- Enables independent versioning of providers
- Reduces binary size

**Trade-offs**:
- More complex release process
- Replace directives needed for local development

### D2: Interface-Based Design

**Decision**: All major components are interfaces, not concrete types.

**Rationale**:
- Enables easy mocking for tests
- Allows swapping implementations
- Follows Go idioms

**Trade-offs**:
- Slightly more verbose
- Must be careful about interface pollution

### D3: Context-First APIs

**Decision**: All operations take `context.Context` as first parameter.

**Rationale**:
- Standard Go pattern
- Enables cancellation and timeouts
- Required for observability (trace propagation)

### D4: Observability Built-In

**Decision**: Observer is integrated into retrievers, not bolted on.

**Rationale**:
- Consistent tracing across all operations
- No manual instrumentation needed
- Compatible with existing observability tools

**Trade-offs**:
- Small overhead even when not used
- Must handle nil observer gracefully

### D5: Hybrid Policies as Configuration

**Decision**: Hybrid retrieval policies (parallel, sequential) are configuration, not separate types.

**Rationale**:
- Simpler API (one HybridRetriever type)
- Easy to switch strategies
- Runtime configuration possible

## Data Flow

### Vector Retrieval Flow

```
Query → Embed Text → Search Index → Filter by Score → Rerank → Return Results
  │         │            │               │              │           │
  ▼         ▼            ▼               ▼              ▼           ▼
Query   Embedder      Index         MinScore       Reranker     Result
.Text   .Embed()     .Search()      Filter         .Rerank()    .Items
```

### Hybrid Retrieval Flow (Parallel Policy)

```
                    ┌─────────────────┐
                    │      Query      │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌─────────────────┐           ┌─────────────────┐
    │ Vector Retriever│           │ Graph Retriever │
    └────────┬────────┘           └────────┬────────┘
             │                             │
             └──────────────┬──────────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │  Merge Results  │
                   │ (weighted, RRF) │
                   └────────┬────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │    Deduplicate  │
                   └────────┬────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │     Rerank      │
                   └────────┬────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │     Result      │
                   └─────────────────┘
```

## Performance Considerations

### Memory Allocation

- Pre-allocate slices where size is known
- Reuse buffers for embedding serialization
- Use `sync.Pool` for frequently allocated objects

### Concurrency

- Hybrid parallel policy uses goroutines with proper synchronization
- Index operations are goroutine-safe (provider responsibility)
- Observer uses mutex for span storage

### Latency Optimization

- Batch operations for bulk inserts
- Connection pooling (provider responsibility)
- Lazy embedding (only embed if not provided)

## Security Considerations

### SQL Injection Prevention

- All pgvector queries use parameterized statements
- Table names use `pq.QuoteIdentifier()`
- No string concatenation of user input

### Input Validation

- Dimension validation on index creation
- ID format validation
- Metadata key/value length limits

## Testing Strategy

### Unit Tests

- All packages have `*_test.go` files
- Use in-memory implementations from `memory/` package
- No external dependencies

### Integration Tests

- Build tag `//go:build integration`
- Require running database (pgvector, Neo4j, etc.)
- Environment variable configuration

### Benchmarks

- Planned for v0.2.0
- Focus on hot paths (search, embedding serialization)
- Memory allocation profiling

## Monitoring and Observability

### Span Types

| Span Type | Attributes |
|-----------|------------|
| `retrieval` | query, top_k, result_count, latency_ms |
| `vector.search` | backend, top_k, result_count, latency_ms |
| `graph.traverse` | backend, depth, node_count, latency_ms |
| `rerank` | model, input_count, output_count, latency_ms |

### Exporter Interface

```go
type Exporter interface {
    Export(ctx context.Context, spans []Span) error
    Shutdown(ctx context.Context) error
}
```

## Dependencies

### Core Module

- None (stdlib only)

### pgvector Provider

- `github.com/lib/pq` - PostgreSQL driver
- `github.com/plexusone/omniretrieve` - Core interfaces

## Deployment

OmniRetrieve is a library, not a service. Deployment considerations:

- Import as Go module
- Configure providers with connection strings
- Handle provider credentials via environment variables
- Infrastructure (databases) deployed separately via agentkit-aws-*

## Future Considerations

### Streaming Results

```go
type StreamingRetriever interface {
    RetrieveStream(ctx context.Context, query Query) (<-chan ContextItem, error)
}
```

### Query Planning

Automatic strategy selection based on query characteristics.

### Caching Layer

Optional caching for frequently accessed embeddings/results.

### Multi-Tenancy

Tenant isolation at the index level.
