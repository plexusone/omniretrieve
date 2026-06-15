# API Reference

This page provides an overview of the main packages and types in OmniRetrieve.

## Packages

| Package | Description |
|---------|-------------|
| `vector` | Vector index and similarity search |
| `bm25` | BM25 keyword search |
| `hybrid` | Combined vector + BM25 search |
| `memory` | Collection-based memory manager |
| `rerank` | Result reranking |
| `retrieve` | Core retrieval interface |
| `providers/pgvector` | PostgreSQL vector storage |

## Core Types

### Document

```go
type Document struct {
    ID        string            `json:"id"`
    Content   string            `json:"content"`
    Embedding []float64         `json:"embedding,omitempty"`
    Metadata  map[string]string `json:"metadata,omitempty"`
    CreatedAt time.Time         `json:"created_at"`
}
```

### SearchResult

```go
type SearchResult struct {
    Document *Document
    Score    float64
}
```

### Embedder

```go
type Embedder interface {
    Embed(ctx context.Context, texts []string) ([][]float64, error)
    Dimensions() int
}
```

## memory Package

### Manager

```go
func NewManager(config ManagerConfig) *Manager
func (m *Manager) GetOrCreateCollection(ctx, name, desc string) (*Collection, bool)
func (m *Manager) Store(ctx, collection, key string, doc *Document) error
func (m *Manager) Search(ctx, collection, query string, opts SearchOptions) ([]SearchResult, error)
func (m *Manager) List(ctx, collection string, limit, offset int) ([]*Document, error)
func (m *Manager) Delete(ctx, collection, key string) error
func (m *Manager) ListCollections() []*Collection
```

### ManagerConfig

```go
type ManagerConfig struct {
    Embedder    Embedder
    DefaultTopK int
}
```

### SearchOptions

```go
type SearchOptions struct {
    TopK            int
    IncludeMetadata bool
    MinScore        float64
}
```

## bm25 Package

### Index

```go
func NewIndex(config ...Config) *Index
func (i *Index) Add(id, content string)
func (i *Index) Remove(id string)
func (i *Index) Search(query string, k int) []*ScoredDocument
func (i *Index) Stats() IndexStats
```

### Config

```go
type Config struct {
    K1        float64
    B         float64
    Tokenizer func(string) []string
}
```

## vector Package

### Index

```go
func NewIndex(config Config) *Index
func (i *Index) Add(id string, embedding []float64) error
func (i *Index) Remove(id string) error
func (i *Index) Search(query []float64, k int) []ScoredDocument
```

### Config

```go
type Config struct {
    Dimensions int
    Metric     Metric
    Normalize  bool
}
```

### Similarity Functions

```go
func CosineSimilarity(a, b []float64) float64
func EuclideanDistance(a, b []float64) float64
func DotProduct(a, b []float64) float64
func Normalize(v []float64) []float64
```

## hybrid Package

### Searcher

```go
func NewSearcher(config Config) *Searcher
func (s *Searcher) Add(ctx, id, content string, embedding []float64) error
func (s *Searcher) Search(ctx, query string, embedding []float64, k int) ([]Result, error)
func (s *Searcher) Remove(id string) error
```

### Config

```go
type Config struct {
    VectorIndex *vector.Index
    BM25Index   *bm25.Index
    Alpha       float64
    Fusion      FusionStrategy
    RRFConstant int
    Normalize   bool
}
```

## rerank Package

### Reranker

```go
type Reranker interface {
    Rerank(ctx context.Context, query string, docs []Document) ([]ScoredDocument, error)
}

func NewCrossEncoder(config Config) Reranker
func NewLLMReranker(config LLMConfig) Reranker
func NewCohereReranker(config CohereConfig) Reranker
```

## providers/pgvector Package

### Manager

```go
func NewManager(config Config) (*Manager, error)
func (m *Manager) Close() error
func (m *Manager) GetOrCreateCollection(ctx, name, desc string) (*Collection, bool)
func (m *Manager) Store(ctx, collection, key string, doc *Document) error
func (m *Manager) Search(ctx, collection string, embedding []float64, opts SearchOptions) ([]SearchResult, error)
func (m *Manager) NewBatch(collection string) *Batch
```

### Config

```go
type Config struct {
    ConnectionString string
    Dimensions       int
    TablePrefix      string
    IndexType        string
    DistanceFunction string
    MaxConnections   int
    CreateTables     bool
}
```

## Full Documentation

For complete API documentation, see [pkg.go.dev/github.com/plexusone/omniretrieve](https://pkg.go.dev/github.com/plexusone/omniretrieve).
