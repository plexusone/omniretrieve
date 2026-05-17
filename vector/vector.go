// Package vector provides vector similarity search for retrieval.
package vector

import (
	"context"
	"time"

	"github.com/plexusone/omniretrieve/retrieve"
)

// Node represents a node in the vector index.
type Node struct {
	// ID is the unique identifier for this node.
	ID string
	// Content is the text content of this node.
	Content string
	// Embedding is the vector embedding for this node.
	Embedding []float32
	// Source identifies where this node came from.
	Source string
	// Metadata contains additional node metadata.
	Metadata map[string]string
}

// SearchResult represents a single search result from vector search.
type SearchResult struct {
	// Node is the matched node.
	Node Node
	// Score is the similarity score (0.0-1.0).
	Score float64
}

// Index defines the interface for vector index operations.
type Index interface {
	// Search finds the k most similar nodes to the given embedding.
	Search(ctx context.Context, embedding []float32, k int, filters map[string]string) ([]SearchResult, error)
	// Insert adds a node to the index.
	Insert(ctx context.Context, node Node) error
	// Upsert inserts or updates a node in the index.
	Upsert(ctx context.Context, node Node) error
	// Delete removes a node from the index.
	Delete(ctx context.Context, id string) error
	// Name returns the name/identifier of this index.
	Name() string
}

// BatchIndex extends Index with batch operations for efficiency.
type BatchIndex interface {
	Index
	// InsertBatch adds multiple nodes to the index.
	InsertBatch(ctx context.Context, nodes []Node) error
	// UpsertBatch inserts or updates multiple nodes.
	UpsertBatch(ctx context.Context, nodes []Node) error
	// DeleteBatch removes multiple nodes from the index.
	DeleteBatch(ctx context.Context, ids []string) error
}

// IndexConfig configures a vector index.
type IndexConfig struct {
	// Name is the index name.
	Name string
	// Dimensions is the vector dimension size.
	Dimensions int
	// DistanceMetric is the distance function (cosine, euclidean, dot).
	DistanceMetric DistanceMetric
	// IndexType is the index algorithm (hnsw, ivfflat, flat).
	IndexType IndexType
	// HNSWConfig contains HNSW-specific settings.
	HNSWConfig *HNSWConfig
}

// DistanceMetric defines the distance function for similarity.
type DistanceMetric string

const (
	DistanceCosine    DistanceMetric = "cosine"
	DistanceEuclidean DistanceMetric = "euclidean"
	DistanceDot       DistanceMetric = "dot"
)

// IndexType defines the index algorithm.
type IndexType string

const (
	IndexTypeHNSW    IndexType = "hnsw"
	IndexTypeIVFFlat IndexType = "ivfflat"
	IndexTypeFlat    IndexType = "flat"
)

// HNSWConfig contains HNSW index parameters.
type HNSWConfig struct {
	// M is the number of connections per layer.
	M int
	// EfConstruction is the size of the dynamic candidate list during construction.
	EfConstruction int
	// EfSearch is the size of the dynamic candidate list during search.
	EfSearch int
}

// IndexStats contains index statistics.
type IndexStats struct {
	// Name is the index name.
	Name string
	// NodeCount is the number of nodes in the index.
	NodeCount int64
	// Dimensions is the vector dimension size.
	Dimensions int
	// IndexSizeBytes is the approximate index size in bytes.
	IndexSizeBytes int64
}

// IndexManager provides index lifecycle operations.
type IndexManager interface {
	// CreateIndex creates a new vector index.
	CreateIndex(ctx context.Context, cfg IndexConfig) error
	// DropIndex removes an index.
	DropIndex(ctx context.Context, name string) error
	// IndexExists checks if an index exists.
	IndexExists(ctx context.Context, name string) (bool, error)
	// IndexStats returns statistics for an index.
	IndexStats(ctx context.Context, name string) (*IndexStats, error)
	// ListIndexes returns all index names.
	ListIndexes(ctx context.Context) ([]string, error)
}

// Embedder creates embeddings from text.
type Embedder interface {
	// Embed creates an embedding for the given text.
	Embed(ctx context.Context, text string) ([]float32, error)
	// EmbedBatch creates embeddings for multiple texts.
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
	// Model returns the name of the embedding model.
	Model() string
}

// RetrieverConfig configures the vector retriever.
type RetrieverConfig struct {
	// Index is the vector index to search.
	Index Index
	// Embedder creates embeddings for queries.
	Embedder Embedder
	// DefaultTopK is the default number of results to return.
	DefaultTopK int
	// MinScore is the minimum similarity score threshold.
	MinScore float64
	// Observer for tracing and metrics.
	Observer retrieve.Observer
}

// Retriever implements vector-based retrieval.
type Retriever struct {
	config RetrieverConfig
}

// NewRetriever creates a new vector retriever.
func NewRetriever(cfg RetrieverConfig) *Retriever {
	if cfg.DefaultTopK == 0 {
		cfg.DefaultTopK = 10
	}
	return &Retriever{config: cfg}
}

// Retrieve performs vector similarity search.
func (r *Retriever) Retrieve(ctx context.Context, q retrieve.Query) (*retrieve.Result, error) {
	start := time.Now()

	// Get or compute embedding
	embedding := q.Embedding
	if len(embedding) == 0 && r.config.Embedder != nil {
		var err error
		embedding, err = r.config.Embedder.Embed(ctx, q.Text)
		if err != nil {
			return nil, err
		}
	}

	// Determine top-k
	topK := q.TopK
	if topK == 0 {
		topK = r.config.DefaultTopK
	}

	// Perform search
	results, err := r.config.Index.Search(ctx, embedding, topK, q.Filters)
	if err != nil {
		return nil, err
	}

	// Convert to context items
	minScore := q.MinScore
	if minScore == 0 {
		minScore = r.config.MinScore
	}

	items := make([]retrieve.ContextItem, 0, len(results))
	for _, res := range results {
		if res.Score < minScore {
			continue
		}
		items = append(items, retrieve.ContextItem{
			ID:       res.Node.ID,
			Content:  res.Node.Content,
			Source:   res.Node.Source,
			Score:    res.Score,
			Metadata: res.Node.Metadata,
			Provenance: retrieve.Provenance{
				Mode:            retrieve.ModeVector,
				Backend:         r.config.Index.Name(),
				SimilarityScore: res.Score,
			},
		})
	}

	latency := time.Since(start).Milliseconds()

	// Report to observer
	if r.config.Observer != nil {
		r.config.Observer.OnVectorSearch(ctx, r.config.Index.Name(), topK, len(items), latency)
	}

	return &retrieve.Result{
		Items: items,
		Query: q,
		Metadata: retrieve.ResultMetadata{
			TotalCandidates: len(results),
			LatencyMS:       latency,
			ModesUsed:       []retrieve.Mode{retrieve.ModeVector},
		},
	}, nil
}
