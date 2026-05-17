// Package pgvector provides a pgvector implementation of vector.Index for OmniRetrieve.
package pgvector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/plexusone/omniretrieve/vector"
	"github.com/lib/pq"
)

// Index implements vector.Index using PostgreSQL with pgvector extension.
type Index struct {
	db        *sql.DB
	tableName string
	config    Config
}

// Config configures the pgvector index.
type Config struct {
	// TableName is the name of the table to use for vectors.
	TableName string
	// Dimensions is the vector dimension size.
	Dimensions int
	// DistanceMetric is the distance function (cosine, euclidean, inner_product).
	DistanceMetric DistanceMetric
	// CreateTableIfNotExists creates the table on first use if true.
	CreateTableIfNotExists bool
	// IndexType specifies the index algorithm (hnsw, ivfflat, or none).
	IndexType IndexType
	// HNSWConfig contains HNSW-specific parameters.
	HNSWConfig *HNSWConfig
	// IVFFlatConfig contains IVFFlat-specific parameters.
	IVFFlatConfig *IVFFlatConfig
}

// DistanceMetric defines the distance function for similarity.
type DistanceMetric string

const (
	// DistanceCosine uses cosine distance (1 - cosine similarity).
	DistanceCosine DistanceMetric = "cosine"
	// DistanceEuclidean uses L2 (Euclidean) distance.
	DistanceEuclidean DistanceMetric = "euclidean"
	// DistanceInnerProduct uses negative inner product (for max inner product search).
	DistanceInnerProduct DistanceMetric = "inner_product"
)

// IndexType defines the vector index algorithm.
type IndexType string

const (
	// IndexTypeNone uses no index (brute force).
	IndexTypeNone IndexType = "none"
	// IndexTypeHNSW uses HNSW (Hierarchical Navigable Small World) index.
	IndexTypeHNSW IndexType = "hnsw"
	// IndexTypeIVFFlat uses IVFFlat (Inverted File with Flat compression) index.
	IndexTypeIVFFlat IndexType = "ivfflat"
)

// HNSWConfig contains HNSW index parameters.
type HNSWConfig struct {
	// M is the number of connections per layer (default 16).
	M int
	// EfConstruction is the size of the dynamic candidate list during construction (default 64).
	EfConstruction int
}

// IVFFlatConfig contains IVFFlat index parameters.
type IVFFlatConfig struct {
	// Lists is the number of inverted lists (default sqrt(n) where n is row count).
	Lists int
}

// DefaultConfig returns a default configuration.
func DefaultConfig(tableName string, dimensions int) Config {
	return Config{
		TableName:              tableName,
		Dimensions:             dimensions,
		DistanceMetric:         DistanceCosine,
		CreateTableIfNotExists: true,
		IndexType:              IndexTypeHNSW,
		HNSWConfig: &HNSWConfig{
			M:              16,
			EfConstruction: 64,
		},
	}
}

// New creates a new pgvector Index.
func New(db *sql.DB, cfg Config) (*Index, error) {
	if cfg.TableName == "" {
		return nil, fmt.Errorf("table name is required")
	}
	if cfg.Dimensions <= 0 {
		return nil, fmt.Errorf("dimensions must be positive")
	}
	if cfg.DistanceMetric == "" {
		cfg.DistanceMetric = DistanceCosine
	}

	idx := &Index{
		db:        db,
		tableName: cfg.TableName,
		config:    cfg,
	}

	if cfg.CreateTableIfNotExists {
		if err := idx.ensureTable(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return idx, nil
}

// ensureTable creates the vector table if it doesn't exist.
func (idx *Index) ensureTable(ctx context.Context) error {
	// Ensure pgvector extension is available
	_, err := idx.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector")
	if err != nil {
		return fmt.Errorf("failed to create vector extension: %w", err)
	}

	// Create table
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			content TEXT,
			embedding vector(%d),
			source TEXT,
			metadata JSONB DEFAULT '{}'::jsonb,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)
	`, pq.QuoteIdentifier(idx.tableName), idx.config.Dimensions)

	_, err = idx.db.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create vector index based on configuration
	if idx.config.IndexType != IndexTypeNone {
		if err := idx.createVectorIndex(ctx); err != nil {
			return fmt.Errorf("failed to create vector index: %w", err)
		}
	}

	return nil
}

// createVectorIndex creates the appropriate vector index.
func (idx *Index) createVectorIndex(ctx context.Context) error {
	indexName := fmt.Sprintf("%s_embedding_idx", idx.tableName)
	opClass := idx.distanceOpClass()

	var createSQL string
	switch idx.config.IndexType {
	case IndexTypeHNSW:
		m := 16
		efConstruction := 64
		if idx.config.HNSWConfig != nil {
			if idx.config.HNSWConfig.M > 0 {
				m = idx.config.HNSWConfig.M
			}
			if idx.config.HNSWConfig.EfConstruction > 0 {
				efConstruction = idx.config.HNSWConfig.EfConstruction
			}
		}
		createSQL = fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS %s ON %s
			USING hnsw (embedding %s)
			WITH (m = %d, ef_construction = %d)
		`, pq.QuoteIdentifier(indexName), pq.QuoteIdentifier(idx.tableName), opClass, m, efConstruction)

	case IndexTypeIVFFlat:
		lists := 100 // Default
		if idx.config.IVFFlatConfig != nil && idx.config.IVFFlatConfig.Lists > 0 {
			lists = idx.config.IVFFlatConfig.Lists
		}
		createSQL = fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS %s ON %s
			USING ivfflat (embedding %s)
			WITH (lists = %d)
		`, pq.QuoteIdentifier(indexName), pq.QuoteIdentifier(idx.tableName), opClass, lists)

	default:
		return nil
	}

	_, err := idx.db.ExecContext(ctx, createSQL)
	return err
}

// distanceOpClass returns the pgvector operator class for the configured distance metric.
func (idx *Index) distanceOpClass() string {
	switch idx.config.DistanceMetric {
	case DistanceEuclidean:
		return "vector_l2_ops"
	case DistanceInnerProduct:
		return "vector_ip_ops"
	default: // Cosine
		return "vector_cosine_ops"
	}
}

// distanceOperator returns the SQL operator for the configured distance metric.
func (idx *Index) distanceOperator() string {
	switch idx.config.DistanceMetric {
	case DistanceEuclidean:
		return "<->"
	case DistanceInnerProduct:
		return "<#>"
	default: // Cosine
		return "<=>"
	}
}

// Search implements vector.Index.
func (idx *Index) Search(ctx context.Context, embedding []float32, k int, filters map[string]string) ([]vector.SearchResult, error) {
	// Build query
	op := idx.distanceOperator()
	embeddingStr := vectorToString(embedding)

	//nolint:gosec // Table name escaped via pq.QuoteIdentifier, operator is from fixed set
	query := fmt.Sprintf(`
		SELECT id, content, embedding, source, metadata,
		       1 - (embedding %s $1::vector) as score
		FROM %s
	`, op, pq.QuoteIdentifier(idx.tableName))

	args := []any{embeddingStr}
	argIdx := 2

	// Add metadata filters
	if len(filters) > 0 {
		conditions := make([]string, 0, len(filters))
		for key, value := range filters {
			conditions = append(conditions, fmt.Sprintf("metadata->>$%d = $%d", argIdx, argIdx+1))
			args = append(args, key, value)
			argIdx += 2
		}
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += fmt.Sprintf(" ORDER BY embedding %s $1::vector LIMIT $%d", op, argIdx)
	args = append(args, k)

	rows, err := idx.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []vector.SearchResult
	for rows.Next() {
		var (
			id           string
			content      sql.NullString
			embeddingRaw string
			source       sql.NullString
			metadataRaw  []byte
			score        float64
		)

		if err := rows.Scan(&id, &content, &embeddingRaw, &source, &metadataRaw, &score); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		metadata := make(map[string]string)
		if len(metadataRaw) > 0 {
			var rawMap map[string]any
			if err := json.Unmarshal(metadataRaw, &rawMap); err == nil {
				for k, v := range rawMap {
					if s, ok := v.(string); ok {
						metadata[k] = s
					}
				}
			}
		}

		emb := parseVector(embeddingRaw)

		results = append(results, vector.SearchResult{
			Node: vector.Node{
				ID:        id,
				Content:   content.String,
				Embedding: emb,
				Source:    source.String,
				Metadata:  metadata,
			},
			Score: score,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// Insert implements vector.Index.
func (idx *Index) Insert(ctx context.Context, node vector.Node) error {
	metadataJSON, err := json.Marshal(node.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, content, embedding, source, metadata)
		VALUES ($1, $2, $3::vector, $4, $5::jsonb)
	`, pq.QuoteIdentifier(idx.tableName))

	_, err = idx.db.ExecContext(ctx, query,
		node.ID,
		node.Content,
		vectorToString(node.Embedding),
		node.Source,
		string(metadataJSON),
	)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	return nil
}

// Upsert implements vector.Index.
func (idx *Index) Upsert(ctx context.Context, node vector.Node) error {
	metadataJSON, err := json.Marshal(node.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, content, embedding, source, metadata)
		VALUES ($1, $2, $3::vector, $4, $5::jsonb)
		ON CONFLICT (id) DO UPDATE SET
			content = EXCLUDED.content,
			embedding = EXCLUDED.embedding,
			source = EXCLUDED.source,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`, pq.QuoteIdentifier(idx.tableName))

	_, err = idx.db.ExecContext(ctx, query,
		node.ID,
		node.Content,
		vectorToString(node.Embedding),
		node.Source,
		string(metadataJSON),
	)
	if err != nil {
		return fmt.Errorf("upsert failed: %w", err)
	}

	return nil
}

// Delete implements vector.Index.
func (idx *Index) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", pq.QuoteIdentifier(idx.tableName))
	_, err := idx.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

// Name implements vector.Index.
func (idx *Index) Name() string {
	return idx.tableName
}

// vectorToString converts a float32 slice to pgvector string format.
func vectorToString(v []float32) string {
	strs := make([]string, len(v))
	for i, f := range v {
		strs[i] = fmt.Sprintf("%f", f)
	}
	return "[" + strings.Join(strs, ",") + "]"
}

// parseVector parses a pgvector string to float32 slice.
func parseVector(s string) []float32 {
	// Remove brackets
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")

	if s == "" {
		return nil
	}

	parts := strings.Split(s, ",")
	result := make([]float32, len(parts))
	for i, p := range parts {
		f, _ := strconv.ParseFloat(strings.TrimSpace(p), 64)
		result[i] = float32(f)
	}
	return result
}

// Verify interface compliance
var _ vector.Index = (*Index)(nil)
