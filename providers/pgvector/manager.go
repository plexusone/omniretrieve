package pgvector

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/plexusone/omniretrieve/vector"
	"github.com/lib/pq"
)

// Manager implements vector.IndexManager for PostgreSQL with pgvector.
type Manager struct {
	db *sql.DB
}

// NewManager creates a new index manager.
func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// CreateIndex implements vector.IndexManager.
func (m *Manager) CreateIndex(ctx context.Context, cfg vector.IndexConfig) error {
	// Ensure pgvector extension is available
	_, err := m.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector")
	if err != nil {
		return fmt.Errorf("failed to create vector extension: %w", err)
	}

	// Create table
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			content TEXT,
			embedding vector(%d),
			source TEXT,
			metadata JSONB DEFAULT '{}'::jsonb,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)
	`, pq.QuoteIdentifier(cfg.Name), cfg.Dimensions)

	_, err = m.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create vector index if specified
	if cfg.IndexType != "" && cfg.IndexType != vector.IndexTypeFlat {
		opClass := distanceMetricToOpClass(cfg.DistanceMetric)
		indexName := fmt.Sprintf("%s_embedding_idx", cfg.Name)

		var createIndexSQL string
		switch cfg.IndexType {
		case vector.IndexTypeHNSW:
			m := 16
			efConstruction := 64
			if cfg.HNSWConfig != nil {
				if cfg.HNSWConfig.M > 0 {
					m = cfg.HNSWConfig.M
				}
				if cfg.HNSWConfig.EfConstruction > 0 {
					efConstruction = cfg.HNSWConfig.EfConstruction
				}
			}
			createIndexSQL = fmt.Sprintf(`
				CREATE INDEX IF NOT EXISTS %s ON %s
				USING hnsw (embedding %s)
				WITH (m = %d, ef_construction = %d)
			`, pq.QuoteIdentifier(indexName), pq.QuoteIdentifier(cfg.Name), opClass, m, efConstruction)

		case vector.IndexTypeIVFFlat:
			lists := 100 // Default
			createIndexSQL = fmt.Sprintf(`
				CREATE INDEX IF NOT EXISTS %s ON %s
				USING ivfflat (embedding %s)
				WITH (lists = %d)
			`, pq.QuoteIdentifier(indexName), pq.QuoteIdentifier(cfg.Name), opClass, lists)
		}

		if createIndexSQL != "" {
			_, err = m.db.ExecContext(ctx, createIndexSQL)
			if err != nil {
				return fmt.Errorf("failed to create vector index: %w", err)
			}
		}
	}

	return nil
}

// DropIndex implements vector.IndexManager.
func (m *Manager) DropIndex(ctx context.Context, name string) error {
	// Drop the table (CASCADE will remove the index too)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", pq.QuoteIdentifier(name))
	_, err := m.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	return nil
}

// IndexExists implements vector.IndexManager.
func (m *Manager) IndexExists(ctx context.Context, name string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_name = $1
		)
	`
	var exists bool
	err := m.db.QueryRowContext(ctx, query, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}
	return exists, nil
}

// IndexStats implements vector.IndexManager.
func (m *Manager) IndexStats(ctx context.Context, name string) (*vector.IndexStats, error) {
	// Get row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(name))
	var count int64
	if err := m.db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	// Get dimensions from column definition (best effort, ignore errors)
	dimQuery := `
		SELECT character_maximum_length
		FROM information_schema.columns
		WHERE table_name = $1 AND column_name = 'embedding'
	`
	var dimensions sql.NullInt64
	_ = m.db.QueryRowContext(ctx, dimQuery, name).Scan(&dimensions)

	// Get table size (best effort, ignore errors)
	//nolint:gosec // Table name escaped via pq.QuoteLiteral
	sizeQuery := fmt.Sprintf("SELECT pg_total_relation_size(%s)", pq.QuoteLiteral(name))
	var size int64
	_ = m.db.QueryRowContext(ctx, sizeQuery).Scan(&size)

	return &vector.IndexStats{
		Name:           name,
		NodeCount:      count,
		Dimensions:     int(dimensions.Int64),
		IndexSizeBytes: size,
	}, nil
}

// ListIndexes implements vector.IndexManager.
func (m *Manager) ListIndexes(ctx context.Context) ([]string, error) {
	// Find tables that have a vector column named 'embedding'
	query := `
		SELECT table_name
		FROM information_schema.columns
		WHERE column_name = 'embedding'
		  AND data_type = 'USER-DEFINED'
		  AND udt_name = 'vector'
	`

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, name)
	}

	return tables, rows.Err()
}

// distanceMetricToOpClass converts OmniRetrieve distance metric to pgvector operator class.
func distanceMetricToOpClass(metric vector.DistanceMetric) string {
	switch metric {
	case vector.DistanceEuclidean:
		return "vector_l2_ops"
	case vector.DistanceDot:
		return "vector_ip_ops"
	default: // Cosine
		return "vector_cosine_ops"
	}
}

// Verify interface compliance
var _ vector.IndexManager = (*Manager)(nil)
