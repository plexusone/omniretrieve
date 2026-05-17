// Package pgvector provides a PostgreSQL/pgvector implementation of OmniRetrieve's
// vector.Index interface for vector similarity search.
//
// # Features
//
//   - Full vector.Index, vector.BatchIndex, and vector.IndexManager support
//   - HNSW and IVFFlat index types
//   - Cosine, Euclidean, and Inner Product distance metrics
//   - Efficient batch upsert using PostgreSQL's ON CONFLICT
//   - Metadata filtering via JSONB
//
// # Usage
//
//	import (
//		"database/sql"
//		_ "github.com/lib/pq"
//		"github.com/plexusone/omniretrieve/providers/pgvector"
//	)
//
//	// Connect to PostgreSQL
//	db, err := sql.Open("postgres", "postgres://user:pass@localhost/mydb?sslmode=disable")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create index with default configuration
//	idx, err := pgvector.New(db, pgvector.DefaultConfig("embeddings", 1536))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Use with OmniRetrieve
//	retriever := vector.NewRetriever(vector.RetrieverConfig{
//		Index:    idx,
//		Embedder: myEmbedder,
//	})
//
// # Configuration
//
// The Config struct allows customization of:
//
//   - Table name and vector dimensions
//   - Distance metric (cosine, euclidean, inner_product)
//   - Index type (HNSW, IVFFlat, or none)
//   - HNSW parameters (M, ef_construction)
//   - IVFFlat parameters (lists)
//
// # Requirements
//
//   - PostgreSQL 11+ with pgvector extension installed
//   - CREATE EXTENSION permissions (or pre-installed extension)
//
// # Index Types
//
// HNSW (recommended):
//   - Best for high recall and low latency
//   - Higher memory usage
//   - Good for datasets up to ~10M vectors
//
// IVFFlat:
//   - Good balance of speed and accuracy
//   - Lower memory usage
//   - Requires training (happens automatically)
//   - Good for larger datasets
//
// Flat (no index):
//   - Exact search (100% recall)
//   - Slow for large datasets
//   - Use only for small datasets or testing
package pgvector
