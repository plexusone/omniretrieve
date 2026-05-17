//go:build integration

package pgvector_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/plexusone/omniretrieve/providers/pgvector"
	"github.com/plexusone/omniretrieve/vector"
	_ "github.com/lib/pq"
)

func getTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("PGVECTOR_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/omniretrieve_test?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	return db
}

func TestIndex_CRUD(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tableName := fmt.Sprintf("test_vectors_%d", os.Getpid())

	// Create index
	idx, err := pgvector.New(db, pgvector.Config{
		TableName:              tableName,
		Dimensions:             128,
		DistanceMetric:         pgvector.DistanceCosine,
		CreateTableIfNotExists: true,
		IndexType:              pgvector.IndexTypeHNSW,
	})
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Clean up
	defer func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Insert
	node := vector.Node{
		ID:        "test-1",
		Content:   "This is a test document",
		Embedding: make([]float32, 128),
		Source:    "test",
		Metadata:  map[string]string{"category": "test"},
	}
	// Set some embedding values
	for i := range node.Embedding {
		node.Embedding[i] = float32(i) / 128.0
	}

	if err := idx.Insert(ctx, node); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Search
	results, err := idx.Search(ctx, node.Embedding, 10, nil)
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	if results[0].Node.ID != "test-1" {
		t.Errorf("expected ID 'test-1', got '%s'", results[0].Node.ID)
	}

	// Upsert (update)
	node.Content = "Updated content"
	if err := idx.Upsert(ctx, node); err != nil {
		t.Fatalf("failed to upsert: %v", err)
	}

	// Search again
	results, err = idx.Search(ctx, node.Embedding, 10, nil)
	if err != nil {
		t.Fatalf("failed to search after upsert: %v", err)
	}

	if results[0].Node.Content != "Updated content" {
		t.Errorf("expected updated content, got '%s'", results[0].Node.Content)
	}

	// Delete
	if err := idx.Delete(ctx, "test-1"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Search after delete
	results, err = idx.Search(ctx, node.Embedding, 10, nil)
	if err != nil {
		t.Fatalf("failed to search after delete: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}
}

func TestIndex_MetadataFilter(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tableName := fmt.Sprintf("test_vectors_filter_%d", os.Getpid())

	idx, err := pgvector.New(db, pgvector.DefaultConfig(tableName, 64))
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	defer func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Insert nodes with different categories
	nodes := []vector.Node{
		{ID: "tech-1", Content: "Technology article", Embedding: make([]float32, 64), Metadata: map[string]string{"category": "tech"}},
		{ID: "tech-2", Content: "Another tech article", Embedding: make([]float32, 64), Metadata: map[string]string{"category": "tech"}},
		{ID: "food-1", Content: "Recipe article", Embedding: make([]float32, 64), Metadata: map[string]string{"category": "food"}},
	}

	for i := range nodes {
		for j := range nodes[i].Embedding {
			nodes[i].Embedding[j] = float32(i*100+j) / 1000.0
		}
		if err := idx.Insert(ctx, nodes[i]); err != nil {
			t.Fatalf("failed to insert node %s: %v", nodes[i].ID, err)
		}
	}

	// Search with filter
	results, err := idx.Search(ctx, nodes[0].Embedding, 10, map[string]string{"category": "tech"})
	if err != nil {
		t.Fatalf("failed to search with filter: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 tech results, got %d", len(results))
	}

	for _, r := range results {
		if r.Node.Metadata["category"] != "tech" {
			t.Errorf("expected category 'tech', got '%s'", r.Node.Metadata["category"])
		}
	}
}

func TestIndex_BatchOperations(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tableName := fmt.Sprintf("test_vectors_batch_%d", os.Getpid())

	idx, err := pgvector.New(db, pgvector.DefaultConfig(tableName, 64))
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	defer func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Prepare batch of nodes
	nodes := make([]vector.Node, 100)
	for i := range nodes {
		nodes[i] = vector.Node{
			ID:        fmt.Sprintf("batch-%d", i),
			Content:   fmt.Sprintf("Batch document %d", i),
			Embedding: make([]float32, 64),
			Source:    "batch",
			Metadata:  map[string]string{"index": fmt.Sprintf("%d", i)},
		}
		for j := range nodes[i].Embedding {
			nodes[i].Embedding[j] = float32(i*100+j) / 10000.0
		}
	}

	// Batch upsert
	if err := idx.UpsertBatch(ctx, nodes); err != nil {
		t.Fatalf("failed to upsert batch: %v", err)
	}

	// Search to verify
	results, err := idx.Search(ctx, nodes[0].Embedding, 100, nil)
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}

	if len(results) != 100 {
		t.Errorf("expected 100 results, got %d", len(results))
	}

	// Batch delete
	ids := make([]string, 50)
	for i := range ids {
		ids[i] = fmt.Sprintf("batch-%d", i)
	}

	if err := idx.DeleteBatch(ctx, ids); err != nil {
		t.Fatalf("failed to delete batch: %v", err)
	}

	// Verify delete
	results, err = idx.Search(ctx, nodes[0].Embedding, 100, nil)
	if err != nil {
		t.Fatalf("failed to search after delete: %v", err)
	}

	if len(results) != 50 {
		t.Errorf("expected 50 results after delete, got %d", len(results))
	}
}

func TestManager(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tableName := fmt.Sprintf("test_manager_%d", os.Getpid())

	manager := pgvector.NewManager(db)

	// Create index
	cfg := vector.IndexConfig{
		Name:           tableName,
		Dimensions:     256,
		DistanceMetric: vector.DistanceCosine,
		IndexType:      vector.IndexTypeHNSW,
		HNSWConfig: &vector.HNSWConfig{
			M:              32,
			EfConstruction: 128,
		},
	}

	if err := manager.CreateIndex(ctx, cfg); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	defer func() {
		manager.DropIndex(ctx, tableName)
	}()

	// Check exists
	exists, err := manager.IndexExists(ctx, tableName)
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if !exists {
		t.Error("expected index to exist")
	}

	// Get stats
	stats, err := manager.IndexStats(ctx, tableName)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.Name != tableName {
		t.Errorf("expected name '%s', got '%s'", tableName, stats.Name)
	}

	if stats.NodeCount != 0 {
		t.Errorf("expected 0 nodes, got %d", stats.NodeCount)
	}

	// List indexes
	indexes, err := manager.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("failed to list indexes: %v", err)
	}

	found := false
	for _, idx := range indexes {
		if idx == tableName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected to find '%s' in index list", tableName)
	}

	// Drop index
	if err := manager.DropIndex(ctx, tableName); err != nil {
		t.Fatalf("failed to drop index: %v", err)
	}

	// Verify dropped
	exists, err = manager.IndexExists(ctx, tableName)
	if err != nil {
		t.Fatalf("failed to check existence after drop: %v", err)
	}
	if exists {
		t.Error("expected index to not exist after drop")
	}
}
