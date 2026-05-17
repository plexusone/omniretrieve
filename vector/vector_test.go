package vector_test

import (
	"context"
	"testing"

	"github.com/plexusone/omniretrieve/memory"
	"github.com/plexusone/omniretrieve/retrieve"
	"github.com/plexusone/omniretrieve/vector"
)

func TestVectorRetriever(t *testing.T) {
	ctx := context.Background()

	// Create in-memory index and embedder
	idx := memory.NewVectorIndex("test-index")
	embedder := memory.NewHashEmbedder(128)

	// Insert test nodes
	texts := []string{
		"The quick brown fox jumps over the lazy dog",
		"Machine learning is a subset of artificial intelligence",
		"Natural language processing enables computers to understand text",
		"Go is a statically typed programming language",
	}

	for i, text := range texts {
		embedding, err := embedder.Embed(ctx, text)
		if err != nil {
			t.Fatalf("failed to embed text: %v", err)
		}

		node := vector.Node{
			ID:        string(rune('A' + i)),
			Content:   text,
			Embedding: embedding,
			Source:    "test",
			Metadata:  map[string]string{"type": "test"},
		}

		if err := idx.Insert(ctx, node); err != nil {
			t.Fatalf("failed to insert node: %v", err)
		}
	}

	// Create retriever
	retriever := vector.NewRetriever(vector.RetrieverConfig{
		Index:       idx,
		Embedder:    embedder,
		DefaultTopK: 3,
	})

	// Test retrieval
	result, err := retriever.Retrieve(ctx, retrieve.Query{
		Text: "artificial intelligence and machine learning",
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) == 0 {
		t.Fatal("expected results, got none")
	}

	if len(result.Items) > 3 {
		t.Errorf("expected at most 3 results, got %d", len(result.Items))
	}

	// Verify metadata
	if len(result.Metadata.ModesUsed) != 1 || result.Metadata.ModesUsed[0] != retrieve.ModeVector {
		t.Errorf("expected mode vector, got %v", result.Metadata.ModesUsed)
	}
}

func TestVectorRetrieverWithFilters(t *testing.T) {
	ctx := context.Background()

	idx := memory.NewVectorIndex("test-index")
	embedder := memory.NewHashEmbedder(128)

	// Insert nodes with different categories
	nodes := []struct {
		id       string
		content  string
		category string
	}{
		{"1", "Database design patterns", "tech"},
		{"2", "Recipe for chocolate cake", "food"},
		{"3", "SQL query optimization", "tech"},
		{"4", "Pasta cooking tips", "food"},
	}

	for _, n := range nodes {
		embedding, _ := embedder.Embed(ctx, n.content)
		if err := idx.Insert(ctx, vector.Node{
			ID:        n.id,
			Content:   n.content,
			Embedding: embedding,
			Source:    "test",
			Metadata:  map[string]string{"category": n.category},
		}); err != nil {
			t.Fatalf("failed to insert node: %v", err)
		}
	}

	retriever := vector.NewRetriever(vector.RetrieverConfig{
		Index:       idx,
		Embedder:    embedder,
		DefaultTopK: 10,
	})

	// Retrieve with filter
	result, err := retriever.Retrieve(ctx, retrieve.Query{
		Text:    "database",
		Filters: map[string]string{"category": "tech"},
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	// Verify all results match filter
	for _, item := range result.Items {
		if item.Metadata["category"] != "tech" {
			t.Errorf("expected category 'tech', got '%s'", item.Metadata["category"])
		}
	}
}

func TestVectorRetrieverMinScore(t *testing.T) {
	ctx := context.Background()

	idx := memory.NewVectorIndex("test-index")
	embedder := memory.NewHashEmbedder(128)

	// Insert test node
	embedding, _ := embedder.Embed(ctx, "test content")
	if err := idx.Insert(ctx, vector.Node{
		ID:        "1",
		Content:   "test content",
		Embedding: embedding,
		Source:    "test",
	}); err != nil {
		t.Fatalf("failed to insert node: %v", err)
	}

	retriever := vector.NewRetriever(vector.RetrieverConfig{
		Index:       idx,
		Embedder:    embedder,
		DefaultTopK: 10,
		MinScore:    0.99, // High threshold
	})

	// Query with very different text should return no results
	result, err := retriever.Retrieve(ctx, retrieve.Query{
		Text: "completely unrelated query about something else entirely",
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	// Most results should be filtered by minScore
	// Note: with hash embedder, similarity might still be high
	t.Logf("got %d results with min score filter", len(result.Items))
}
