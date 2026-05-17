package hybrid_test

import (
	"context"
	"testing"

	"github.com/plexusone/omniretrieve/graph"
	"github.com/plexusone/omniretrieve/hybrid"
	"github.com/plexusone/omniretrieve/memory"
	"github.com/plexusone/omniretrieve/retrieve"
	"github.com/plexusone/omniretrieve/vector"
)

func setupTestRetrievers(t *testing.T) (retrieve.Retriever, retrieve.Retriever) {
	ctx := context.Background()

	// Setup vector retriever
	idx := memory.NewVectorIndex("test-vector")
	embedder := memory.NewHashEmbedder(128)

	texts := []struct {
		id      string
		content string
	}{
		{"v1", "Machine learning algorithms"},
		{"v2", "Neural network architectures"},
		{"v3", "Deep learning frameworks"},
	}

	for _, txt := range texts {
		embedding, _ := embedder.Embed(ctx, txt.content)
		if err := idx.Insert(ctx, vector.Node{
			ID:        txt.id,
			Content:   txt.content,
			Embedding: embedding,
			Source:    "vector",
		}); err != nil {
			t.Fatalf("failed to insert node: %v", err)
		}
	}

	vectorRetriever := vector.NewRetriever(vector.RetrieverConfig{
		Index:       idx,
		Embedder:    embedder,
		DefaultTopK: 5,
	})

	// Setup graph retriever
	kg := memory.NewKnowledgeGraph("test-graph")

	nodes := []graph.Node{
		{ID: "g1", Type: "concept", Content: "Supervised learning", Source: "graph"},
		{ID: "g2", Type: "concept", Content: "Classification models", Source: "graph"},
		{ID: "v1", Type: "concept", Content: "Machine learning algorithms", Source: "graph"}, // Same ID as vector
	}

	edges := []graph.Edge{
		{From: "g1", To: "g2", Type: "includes", Weight: 0.8},
		{From: "g1", To: "v1", Type: "relates_to", Weight: 0.9},
	}

	for _, n := range nodes {
		if err := kg.AddNode(ctx, n); err != nil {
			t.Fatalf("failed to add node: %v", err)
		}
	}
	for _, e := range edges {
		if err := kg.AddEdge(ctx, e); err != nil {
			t.Fatalf("failed to add edge: %v", err)
		}
	}

	graphRetriever := graph.NewRetriever(graph.RetrieverConfig{
		Graph:           kg,
		DefaultDepth:    2,
		DefaultMaxNodes: 10,
	})

	return vectorRetriever, graphRetriever
}

func TestHybridRetrieverParallel(t *testing.T) {
	ctx := context.Background()
	vectorRetriever, graphRetriever := setupTestRetrievers(t)

	hybridRetriever := hybrid.NewRetriever(hybrid.RetrieverConfig{
		Vector:    vectorRetriever,
		Graph:     graphRetriever,
		Policy:    hybrid.PolicyParallel,
		DedupByID: true,
		Weights:   hybrid.DefaultWeights(),
	})

	result, err := hybridRetriever.Retrieve(ctx, retrieve.Query{
		Text:     "machine learning",
		Entities: []retrieve.EntityHint{{ID: "g1"}},
		TopK:     10,
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) == 0 {
		t.Fatal("expected results, got none")
	}

	// Verify hybrid mode is used
	hasHybrid := false
	for _, mode := range result.Metadata.ModesUsed {
		if mode == retrieve.ModeHybrid {
			hasHybrid = true
			break
		}
	}
	if !hasHybrid {
		t.Errorf("expected hybrid mode, got %v", result.Metadata.ModesUsed)
	}

	// Check for deduplicated item (v1 exists in both)
	v1Count := 0
	for _, item := range result.Items {
		if item.ID == "v1" {
			v1Count++
		}
	}
	if v1Count > 1 {
		t.Errorf("expected v1 to be deduplicated, found %d copies", v1Count)
	}
}

func TestHybridRetrieverVectorThenGraph(t *testing.T) {
	ctx := context.Background()
	vectorRetriever, graphRetriever := setupTestRetrievers(t)

	hybridRetriever := hybrid.NewRetriever(hybrid.RetrieverConfig{
		Vector:    vectorRetriever,
		Graph:     graphRetriever,
		Policy:    hybrid.PolicyVectorThenGraph,
		DedupByID: true,
	})

	result, err := hybridRetriever.Retrieve(ctx, retrieve.Query{
		Text: "machine learning",
		TopK: 10,
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) == 0 {
		t.Fatal("expected results, got none")
	}

	t.Logf("VectorThenGraph found %d items", len(result.Items))
}

func TestHybridRetrieverGraphThenVector(t *testing.T) {
	ctx := context.Background()
	vectorRetriever, graphRetriever := setupTestRetrievers(t)

	hybridRetriever := hybrid.NewRetriever(hybrid.RetrieverConfig{
		Vector:    vectorRetriever,
		Graph:     graphRetriever,
		Policy:    hybrid.PolicyGraphThenVector,
		DedupByID: true,
	})

	result, err := hybridRetriever.Retrieve(ctx, retrieve.Query{
		Text:     "supervised learning",
		Entities: []retrieve.EntityHint{{ID: "g1"}},
		TopK:     10,
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) == 0 {
		t.Fatal("expected results, got none")
	}

	t.Logf("GraphThenVector found %d items", len(result.Items))
}

func TestHybridRetrieverWeights(t *testing.T) {
	ctx := context.Background()
	vectorRetriever, graphRetriever := setupTestRetrievers(t)

	// Test with vector-heavy weights
	vectorHeavy := hybrid.NewRetriever(hybrid.RetrieverConfig{
		Vector:    vectorRetriever,
		Graph:     graphRetriever,
		Policy:    hybrid.PolicyParallel,
		DedupByID: true,
		Weights:   hybrid.Weights{Vector: 0.9, Graph: 0.1},
	})

	// Test with graph-heavy weights
	graphHeavy := hybrid.NewRetriever(hybrid.RetrieverConfig{
		Vector:    vectorRetriever,
		Graph:     graphRetriever,
		Policy:    hybrid.PolicyParallel,
		DedupByID: true,
		Weights:   hybrid.Weights{Vector: 0.1, Graph: 0.9},
	})

	query := retrieve.Query{
		Text:     "machine learning",
		Entities: []retrieve.EntityHint{{ID: "g1"}},
		TopK:     10,
	}

	vectorResult, _ := vectorHeavy.Retrieve(ctx, query)
	graphResult, _ := graphHeavy.Retrieve(ctx, query)

	// With different weights, the same items should have different scores
	t.Logf("Vector-heavy results: %d items", len(vectorResult.Items))
	t.Logf("Graph-heavy results: %d items", len(graphResult.Items))
}

func TestHybridRetrieverVectorOnly(t *testing.T) {
	ctx := context.Background()
	vectorRetriever, _ := setupTestRetrievers(t)

	// Hybrid with only vector
	hybridRetriever := hybrid.NewRetriever(hybrid.RetrieverConfig{
		Vector:    vectorRetriever,
		Graph:     nil, // No graph
		Policy:    hybrid.PolicyParallel,
		DedupByID: true,
	})

	result, err := hybridRetriever.Retrieve(ctx, retrieve.Query{
		Text: "neural networks",
		TopK: 5,
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) == 0 {
		t.Fatal("expected results with vector-only hybrid")
	}
}

func TestHybridRetrieverGraphOnly(t *testing.T) {
	ctx := context.Background()
	_, graphRetriever := setupTestRetrievers(t)

	// Hybrid with only graph
	hybridRetriever := hybrid.NewRetriever(hybrid.RetrieverConfig{
		Vector:    nil, // No vector
		Graph:     graphRetriever,
		Policy:    hybrid.PolicyParallel,
		DedupByID: true,
	})

	result, err := hybridRetriever.Retrieve(ctx, retrieve.Query{
		Entities: []retrieve.EntityHint{{ID: "g1"}},
		TopK:     5,
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) == 0 {
		t.Fatal("expected results with graph-only hybrid")
	}
}
