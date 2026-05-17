package graph_test

import (
	"context"
	"testing"

	"github.com/plexusone/omniretrieve/graph"
	"github.com/plexusone/omniretrieve/memory"
	"github.com/plexusone/omniretrieve/retrieve"
)

func setupTestGraph(t *testing.T) *memory.KnowledgeGraph {
	ctx := context.Background()
	kg := memory.NewKnowledgeGraph("test-graph")

	// Create a simple knowledge graph:
	// A (concept) --relates_to--> B (concept) --part_of--> C (document)
	//                                          --caused_by--> D (entity)

	nodes := []graph.Node{
		{ID: "A", Type: "concept", Content: "Machine Learning", Source: "test"},
		{ID: "B", Type: "concept", Content: "Neural Networks", Source: "test"},
		{ID: "C", Type: "document", Content: "Deep Learning Paper", Source: "test"},
		{ID: "D", Type: "entity", Content: "Geoffrey Hinton", Source: "test"},
	}

	edges := []graph.Edge{
		{From: "A", To: "B", Type: "relates_to", Weight: 0.9},
		{From: "B", To: "C", Type: "part_of", Weight: 0.8},
		{From: "B", To: "D", Type: "caused_by", Weight: 0.7},
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

	return kg
}

func TestGraphRetriever(t *testing.T) {
	ctx := context.Background()
	kg := setupTestGraph(t)

	retriever := graph.NewRetriever(graph.RetrieverConfig{
		Graph:           kg,
		DefaultDepth:    2,
		DefaultMaxNodes: 10,
	})

	// Retrieve starting from node A
	result, err := retriever.Retrieve(ctx, retrieve.Query{
		Entities: []retrieve.EntityHint{{ID: "A"}},
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) == 0 {
		t.Fatal("expected results, got none")
	}

	// Should find A, B, and possibly C and D
	ids := make(map[string]bool)
	for _, item := range result.Items {
		ids[item.ID] = true
	}

	if !ids["A"] {
		t.Error("expected to find node A")
	}
	if !ids["B"] {
		t.Error("expected to find node B (1 hop from A)")
	}

	// Verify mode
	if len(result.Metadata.ModesUsed) != 1 || result.Metadata.ModesUsed[0] != retrieve.ModeGraph {
		t.Errorf("expected mode graph, got %v", result.Metadata.ModesUsed)
	}
}

func TestGraphRetrieverDepthLimit(t *testing.T) {
	ctx := context.Background()
	kg := setupTestGraph(t)

	retriever := graph.NewRetriever(graph.RetrieverConfig{
		Graph:           kg,
		DefaultDepth:    1, // Only 1 hop
		DefaultMaxNodes: 10,
	})

	result, err := retriever.Retrieve(ctx, retrieve.Query{
		Entities: []retrieve.EntityHint{{ID: "A"}},
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	// Should find A and B, but not C or D (2 hops)
	ids := make(map[string]bool)
	for _, item := range result.Items {
		ids[item.ID] = true
	}

	if !ids["A"] {
		t.Error("expected to find node A")
	}
	if !ids["B"] {
		t.Error("expected to find node B")
	}
	if ids["C"] || ids["D"] {
		t.Error("did not expect to find C or D with depth=1")
	}
}

func TestGraphRetrieverEdgeTypeFilter(t *testing.T) {
	ctx := context.Background()
	kg := setupTestGraph(t)

	retriever := graph.NewRetriever(graph.RetrieverConfig{
		Graph:           kg,
		DefaultDepth:    3,
		DefaultMaxNodes: 10,
		EdgeTypes:       []string{"relates_to"}, // Only follow relates_to edges
	})

	result, err := retriever.Retrieve(ctx, retrieve.Query{
		Entities: []retrieve.EntityHint{{ID: "A"}},
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	// Should find A and B only (C and D are connected via different edge types)
	ids := make(map[string]bool)
	for _, item := range result.Items {
		ids[item.ID] = true
	}

	if !ids["A"] || !ids["B"] {
		t.Error("expected to find nodes A and B")
	}
	if ids["C"] || ids["D"] {
		t.Error("did not expect to find C or D (wrong edge types)")
	}
}

func TestGraphRetrieverEmptyStart(t *testing.T) {
	ctx := context.Background()
	kg := setupTestGraph(t)

	retriever := graph.NewRetriever(graph.RetrieverConfig{
		Graph:           kg,
		DefaultDepth:    2,
		DefaultMaxNodes: 10,
	})

	// Query with no entities and no matching filters
	result, err := retriever.Retrieve(ctx, retrieve.Query{
		Filters: map[string]string{"nonexistent": "value"},
	})
	if err != nil {
		t.Fatalf("failed to retrieve: %v", err)
	}

	if len(result.Items) != 0 {
		t.Errorf("expected 0 results, got %d", len(result.Items))
	}
}
