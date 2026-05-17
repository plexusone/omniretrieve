package rerank_test

import (
	"context"
	"testing"

	"github.com/plexusone/omniretrieve/rerank"
	"github.com/plexusone/omniretrieve/retrieve"
)

func createTestItems() []retrieve.ContextItem {
	return []retrieve.ContextItem{
		{ID: "1", Content: "Machine learning is a subset of AI", Score: 0.8, Source: "test"},
		{ID: "2", Content: "Deep learning uses neural networks", Score: 0.7, Source: "test"},
		{ID: "3", Content: "Natural language processing basics", Score: 0.6, Source: "test"},
		{ID: "4", Content: "Computer vision applications", Score: 0.5, Source: "test"},
	}
}

func TestHeuristicReranker(t *testing.T) {
	ctx := context.Background()
	items := createTestItems()

	reranker := rerank.NewHeuristic(rerank.HeuristicConfig{
		Strategy:        rerank.StrategyLinear,
		TopK:            3,
		BoostExactMatch: true,
		ExactMatchBoost: 2.0,
	})

	query := retrieve.Query{Text: "machine learning"}

	result, err := reranker.Rerank(ctx, query, items)
	if err != nil {
		t.Fatalf("failed to rerank: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("expected 3 results (topK), got %d", len(result))
	}

	// First item should be boosted (contains "machine learning")
	if result[0].ID != "1" {
		t.Errorf("expected item 1 to be first (exact match boost), got %s", result[0].ID)
	}
}

func TestHeuristicRerankerReciprocal(t *testing.T) {
	ctx := context.Background()
	items := createTestItems()

	reranker := rerank.NewHeuristic(rerank.HeuristicConfig{
		Strategy: rerank.StrategyReciprocal,
		TopK:     4,
	})

	query := retrieve.Query{Text: "neural networks"}

	result, err := reranker.Rerank(ctx, query, items)
	if err != nil {
		t.Fatalf("failed to rerank: %v", err)
	}

	// Verify all items have reranker scores
	for _, item := range result {
		if item.Provenance.RerankerScore == 0 {
			t.Errorf("expected reranker score for item %s", item.ID)
		}
	}
}

func TestHeuristicRerankerMinScore(t *testing.T) {
	ctx := context.Background()
	items := createTestItems()

	reranker := rerank.NewHeuristic(rerank.HeuristicConfig{
		Strategy: rerank.StrategyLinear,
		MinScore: 0.65, // Should filter out items with score < 0.65
	})

	query := retrieve.Query{Text: "test"}

	result, err := reranker.Rerank(ctx, query, items)
	if err != nil {
		t.Fatalf("failed to rerank: %v", err)
	}

	// Only items 1, 2 should pass (0.8, 0.7)
	if len(result) != 2 {
		t.Errorf("expected 2 results after min score filter, got %d", len(result))
	}
}

func TestRerankerChain(t *testing.T) {
	ctx := context.Background()
	items := createTestItems()

	// Create a chain of rerankers
	chain := rerank.NewChain(
		rerank.NewHeuristic(rerank.HeuristicConfig{
			Strategy:        rerank.StrategyLinear,
			BoostExactMatch: true,
			ExactMatchBoost: 1.5,
		}),
		rerank.NewHeuristic(rerank.HeuristicConfig{
			Strategy: rerank.StrategyReciprocal,
			TopK:     2,
		}),
	)

	query := retrieve.Query{Text: "machine learning"}

	result, err := chain.Rerank(ctx, query, items)
	if err != nil {
		t.Fatalf("failed to rerank with chain: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("expected 2 results after chain, got %d", len(result))
	}
}

func TestRerankerEmptyInput(t *testing.T) {
	ctx := context.Background()

	reranker := rerank.NewHeuristic(rerank.HeuristicConfig{
		Strategy: rerank.StrategyLinear,
	})

	result, err := reranker.Rerank(ctx, retrieve.Query{}, []retrieve.ContextItem{})
	if err != nil {
		t.Fatalf("failed to rerank empty input: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected 0 results for empty input, got %d", len(result))
	}
}
