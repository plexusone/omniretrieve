// Package rerank provides reranking implementations for retrieval results.
package rerank

import (
	"context"
	"sort"
	"strings"

	"github.com/plexusone/omniretrieve/retrieve"
)

// CrossEncoderScorer scores query-document pairs using a cross-encoder model.
type CrossEncoderScorer interface {
	// Score returns relevance scores for query-document pairs.
	Score(ctx context.Context, query string, documents []string) ([]float64, error)
	// Model returns the model name.
	Model() string
}

// CrossEncoderConfig configures the cross-encoder reranker.
type CrossEncoderConfig struct {
	// Scorer is the cross-encoder model to use.
	Scorer CrossEncoderScorer
	// TopK limits output to top K results after reranking.
	TopK int
	// MinScore filters results below this threshold.
	MinScore float64
}

// CrossEncoder implements reranking using a cross-encoder model.
type CrossEncoder struct {
	config CrossEncoderConfig
}

// NewCrossEncoder creates a new cross-encoder reranker.
func NewCrossEncoder(cfg CrossEncoderConfig) *CrossEncoder {
	return &CrossEncoder{config: cfg}
}

// Rerank implements retrieve.Reranker.
func (r *CrossEncoder) Rerank(ctx context.Context, q retrieve.Query, items []retrieve.ContextItem) ([]retrieve.ContextItem, error) {
	if len(items) == 0 {
		return items, nil
	}

	// Extract documents
	documents := make([]string, len(items))
	for i, item := range items {
		documents[i] = item.Content
	}

	// Score with cross-encoder
	scores, err := r.config.Scorer.Score(ctx, q.Text, documents)
	if err != nil {
		return nil, err
	}

	// Apply scores and filter
	result := make([]retrieve.ContextItem, 0, len(items))
	for i, item := range items {
		if i < len(scores) {
			item.Provenance.RerankerScore = scores[i]
			item.Score = scores[i] // Replace original score
		}
		if item.Score >= r.config.MinScore {
			result = append(result, item)
		}
	}

	// Sort by score descending
	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score
	})

	// Apply top-k
	if r.config.TopK > 0 && len(result) > r.config.TopK {
		result = result[:r.config.TopK]
	}

	return result, nil
}

// Strategy defines a reranking strategy.
type Strategy string

const (
	// StrategyReciprocal uses reciprocal rank fusion.
	StrategyReciprocal Strategy = "reciprocal"
	// StrategyLinear uses linear score combination.
	StrategyLinear Strategy = "linear"
	// StrategyMax takes the maximum score.
	StrategyMax Strategy = "max"
)

// HeuristicConfig configures heuristic reranking.
type HeuristicConfig struct {
	// Strategy is the scoring strategy.
	Strategy Strategy
	// Weights for different signals (e.g., "similarity", "recency", "popularity").
	Weights map[string]float64
	// TopK limits output.
	TopK int
	// MinScore threshold.
	MinScore float64
	// BoostExactMatch boosts items containing exact query matches.
	BoostExactMatch bool
	// ExactMatchBoost is the boost factor for exact matches.
	ExactMatchBoost float64
}

// Heuristic implements heuristic-based reranking.
type Heuristic struct {
	config HeuristicConfig
}

// NewHeuristic creates a new heuristic reranker.
func NewHeuristic(cfg HeuristicConfig) *Heuristic {
	if cfg.Strategy == "" {
		cfg.Strategy = StrategyLinear
	}
	if cfg.ExactMatchBoost == 0 {
		cfg.ExactMatchBoost = 1.5
	}
	return &Heuristic{config: cfg}
}

// Rerank implements retrieve.Reranker.
func (r *Heuristic) Rerank(ctx context.Context, q retrieve.Query, items []retrieve.ContextItem) ([]retrieve.ContextItem, error) {
	if len(items) == 0 {
		return items, nil
	}

	result := make([]retrieve.ContextItem, len(items))
	copy(result, items)

	// Apply scoring strategy
	for i := range result {
		var score float64

		switch r.config.Strategy {
		case StrategyReciprocal:
			// Reciprocal rank fusion
			score = 1.0 / (float64(i) + 60.0) // k=60 is common
			score += result[i].Score * 0.5
		case StrategyMax:
			score = result[i].Score
		default: // Linear
			score = result[i].Score
		}

		// Apply exact match boost
		if r.config.BoostExactMatch {
			if containsExactMatch(result[i].Content, q.Text) {
				score *= r.config.ExactMatchBoost
			}
		}

		result[i].Score = score
		result[i].Provenance.RerankerScore = score
	}

	// Sort by score descending
	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score
	})

	// Filter by min score
	if r.config.MinScore > 0 {
		filtered := make([]retrieve.ContextItem, 0, len(result))
		for _, item := range result {
			if item.Score >= r.config.MinScore {
				filtered = append(filtered, item)
			}
		}
		result = filtered
	}

	// Apply top-k
	if r.config.TopK > 0 && len(result) > r.config.TopK {
		result = result[:r.config.TopK]
	}

	return result, nil
}

// containsExactMatch checks if content contains an exact query match.
func containsExactMatch(content, query string) bool {
	return strings.Contains(
		strings.ToLower(content),
		strings.ToLower(query),
	)
}

// Chain chains multiple rerankers together.
type Chain struct {
	rerankers []retrieve.Reranker
}

// NewChain creates a new reranker chain.
func NewChain(rerankers ...retrieve.Reranker) *Chain {
	return &Chain{rerankers: rerankers}
}

// Rerank implements retrieve.Reranker.
func (c *Chain) Rerank(ctx context.Context, q retrieve.Query, items []retrieve.ContextItem) ([]retrieve.ContextItem, error) {
	var err error
	for _, r := range c.rerankers {
		items, err = r.Rerank(ctx, q, items)
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

// Verify interface compliance
var _ retrieve.Reranker = (*CrossEncoder)(nil)
var _ retrieve.Reranker = (*Heuristic)(nil)
var _ retrieve.Reranker = (*Chain)(nil)
