// Package hybrid provides hybrid retrieval combining vector and graph strategies.
package hybrid

import (
	"context"
	"sort"
	"time"

	"github.com/plexusone/omniretrieve/retrieve"
)

// Policy defines how to combine vector and graph retrieval.
type Policy string

const (
	// PolicyParallel runs vector and graph retrieval in parallel and merges results.
	PolicyParallel Policy = "parallel"
	// PolicyVectorThenGraph runs vector search first, then expands via graph.
	PolicyVectorThenGraph Policy = "vector_then_graph"
	// PolicyGraphThenVector runs graph traversal first, then grounds via vector.
	PolicyGraphThenVector Policy = "graph_then_vector"
)

// Weights configures the relative importance of vector vs graph results.
type Weights struct {
	// Vector weight (0.0-1.0).
	Vector float64
	// Graph weight (0.0-1.0).
	Graph float64
}

// DefaultWeights returns balanced weights.
func DefaultWeights() Weights {
	return Weights{Vector: 0.6, Graph: 0.4}
}

// RetrieverConfig configures the hybrid retriever.
type RetrieverConfig struct {
	// Vector is the vector retriever.
	Vector retrieve.Retriever
	// Graph is the graph retriever.
	Graph retrieve.Retriever
	// Policy defines how to combine results.
	Policy Policy
	// Weights for combining scores.
	Weights Weights
	// Reranker to apply after merging (optional).
	Reranker retrieve.Reranker
	// DedupByID removes duplicate items by ID.
	DedupByID bool
	// Observer for tracing and metrics.
	Observer retrieve.Observer
}

// Retriever implements hybrid vector+graph retrieval.
type Retriever struct {
	config RetrieverConfig
}

// NewRetriever creates a new hybrid retriever.
func NewRetriever(cfg RetrieverConfig) *Retriever {
	if cfg.Policy == "" {
		cfg.Policy = PolicyParallel
	}
	if cfg.Weights.Vector == 0 && cfg.Weights.Graph == 0 {
		cfg.Weights = DefaultWeights()
	}
	return &Retriever{config: cfg}
}

// Retrieve performs hybrid retrieval based on the configured policy.
func (r *Retriever) Retrieve(ctx context.Context, q retrieve.Query) (*retrieve.Result, error) {
	start := time.Now()

	var items []retrieve.ContextItem
	var modesUsed []retrieve.Mode
	var totalCandidates int
	var err error

	switch r.config.Policy {
	case PolicyParallel:
		items, modesUsed, totalCandidates, err = r.retrieveParallel(ctx, q)
	case PolicyVectorThenGraph:
		items, modesUsed, totalCandidates, err = r.retrieveVectorThenGraph(ctx, q)
	case PolicyGraphThenVector:
		items, modesUsed, totalCandidates, err = r.retrieveGraphThenVector(ctx, q)
	default:
		items, modesUsed, totalCandidates, err = r.retrieveParallel(ctx, q)
	}

	if err != nil {
		return nil, err
	}

	// Deduplicate if configured
	if r.config.DedupByID {
		items = deduplicate(items)
	}

	// Sort by score
	sort.Slice(items, func(i, j int) bool {
		return items[i].Score > items[j].Score
	})

	// Apply top-k limit
	if q.TopK > 0 && len(items) > q.TopK {
		items = items[:q.TopK]
	}

	// Apply reranker if configured
	if r.config.Reranker != nil {
		rerankStart := time.Now()
		items, err = r.config.Reranker.Rerank(ctx, q, items)
		if err != nil {
			return nil, err
		}
		if r.config.Observer != nil {
			r.config.Observer.OnRerank(ctx, "hybrid", len(items), len(items), time.Since(rerankStart).Milliseconds())
		}
	}

	return &retrieve.Result{
		Items: items,
		Query: q,
		Metadata: retrieve.ResultMetadata{
			TotalCandidates: totalCandidates,
			LatencyMS:       time.Since(start).Milliseconds(),
			ModesUsed:       modesUsed,
		},
	}, nil
}

// retrieveParallel runs vector and graph retrieval concurrently.
func (r *Retriever) retrieveParallel(ctx context.Context, q retrieve.Query) ([]retrieve.ContextItem, []retrieve.Mode, int, error) {
	type result struct {
		items []retrieve.ContextItem
		count int
		err   error
	}

	vectorCh := make(chan result, 1)
	graphCh := make(chan result, 1)

	// Run vector retrieval
	go func() {
		if r.config.Vector == nil {
			vectorCh <- result{}
			return
		}
		res, err := r.config.Vector.Retrieve(ctx, q)
		if err != nil {
			vectorCh <- result{err: err}
			return
		}
		vectorCh <- result{items: res.Items, count: res.Metadata.TotalCandidates}
	}()

	// Run graph retrieval
	go func() {
		if r.config.Graph == nil {
			graphCh <- result{}
			return
		}
		res, err := r.config.Graph.Retrieve(ctx, q)
		if err != nil {
			graphCh <- result{err: err}
			return
		}
		graphCh <- result{items: res.Items, count: res.Metadata.TotalCandidates}
	}()

	// Collect results
	vectorRes := <-vectorCh
	graphRes := <-graphCh

	if vectorRes.err != nil {
		return nil, nil, 0, vectorRes.err
	}
	if graphRes.err != nil {
		return nil, nil, 0, graphRes.err
	}

	// Merge and weight results
	items := r.mergeResults(vectorRes.items, graphRes.items)
	modesUsed := []retrieve.Mode{retrieve.ModeHybrid}
	if len(vectorRes.items) > 0 {
		modesUsed = append(modesUsed, retrieve.ModeVector)
	}
	if len(graphRes.items) > 0 {
		modesUsed = append(modesUsed, retrieve.ModeGraph)
	}

	return items, modesUsed, vectorRes.count + graphRes.count, nil
}

// retrieveVectorThenGraph runs vector search, then expands results via graph.
func (r *Retriever) retrieveVectorThenGraph(ctx context.Context, q retrieve.Query) ([]retrieve.ContextItem, []retrieve.Mode, int, error) {
	modesUsed := []retrieve.Mode{retrieve.ModeHybrid}
	var totalCandidates int

	// First: vector search
	var vectorItems []retrieve.ContextItem
	if r.config.Vector != nil {
		res, err := r.config.Vector.Retrieve(ctx, q)
		if err != nil {
			return nil, nil, 0, err
		}
		vectorItems = res.Items
		totalCandidates += res.Metadata.TotalCandidates
		modesUsed = append(modesUsed, retrieve.ModeVector)
	}

	// Extract entity hints from vector results for graph expansion
	var graphItems []retrieve.ContextItem
	if r.config.Graph != nil && len(vectorItems) > 0 {
		// Use vector results as starting points for graph expansion
		entities := make([]retrieve.EntityHint, 0, len(vectorItems))
		for _, item := range vectorItems {
			entities = append(entities, retrieve.EntityHint{
				ID:   item.ID,
				Name: item.ID,
			})
		}

		graphQuery := q
		graphQuery.Entities = entities

		res, err := r.config.Graph.Retrieve(ctx, graphQuery)
		if err != nil {
			return nil, nil, 0, err
		}
		graphItems = res.Items
		totalCandidates += res.Metadata.TotalCandidates
		modesUsed = append(modesUsed, retrieve.ModeGraph)
	}

	items := r.mergeResults(vectorItems, graphItems)
	return items, modesUsed, totalCandidates, nil
}

// retrieveGraphThenVector runs graph traversal, then grounds via vector search.
func (r *Retriever) retrieveGraphThenVector(ctx context.Context, q retrieve.Query) ([]retrieve.ContextItem, []retrieve.Mode, int, error) {
	modesUsed := []retrieve.Mode{retrieve.ModeHybrid}
	var totalCandidates int

	// First: graph traversal
	var graphItems []retrieve.ContextItem
	if r.config.Graph != nil {
		res, err := r.config.Graph.Retrieve(ctx, q)
		if err != nil {
			return nil, nil, 0, err
		}
		graphItems = res.Items
		totalCandidates += res.Metadata.TotalCandidates
		modesUsed = append(modesUsed, retrieve.ModeGraph)
	}

	// Use graph results to inform vector search
	var vectorItems []retrieve.ContextItem
	if r.config.Vector != nil {
		res, err := r.config.Vector.Retrieve(ctx, q)
		if err != nil {
			return nil, nil, 0, err
		}
		vectorItems = res.Items
		totalCandidates += res.Metadata.TotalCandidates
		modesUsed = append(modesUsed, retrieve.ModeVector)
	}

	items := r.mergeResults(vectorItems, graphItems)
	return items, modesUsed, totalCandidates, nil
}

// mergeResults combines vector and graph results with weighted scoring.
func (r *Retriever) mergeResults(vectorItems, graphItems []retrieve.ContextItem) []retrieve.ContextItem {
	// Create a map for merging by ID
	merged := make(map[string]*retrieve.ContextItem)

	// Add vector items with weighted score
	for _, item := range vectorItems {
		weightedScore := item.Score * r.config.Weights.Vector
		if existing, ok := merged[item.ID]; ok {
			existing.Score += weightedScore
		} else {
			itemCopy := item
			itemCopy.Score = weightedScore
			merged[item.ID] = &itemCopy
		}
	}

	// Add graph items with weighted score
	for _, item := range graphItems {
		weightedScore := item.Score * r.config.Weights.Graph
		if existing, ok := merged[item.ID]; ok {
			existing.Score += weightedScore
			// Preserve graph path if this item came from graph
			if len(item.Provenance.GraphPath) > 0 {
				existing.Provenance.GraphPath = item.Provenance.GraphPath
			}
		} else {
			itemCopy := item
			itemCopy.Score = weightedScore
			merged[item.ID] = &itemCopy
		}
	}

	// Convert to slice
	result := make([]retrieve.ContextItem, 0, len(merged))
	for _, item := range merged {
		item.Provenance.Mode = retrieve.ModeHybrid
		result = append(result, *item)
	}

	return result
}

// deduplicate removes duplicate items by ID, keeping the highest scoring one.
func deduplicate(items []retrieve.ContextItem) []retrieve.ContextItem {
	seen := make(map[string]int) // ID -> index of best item
	result := make([]retrieve.ContextItem, 0, len(items))

	for _, item := range items {
		if idx, ok := seen[item.ID]; ok {
			// Keep the one with higher score
			if item.Score > result[idx].Score {
				result[idx] = item
			}
		} else {
			seen[item.ID] = len(result)
			result = append(result, item)
		}
	}

	return result
}
