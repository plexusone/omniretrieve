// Package memory provides in-memory implementations for testing and development.
package memory

import (
	"context"
	"math"
	"sort"
	"sync"

	"github.com/plexusone/omniretrieve/vector"
)

// VectorIndex is an in-memory vector index using brute-force search.
type VectorIndex struct {
	mu    sync.RWMutex
	name  string
	nodes map[string]vector.Node
}

// NewVectorIndex creates a new in-memory vector index.
func NewVectorIndex(name string) *VectorIndex {
	return &VectorIndex{
		name:  name,
		nodes: make(map[string]vector.Node),
	}
}

// Search implements vector.Index.
func (idx *VectorIndex) Search(ctx context.Context, embedding []float32, k int, filters map[string]string) ([]vector.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Calculate similarity for all nodes
	type scored struct {
		node  vector.Node
		score float64
	}
	candidates := make([]scored, 0, len(idx.nodes))

	for _, node := range idx.nodes {
		// Apply filters
		if !matchesFilters(node.Metadata, filters) {
			continue
		}

		score := cosineSimilarity(embedding, node.Embedding)
		candidates = append(candidates, scored{node: node, score: score})
	}

	// Sort by score descending
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	// Return top-k
	if k > len(candidates) {
		k = len(candidates)
	}

	results := make([]vector.SearchResult, k)
	for i := 0; i < k; i++ {
		results[i] = vector.SearchResult{
			Node:  candidates[i].node,
			Score: candidates[i].score,
		}
	}

	return results, nil
}

// Insert implements vector.Index.
func (idx *VectorIndex) Insert(ctx context.Context, node vector.Node) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.nodes[node.ID] = node
	return nil
}

// Upsert implements vector.Index.
func (idx *VectorIndex) Upsert(ctx context.Context, node vector.Node) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.nodes[node.ID] = node
	return nil
}

// Delete implements vector.Index.
func (idx *VectorIndex) Delete(ctx context.Context, id string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.nodes, id)
	return nil
}

// Name implements vector.Index.
func (idx *VectorIndex) Name() string {
	return idx.name
}

// InsertBatch implements vector.BatchIndex.
func (idx *VectorIndex) InsertBatch(ctx context.Context, nodes []vector.Node) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, node := range nodes {
		idx.nodes[node.ID] = node
	}
	return nil
}

// UpsertBatch implements vector.BatchIndex.
func (idx *VectorIndex) UpsertBatch(ctx context.Context, nodes []vector.Node) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, node := range nodes {
		idx.nodes[node.ID] = node
	}
	return nil
}

// DeleteBatch implements vector.BatchIndex.
func (idx *VectorIndex) DeleteBatch(ctx context.Context, ids []string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, id := range ids {
		delete(idx.nodes, id)
	}
	return nil
}

// Count returns the number of nodes in the index.
func (idx *VectorIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.nodes)
}

// cosineSimilarity calculates the cosine similarity between two vectors.
func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

// matchesFilters checks if metadata matches all filters.
func matchesFilters(metadata, filters map[string]string) bool {
	for k, v := range filters {
		if metadata[k] != v {
			return false
		}
	}
	return true
}

// Verify interface compliance
var (
	_ vector.Index      = (*VectorIndex)(nil)
	_ vector.BatchIndex = (*VectorIndex)(nil)
)
