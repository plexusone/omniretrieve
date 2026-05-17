package memory

import (
	"context"
	"sync"

	"github.com/plexusone/omniretrieve/graph"
)

// KnowledgeGraph is an in-memory knowledge graph.
type KnowledgeGraph struct {
	mu    sync.RWMutex
	name  string
	nodes map[string]graph.Node
	edges map[string][]graph.Edge // From node ID -> edges
}

// NewKnowledgeGraph creates a new in-memory knowledge graph.
func NewKnowledgeGraph(name string) *KnowledgeGraph {
	return &KnowledgeGraph{
		name:  name,
		nodes: make(map[string]graph.Node),
		edges: make(map[string][]graph.Edge),
	}
}

// Traverse implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) Traverse(ctx context.Context, startNodes []string, opts graph.TraversalOptions) (*graph.TraversalResult, error) {
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	visited := make(map[string]bool)
	paths := make(map[string][]string)
	var resultNodes []graph.Node
	var resultEdges []graph.Edge

	// BFS traversal
	type queueItem struct {
		nodeID string
		path   []string
		depth  int
	}

	queue := make([]queueItem, 0, len(startNodes))
	for _, id := range startNodes {
		if _, ok := kg.nodes[id]; ok {
			queue = append(queue, queueItem{nodeID: id, path: []string{id}, depth: 0})
		}
	}

	for len(queue) > 0 && len(resultNodes) < opts.MaxNodes {
		current := queue[0]
		queue = queue[1:]

		if visited[current.nodeID] {
			continue
		}
		visited[current.nodeID] = true

		// Add node to results
		if node, ok := kg.nodes[current.nodeID]; ok {
			// Apply node type filter
			if len(opts.NodeTypes) > 0 && !containsString(opts.NodeTypes, node.Type) {
				continue
			}
			resultNodes = append(resultNodes, node)
			paths[current.nodeID] = current.path
		}

		// Stop if max depth reached
		if current.depth >= opts.Depth {
			continue
		}

		// Traverse edges
		for _, edge := range kg.edges[current.nodeID] {
			// Apply edge type filter
			if len(opts.EdgeTypes) > 0 && !containsString(opts.EdgeTypes, edge.Type) {
				continue
			}

			// Apply min weight filter
			if edge.Weight < opts.MinWeight {
				continue
			}

			if !visited[edge.To] {
				newPath := make([]string, len(current.path)+1)
				copy(newPath, current.path)
				newPath[len(current.path)] = edge.To

				queue = append(queue, queueItem{
					nodeID: edge.To,
					path:   newPath,
					depth:  current.depth + 1,
				})
				resultEdges = append(resultEdges, edge)
			}
		}
	}

	return &graph.TraversalResult{
		Nodes: resultNodes,
		Edges: resultEdges,
		Paths: paths,
	}, nil
}

// FindNodes implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) FindNodes(ctx context.Context, nodeType string, filters map[string]string) ([]graph.Node, error) {
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	var result []graph.Node
	for _, node := range kg.nodes {
		// Filter by type
		if nodeType != "" && node.Type != nodeType {
			continue
		}

		// Filter by metadata
		if !matchesFilters(node.Metadata, filters) {
			continue
		}

		result = append(result, node)
	}

	return result, nil
}

// AddNode implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) AddNode(ctx context.Context, node graph.Node) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	kg.nodes[node.ID] = node
	return nil
}

// UpsertNode implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) UpsertNode(ctx context.Context, node graph.Node) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	kg.nodes[node.ID] = node
	return nil
}

// AddEdge implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) AddEdge(ctx context.Context, edge graph.Edge) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	kg.edges[edge.From] = append(kg.edges[edge.From], edge)
	return nil
}

// UpsertEdge implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) UpsertEdge(ctx context.Context, edge graph.Edge) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	// Remove existing edge if present
	edges := kg.edges[edge.From]
	filtered := make([]graph.Edge, 0, len(edges))
	for _, e := range edges {
		if e.To != edge.To || e.Type != edge.Type {
			filtered = append(filtered, e)
		}
	}
	kg.edges[edge.From] = append(filtered, edge)
	return nil
}

// DeleteNode implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) DeleteNode(ctx context.Context, id string) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	delete(kg.nodes, id)
	delete(kg.edges, id)

	// Remove edges pointing to this node
	for from, edges := range kg.edges {
		filtered := make([]graph.Edge, 0, len(edges))
		for _, e := range edges {
			if e.To != id {
				filtered = append(filtered, e)
			}
		}
		kg.edges[from] = filtered
	}

	return nil
}

// DeleteEdge implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) DeleteEdge(ctx context.Context, from, to, edgeType string) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	edges := kg.edges[from]
	filtered := make([]graph.Edge, 0, len(edges))
	for _, e := range edges {
		if e.To != to || e.Type != edgeType {
			filtered = append(filtered, e)
		}
	}
	kg.edges[from] = filtered
	return nil
}

// Name implements graph.KnowledgeGraph.
func (kg *KnowledgeGraph) Name() string {
	return kg.name
}

// AddNodeBatch implements graph.BatchKnowledgeGraph.
func (kg *KnowledgeGraph) AddNodeBatch(ctx context.Context, nodes []graph.Node) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	for _, node := range nodes {
		kg.nodes[node.ID] = node
	}
	return nil
}

// UpsertNodeBatch implements graph.BatchKnowledgeGraph.
func (kg *KnowledgeGraph) UpsertNodeBatch(ctx context.Context, nodes []graph.Node) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	for _, node := range nodes {
		kg.nodes[node.ID] = node
	}
	return nil
}

// AddEdgeBatch implements graph.BatchKnowledgeGraph.
func (kg *KnowledgeGraph) AddEdgeBatch(ctx context.Context, edges []graph.Edge) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	for _, edge := range edges {
		kg.edges[edge.From] = append(kg.edges[edge.From], edge)
	}
	return nil
}

// UpsertEdgeBatch implements graph.BatchKnowledgeGraph.
func (kg *KnowledgeGraph) UpsertEdgeBatch(ctx context.Context, edges []graph.Edge) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	for _, edge := range edges {
		// Remove existing edge if present
		existingEdges := kg.edges[edge.From]
		filtered := make([]graph.Edge, 0, len(existingEdges))
		for _, e := range existingEdges {
			if e.To != edge.To || e.Type != edge.Type {
				filtered = append(filtered, e)
			}
		}
		kg.edges[edge.From] = append(filtered, edge)
	}
	return nil
}

// DeleteNodeBatch implements graph.BatchKnowledgeGraph.
func (kg *KnowledgeGraph) DeleteNodeBatch(ctx context.Context, ids []string) error {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	for _, id := range ids {
		delete(kg.nodes, id)
		delete(kg.edges, id)
	}

	// Remove edges pointing to deleted nodes
	idSet := make(map[string]bool)
	for _, id := range ids {
		idSet[id] = true
	}

	for from, edges := range kg.edges {
		filtered := make([]graph.Edge, 0, len(edges))
		for _, e := range edges {
			if !idSet[e.To] {
				filtered = append(filtered, e)
			}
		}
		kg.edges[from] = filtered
	}

	return nil
}

// NodeCount returns the number of nodes in the graph.
func (kg *KnowledgeGraph) NodeCount() int {
	kg.mu.RLock()
	defer kg.mu.RUnlock()
	return len(kg.nodes)
}

// EdgeCount returns the number of edges in the graph.
func (kg *KnowledgeGraph) EdgeCount() int {
	kg.mu.RLock()
	defer kg.mu.RUnlock()
	count := 0
	for _, edges := range kg.edges {
		count += len(edges)
	}
	return count
}

// containsString checks if a slice contains a string.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// Verify interface compliance
var (
	_ graph.KnowledgeGraph      = (*KnowledgeGraph)(nil)
	_ graph.BatchKnowledgeGraph = (*KnowledgeGraph)(nil)
)
