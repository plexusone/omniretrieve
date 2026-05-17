// Package graph provides knowledge graph traversal for retrieval.
package graph

import (
	"context"
	"time"

	"github.com/plexusone/omniretrieve/retrieve"
)

// Node represents a node in the knowledge graph.
type Node struct {
	// ID is the unique identifier for this node.
	ID string
	// Type is the node type (e.g., "concept", "document", "entity").
	Type string
	// Content is the text content of this node.
	Content string
	// Source identifies where this node came from.
	Source string
	// Metadata contains additional node metadata.
	Metadata map[string]string
}

// Edge represents an edge in the knowledge graph.
type Edge struct {
	// From is the source node ID.
	From string
	// To is the target node ID.
	To string
	// Type is the edge type (e.g., "relates_to", "part_of", "caused_by").
	Type string
	// Weight is the edge weight (0.0-1.0).
	Weight float64
	// Metadata contains additional edge metadata.
	Metadata map[string]string
}

// TraversalResult represents the result of a graph traversal.
type TraversalResult struct {
	// Nodes are the nodes found during traversal.
	Nodes []Node
	// Edges are the edges traversed.
	Edges []Edge
	// Paths maps node IDs to their traversal paths.
	Paths map[string][]string
}

// TraversalOptions configures graph traversal.
type TraversalOptions struct {
	// Depth is the maximum traversal depth.
	Depth int
	// EdgeTypes filters which edge types to traverse.
	EdgeTypes []string
	// NodeTypes filters which node types to include.
	NodeTypes []string
	// MaxNodes limits the total number of nodes to return.
	MaxNodes int
	// MinWeight is the minimum edge weight to traverse.
	MinWeight float64
}

// KnowledgeGraph defines the interface for knowledge graph operations.
type KnowledgeGraph interface {
	// Traverse performs a graph traversal starting from the given nodes.
	Traverse(ctx context.Context, startNodes []string, opts TraversalOptions) (*TraversalResult, error)
	// FindNodes finds nodes matching the given criteria.
	FindNodes(ctx context.Context, nodeType string, filters map[string]string) ([]Node, error)
	// AddNode adds a node to the graph.
	AddNode(ctx context.Context, node Node) error
	// UpsertNode inserts or updates a node in the graph.
	UpsertNode(ctx context.Context, node Node) error
	// AddEdge adds an edge to the graph.
	AddEdge(ctx context.Context, edge Edge) error
	// UpsertEdge inserts or updates an edge in the graph.
	UpsertEdge(ctx context.Context, edge Edge) error
	// DeleteNode removes a node and its edges from the graph.
	DeleteNode(ctx context.Context, id string) error
	// DeleteEdge removes an edge from the graph.
	DeleteEdge(ctx context.Context, from, to, edgeType string) error
	// Name returns the name/identifier of this graph.
	Name() string
}

// BatchKnowledgeGraph extends KnowledgeGraph with batch operations.
type BatchKnowledgeGraph interface {
	KnowledgeGraph
	// AddNodeBatch adds multiple nodes to the graph.
	AddNodeBatch(ctx context.Context, nodes []Node) error
	// UpsertNodeBatch inserts or updates multiple nodes.
	UpsertNodeBatch(ctx context.Context, nodes []Node) error
	// AddEdgeBatch adds multiple edges to the graph.
	AddEdgeBatch(ctx context.Context, edges []Edge) error
	// UpsertEdgeBatch inserts or updates multiple edges.
	UpsertEdgeBatch(ctx context.Context, edges []Edge) error
	// DeleteNodeBatch removes multiple nodes and their edges.
	DeleteNodeBatch(ctx context.Context, ids []string) error
}

// GraphConfig configures a knowledge graph.
type GraphConfig struct {
	// Name is the graph name.
	Name string
	// NodeTypes defines the allowed node types (empty means any).
	NodeTypes []string
	// EdgeTypes defines the allowed edge types (empty means any).
	EdgeTypes []string
	// Directed indicates if the graph is directed.
	Directed bool
}

// GraphStats contains graph statistics.
type GraphStats struct {
	// Name is the graph name.
	Name string
	// NodeCount is the number of nodes.
	NodeCount int64
	// EdgeCount is the number of edges.
	EdgeCount int64
	// NodeTypeStats maps node types to counts.
	NodeTypeStats map[string]int64
	// EdgeTypeStats maps edge types to counts.
	EdgeTypeStats map[string]int64
}

// GraphManager provides graph lifecycle operations.
type GraphManager interface {
	// CreateGraph creates a new knowledge graph.
	CreateGraph(ctx context.Context, cfg GraphConfig) error
	// DropGraph removes a graph.
	DropGraph(ctx context.Context, name string) error
	// GraphExists checks if a graph exists.
	GraphExists(ctx context.Context, name string) (bool, error)
	// GraphStats returns statistics for a graph.
	GraphStats(ctx context.Context, name string) (*GraphStats, error)
	// ListGraphs returns all graph names.
	ListGraphs(ctx context.Context) ([]string, error)
}

// RetrieverConfig configures the graph retriever.
type RetrieverConfig struct {
	// Graph is the knowledge graph to traverse.
	Graph KnowledgeGraph
	// DefaultDepth is the default traversal depth.
	DefaultDepth int
	// DefaultMaxNodes is the default maximum nodes to return.
	DefaultMaxNodes int
	// EdgeTypes filters which edge types to traverse by default.
	EdgeTypes []string
	// Observer for tracing and metrics.
	Observer retrieve.Observer
}

// Retriever implements graph-based retrieval.
type Retriever struct {
	config RetrieverConfig
}

// NewRetriever creates a new graph retriever.
func NewRetriever(cfg RetrieverConfig) *Retriever {
	if cfg.DefaultDepth == 0 {
		cfg.DefaultDepth = 2
	}
	if cfg.DefaultMaxNodes == 0 {
		cfg.DefaultMaxNodes = 20
	}
	return &Retriever{config: cfg}
}

// Retrieve performs graph traversal to find relevant context.
func (r *Retriever) Retrieve(ctx context.Context, q retrieve.Query) (*retrieve.Result, error) {
	start := time.Now()

	// Determine start nodes from entity hints
	startNodes := make([]string, 0, len(q.Entities))
	for _, e := range q.Entities {
		if e.ID != "" {
			startNodes = append(startNodes, e.ID)
		}
	}

	// If no start nodes, try to find matching nodes
	if len(startNodes) == 0 {
		// Try to find nodes matching query text or metadata
		nodes, err := r.config.Graph.FindNodes(ctx, "", q.Filters)
		if err != nil {
			return nil, err
		}
		for _, n := range nodes {
			startNodes = append(startNodes, n.ID)
		}
	}

	// If still no start nodes, return empty result
	if len(startNodes) == 0 {
		return &retrieve.Result{
			Items: []retrieve.ContextItem{},
			Query: q,
			Metadata: retrieve.ResultMetadata{
				ModesUsed: []retrieve.Mode{retrieve.ModeGraph},
			},
		}, nil
	}

	// Configure traversal
	depth := q.MaxDepth
	if depth == 0 {
		depth = r.config.DefaultDepth
	}

	maxNodes := q.TopK
	if maxNodes == 0 {
		maxNodes = r.config.DefaultMaxNodes
	}

	opts := TraversalOptions{
		Depth:     depth,
		EdgeTypes: r.config.EdgeTypes,
		MaxNodes:  maxNodes,
		MinWeight: q.MinScore,
	}

	// Perform traversal
	result, err := r.config.Graph.Traverse(ctx, startNodes, opts)
	if err != nil {
		return nil, err
	}

	// Convert to context items with path information
	items := make([]retrieve.ContextItem, 0, len(result.Nodes))
	for _, node := range result.Nodes {
		path := result.Paths[node.ID]
		score := computePathScore(path, result.Edges)

		if score < q.MinScore && q.MinScore > 0 {
			continue
		}

		items = append(items, retrieve.ContextItem{
			ID:       node.ID,
			Content:  node.Content,
			Source:   node.Source,
			Score:    score,
			Metadata: node.Metadata,
			Provenance: retrieve.Provenance{
				Mode:      retrieve.ModeGraph,
				Backend:   r.config.Graph.Name(),
				GraphPath: path,
			},
		})
	}

	latency := time.Since(start).Milliseconds()

	// Report to observer
	if r.config.Observer != nil {
		r.config.Observer.OnGraphTraverse(ctx, r.config.Graph.Name(), depth, len(items), latency)
	}

	return &retrieve.Result{
		Items: items,
		Query: q,
		Metadata: retrieve.ResultMetadata{
			TotalCandidates: len(result.Nodes),
			LatencyMS:       latency,
			ModesUsed:       []retrieve.Mode{retrieve.ModeGraph},
		},
	}, nil
}

// computePathScore calculates a relevance score based on path length and edge weights.
func computePathScore(path []string, edges []Edge) float64 {
	if len(path) == 0 {
		return 1.0 // Start nodes have max score
	}

	// Build edge lookup
	edgeWeights := make(map[string]float64)
	for _, e := range edges {
		key := e.From + "->" + e.To
		edgeWeights[key] = e.Weight
	}

	// Calculate cumulative score with decay
	score := 1.0
	decayFactor := 0.8 // Score decays by 20% per hop

	for i := 0; i < len(path)-1; i++ {
		key := path[i] + "->" + path[i+1]
		weight := edgeWeights[key]
		if weight == 0 {
			weight = 0.5 // Default weight
		}
		score *= weight * decayFactor
	}

	return score
}
