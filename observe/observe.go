// Package observe provides observability integration for OmniRetrieve.
// It supports Phoenix, Opik, and Langfuse through a unified interface.
package observe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/agentplexus/omniretrieve/retrieve"
	"github.com/grokify/oscompat/id"
)

// SpanType identifies the type of operation being traced.
type SpanType string

const (
	SpanTypeRetrieval     SpanType = "retrieval"
	SpanTypeVectorSearch  SpanType = "retrieve.vector.search"
	SpanTypeGraphTraverse SpanType = "retrieve.graph.traverse"
	SpanTypeHybridMerge   SpanType = "retrieve.hybrid.merge"
	SpanTypeRerank        SpanType = "retrieve.rerank"
)

// Span represents a traced operation.
type Span struct {
	// ID is the unique span identifier.
	ID string
	// TraceID links spans in the same trace.
	TraceID string
	// ParentID is the parent span ID (empty for root).
	ParentID string
	// Type identifies the operation type.
	Type SpanType
	// Name is the human-readable span name.
	Name string
	// StartTime is when the span started.
	StartTime time.Time
	// EndTime is when the span ended.
	EndTime time.Time
	// Attributes are key-value pairs for this span.
	Attributes map[string]any
	// Artifacts are larger objects attached to this span.
	Artifacts map[string]any
	// Status indicates success or failure.
	Status SpanStatus
	// Error contains error details if Status is Error.
	Error string
}

// SpanStatus indicates the outcome of a span.
type SpanStatus string

const (
	SpanStatusOK    SpanStatus = "ok"
	SpanStatusError SpanStatus = "error"
)

// SpanExporter exports spans to an observability backend.
type SpanExporter interface {
	// Export sends spans to the backend.
	Export(ctx context.Context, spans []Span) error
	// Name returns the exporter name.
	Name() string
}

// Observer implements retrieve.Observer with full tracing support.
type Observer struct {
	mu        sync.Mutex
	exporters []SpanExporter
	logger    *slog.Logger
	spans     map[string]*Span    // Active spans by ID
	traces    map[string][]string // TraceID -> SpanIDs
}

// ObserverConfig configures the Observer.
type ObserverConfig struct {
	// Exporters to send spans to.
	Exporters []SpanExporter
	// Logger for observer errors.
	Logger *slog.Logger
}

// NewObserver creates a new Observer.
func NewObserver(cfg ObserverConfig) *Observer {
	return &Observer{
		exporters: cfg.Exporters,
		logger:    cfg.Logger,
		spans:     make(map[string]*Span),
		traces:    make(map[string][]string),
	}
}

// contextKey is used to store span context.
type contextKey struct{}

// SpanContext holds the current span information in context.
type SpanContext struct {
	TraceID  string
	SpanID   string
	ParentID string
}

// FromContext extracts SpanContext from context.
func FromContext(ctx context.Context) *SpanContext {
	if sc, ok := ctx.Value(contextKey{}).(*SpanContext); ok {
		return sc
	}
	return nil
}

// ToContext stores SpanContext in context.
func ToContext(ctx context.Context, sc *SpanContext) context.Context {
	return context.WithValue(ctx, contextKey{}, sc)
}

// OnRetrieveStart implements retrieve.Observer.
func (o *Observer) OnRetrieveStart(ctx context.Context, q retrieve.Query) context.Context {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Generate IDs
	spanID := generateID()
	traceID := spanID // New trace for root span
	parentID := ""

	// Check for existing trace context
	if sc := FromContext(ctx); sc != nil {
		traceID = sc.TraceID
		parentID = sc.SpanID
	}

	// Create span
	span := &Span{
		ID:        spanID,
		TraceID:   traceID,
		ParentID:  parentID,
		Type:      SpanTypeRetrieval,
		Name:      "retrieve",
		StartTime: time.Now(),
		Attributes: map[string]any{
			"retrieval.query_hash": hashQuery(q.Text),
			"retrieval.top_k":      q.TopK,
			"retrieval.modes":      q.Modes,
			"retrieval.min_score":  q.MinScore,
		},
		Artifacts: make(map[string]any),
		Status:    SpanStatusOK,
	}

	o.spans[spanID] = span
	o.traces[traceID] = append(o.traces[traceID], spanID)

	// Return context with span info
	return ToContext(ctx, &SpanContext{
		TraceID:  traceID,
		SpanID:   spanID,
		ParentID: parentID,
	})
}

// OnRetrieveEnd implements retrieve.Observer.
func (o *Observer) OnRetrieveEnd(ctx context.Context, r *retrieve.Result, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	sc := FromContext(ctx)
	if sc == nil {
		return
	}

	span, ok := o.spans[sc.SpanID]
	if !ok {
		return
	}

	span.EndTime = time.Now()

	if err != nil {
		span.Status = SpanStatusError
		span.Error = err.Error()
	} else if r != nil {
		span.Attributes["retrieval.result_count"] = len(r.Items)
		span.Attributes["retrieval.latency_ms"] = r.Metadata.LatencyMS
		span.Attributes["retrieval.modes_used"] = r.Metadata.ModesUsed
		span.Attributes["retrieval.cache_hit"] = r.Metadata.CacheHit
		span.Artifacts["retrieved.context"] = summarizeItems(r.Items)
	}

	// Export spans for this trace
	o.exportTrace(ctx, sc.TraceID)
}

// OnVectorSearch implements retrieve.Observer.
//
//nolint:dupl // Similar structure to OnGraphTraverse/OnRerank, but different attributes
func (o *Observer) OnVectorSearch(ctx context.Context, backend string, topK int, resultCount int, latencyMS int64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	sc := FromContext(ctx)
	if sc == nil {
		return
	}

	spanID := generateID()
	span := &Span{
		ID:        spanID,
		TraceID:   sc.TraceID,
		ParentID:  sc.SpanID,
		Type:      SpanTypeVectorSearch,
		Name:      "retrieve.vector.search",
		StartTime: time.Now().Add(-time.Duration(latencyMS) * time.Millisecond),
		EndTime:   time.Now(),
		Attributes: map[string]any{
			"vector.backend":      backend,
			"vector.top_k":        topK,
			"vector.result_count": resultCount,
			"vector.latency_ms":   latencyMS,
		},
		Artifacts: make(map[string]any),
		Status:    SpanStatusOK,
	}

	o.spans[spanID] = span
	o.traces[sc.TraceID] = append(o.traces[sc.TraceID], spanID)
}

// OnGraphTraverse implements retrieve.Observer.
//
//nolint:dupl // Similar structure to OnVectorSearch/OnRerank, but different attributes
func (o *Observer) OnGraphTraverse(ctx context.Context, backend string, depth int, nodeCount int, latencyMS int64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	sc := FromContext(ctx)
	if sc == nil {
		return
	}

	spanID := generateID()
	span := &Span{
		ID:        spanID,
		TraceID:   sc.TraceID,
		ParentID:  sc.SpanID,
		Type:      SpanTypeGraphTraverse,
		Name:      "retrieve.graph.traverse",
		StartTime: time.Now().Add(-time.Duration(latencyMS) * time.Millisecond),
		EndTime:   time.Now(),
		Attributes: map[string]any{
			"graph.backend":    backend,
			"graph.depth":      depth,
			"graph.node_count": nodeCount,
			"graph.latency_ms": latencyMS,
		},
		Artifacts: make(map[string]any),
		Status:    SpanStatusOK,
	}

	o.spans[spanID] = span
	o.traces[sc.TraceID] = append(o.traces[sc.TraceID], spanID)
}

// OnRerank implements retrieve.Observer.
//
//nolint:dupl // Similar structure to OnVectorSearch/OnGraphTraverse, but different attributes
func (o *Observer) OnRerank(ctx context.Context, model string, inputCount int, outputCount int, latencyMS int64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	sc := FromContext(ctx)
	if sc == nil {
		return
	}

	spanID := generateID()
	span := &Span{
		ID:        spanID,
		TraceID:   sc.TraceID,
		ParentID:  sc.SpanID,
		Type:      SpanTypeRerank,
		Name:      "retrieve.rerank",
		StartTime: time.Now().Add(-time.Duration(latencyMS) * time.Millisecond),
		EndTime:   time.Now(),
		Attributes: map[string]any{
			"reranker.model":        model,
			"reranker.input_count":  inputCount,
			"reranker.output_count": outputCount,
			"reranker.latency_ms":   latencyMS,
		},
		Artifacts: make(map[string]any),
		Status:    SpanStatusOK,
	}

	o.spans[spanID] = span
	o.traces[sc.TraceID] = append(o.traces[sc.TraceID], spanID)
}

// exportTrace exports all spans for a trace.
func (o *Observer) exportTrace(ctx context.Context, traceID string) {
	spanIDs, ok := o.traces[traceID]
	if !ok {
		return
	}

	spans := make([]Span, 0, len(spanIDs))
	for _, id := range spanIDs {
		if span, ok := o.spans[id]; ok {
			spans = append(spans, *span)
		}
	}

	for _, exporter := range o.exporters {
		if err := exporter.Export(ctx, spans); err != nil && o.logger != nil {
			o.logger.Error("failed to export spans",
				"exporter", exporter.Name(),
				"error", err,
			)
		}
	}

	// Clean up
	for _, id := range spanIDs {
		delete(o.spans, id)
	}
	delete(o.traces, traceID)
}

// generateID generates a unique span ID using crypto/rand.
// This is cross-platform safe, unlike time-based generation which fails
// on Windows due to coarse clock resolution (~15.6ms).
func generateID() string {
	return id.Generate16()
}

// hashQuery creates a hash of the query text for logging.
func hashQuery(text string) string {
	h := sha256.New()
	h.Write([]byte(text))
	return hex.EncodeToString(h.Sum(nil))[:8]
}

// summarizeItems creates a summary of retrieved items for artifacts.
func summarizeItems(items []retrieve.ContextItem) []map[string]any {
	summary := make([]map[string]any, len(items))
	for i, item := range items {
		summary[i] = map[string]any{
			"id":     item.ID,
			"source": item.Source,
			"score":  item.Score,
			"mode":   item.Provenance.Mode,
		}
	}
	return summary
}

// NoOpObserver is a no-op implementation of retrieve.Observer.
type NoOpObserver struct{}

// OnRetrieveStart implements retrieve.Observer.
func (n *NoOpObserver) OnRetrieveStart(ctx context.Context, _ retrieve.Query) context.Context {
	return ctx
}

// OnRetrieveEnd implements retrieve.Observer.
func (n *NoOpObserver) OnRetrieveEnd(_ context.Context, _ *retrieve.Result, _ error) {}

// OnVectorSearch implements retrieve.Observer.
func (n *NoOpObserver) OnVectorSearch(_ context.Context, _ string, _ int, _ int, _ int64) {}

// OnGraphTraverse implements retrieve.Observer.
func (n *NoOpObserver) OnGraphTraverse(_ context.Context, _ string, _ int, _ int, _ int64) {}

// OnRerank implements retrieve.Observer.
func (n *NoOpObserver) OnRerank(_ context.Context, _ string, _ int, _ int, _ int64) {}

// Verify interface compliance
var _ retrieve.Observer = (*Observer)(nil)
var _ retrieve.Observer = (*NoOpObserver)(nil)
