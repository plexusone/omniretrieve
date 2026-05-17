package observe_test

import (
	"context"
	"sync"
	"testing"

	"github.com/plexusone/omniretrieve/observe"
	"github.com/plexusone/omniretrieve/retrieve"
)

// mockExporter captures exported spans for testing.
type mockExporter struct {
	mu    sync.Mutex
	spans []observe.Span
}

func (m *mockExporter) Export(ctx context.Context, spans []observe.Span) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spans = append(m.spans, spans...)
	return nil
}

func (m *mockExporter) Name() string {
	return "mock"
}

func (m *mockExporter) Spans() []observe.Span {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.spans
}

func TestObserver(t *testing.T) {
	exporter := &mockExporter{}
	observer := observe.NewObserver(observe.ObserverConfig{
		Exporters: []observe.SpanExporter{exporter},
	})

	ctx := context.Background()

	// Start retrieval
	query := retrieve.Query{
		Text: "test query",
		TopK: 10,
	}
	ctx = observer.OnRetrieveStart(ctx, query)

	// Report vector search
	observer.OnVectorSearch(ctx, "test-index", 10, 5, 100)

	// Report graph traverse
	observer.OnGraphTraverse(ctx, "test-graph", 2, 3, 50)

	// End retrieval
	result := &retrieve.Result{
		Items: []retrieve.ContextItem{
			{ID: "1", Content: "result 1", Score: 0.9},
		},
		Query: query,
		Metadata: retrieve.ResultMetadata{
			TotalCandidates: 8,
			LatencyMS:       150,
			ModesUsed:       []retrieve.Mode{retrieve.ModeHybrid},
		},
	}
	observer.OnRetrieveEnd(ctx, result, nil)

	// Verify spans were exported
	spans := exporter.Spans()
	if len(spans) != 3 {
		t.Errorf("expected 3 spans, got %d", len(spans))
	}

	// Check span types
	spanTypes := make(map[observe.SpanType]bool)
	for _, span := range spans {
		spanTypes[span.Type] = true
	}

	if !spanTypes[observe.SpanTypeRetrieval] {
		t.Error("expected retrieval span")
	}
	if !spanTypes[observe.SpanTypeVectorSearch] {
		t.Error("expected vector search span")
	}
	if !spanTypes[observe.SpanTypeGraphTraverse] {
		t.Error("expected graph traverse span")
	}
}

func TestObserverTraceContext(t *testing.T) {
	exporter := &mockExporter{}
	observer := observe.NewObserver(observe.ObserverConfig{
		Exporters: []observe.SpanExporter{exporter},
	})

	ctx := context.Background()

	// Start retrieval
	ctx = observer.OnRetrieveStart(ctx, retrieve.Query{Text: "test"})

	// Verify context contains span info
	sc := observe.FromContext(ctx)
	if sc == nil {
		t.Fatal("expected span context in context")
	}

	if sc.TraceID == "" {
		t.Error("expected trace ID")
	}
	if sc.SpanID == "" {
		t.Error("expected span ID")
	}

	// End retrieval
	observer.OnRetrieveEnd(ctx, nil, nil)
}

func TestNoOpObserver(t *testing.T) {
	observer := &observe.NoOpObserver{}
	ctx := context.Background()

	// These should not panic
	ctx = observer.OnRetrieveStart(ctx, retrieve.Query{})
	observer.OnVectorSearch(ctx, "", 0, 0, 0)
	observer.OnGraphTraverse(ctx, "", 0, 0, 0)
	observer.OnRerank(ctx, "", 0, 0, 0)
	observer.OnRetrieveEnd(ctx, nil, nil)
}

func TestObserverWithError(t *testing.T) {
	exporter := &mockExporter{}
	observer := observe.NewObserver(observe.ObserverConfig{
		Exporters: []observe.SpanExporter{exporter},
	})

	ctx := context.Background()

	// Start and end with error
	ctx = observer.OnRetrieveStart(ctx, retrieve.Query{Text: "test"})
	observer.OnRetrieveEnd(ctx, nil, context.DeadlineExceeded)

	spans := exporter.Spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	if spans[0].Status != observe.SpanStatusError {
		t.Errorf("expected error status, got %s", spans[0].Status)
	}
	if spans[0].Error == "" {
		t.Error("expected error message")
	}
}
