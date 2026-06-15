package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/plexusone/omniretrieve/vector"
)

// Document represents a document to store in memory.
type Document struct {
	// ID is the unique document identifier.
	ID string

	// Content is the document text.
	Content string

	// Embedding is the vector embedding (optional, computed if nil).
	Embedding []float32

	// Metadata contains additional document metadata.
	Metadata map[string]string

	// CreatedAt is when the document was created.
	CreatedAt time.Time

	// UpdatedAt is when the document was last updated.
	UpdatedAt time.Time
}

// Collection represents a named collection of documents.
type Collection struct {
	// Name is the collection name.
	Name string

	// Description is an optional description.
	Description string

	// VectorIndex holds the vector embeddings.
	VectorIndex vector.Index

	// Documents keyed by ID.
	documents map[string]*Document

	// mu protects the documents map.
	mu sync.RWMutex
}

// SearchOptions configures a search operation.
type SearchOptions struct {
	// TopK is the maximum number of results.
	TopK int

	// MinScore filters results below this threshold.
	MinScore float64

	// Filters are metadata filters to apply.
	Filters map[string]string

	// IncludeMetadata includes metadata in results.
	IncludeMetadata bool
}

// SearchResult represents a search result.
type SearchResult struct {
	// Document is the matched document.
	Document Document

	// Score is the similarity score.
	Score float64
}

// Manager manages named collections of documents.
type Manager struct {
	mu          sync.RWMutex
	collections map[string]*Collection
	embedder    vector.Embedder
}

// ManagerConfig configures the memory manager.
type ManagerConfig struct {
	// Embedder computes vector embeddings.
	Embedder vector.Embedder
}

// NewManager creates a new memory manager.
func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		collections: make(map[string]*Collection),
		embedder:    cfg.Embedder,
	}
}

// CreateCollection creates a new named collection.
func (m *Manager) CreateCollection(ctx context.Context, name, description string) (*Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.collections[name]; exists {
		return nil, fmt.Errorf("collection %q already exists", name)
	}

	collection := &Collection{
		Name:        name,
		Description: description,
		VectorIndex: NewVectorIndex(name),
		documents:   make(map[string]*Document),
	}

	m.collections[name] = collection
	return collection, nil
}

// GetCollection retrieves a collection by name.
func (m *Manager) GetCollection(name string) (*Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collection, ok := m.collections[name]
	if !ok {
		return nil, fmt.Errorf("collection %q not found", name)
	}
	return collection, nil
}

// GetOrCreateCollection gets an existing collection or creates a new one.
func (m *Manager) GetOrCreateCollection(ctx context.Context, name, description string) (*Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if collection, exists := m.collections[name]; exists {
		return collection, nil
	}

	collection := &Collection{
		Name:        name,
		Description: description,
		VectorIndex: NewVectorIndex(name),
		documents:   make(map[string]*Document),
	}

	m.collections[name] = collection
	return collection, nil
}

// ListCollections returns all collection names.
func (m *Manager) ListCollections() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.collections))
	for name := range m.collections {
		names = append(names, name)
	}
	return names
}

// DeleteCollection removes a collection.
func (m *Manager) DeleteCollection(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.collections[name]; !exists {
		return fmt.Errorf("collection %q not found", name)
	}

	delete(m.collections, name)
	return nil
}

// Store adds a document to a collection.
func (m *Manager) Store(ctx context.Context, collectionName, key string, doc *Document) error {
	collection, err := m.GetOrCreateCollection(ctx, collectionName, "")
	if err != nil {
		return err
	}

	// Set ID if not provided
	if doc.ID == "" {
		doc.ID = key
	}

	// Set timestamps
	now := time.Now()
	if doc.CreatedAt.IsZero() {
		doc.CreatedAt = now
	}
	doc.UpdatedAt = now

	// Compute embedding if not provided
	if doc.Embedding == nil && m.embedder != nil {
		embedding, err := m.embedder.Embed(ctx, doc.Content)
		if err != nil {
			return fmt.Errorf("compute embedding: %w", err)
		}
		doc.Embedding = embedding
	}

	// Store in collection
	collection.mu.Lock()
	collection.documents[key] = doc
	collection.mu.Unlock()

	// Add to vector index if we have an embedding
	if doc.Embedding != nil {
		node := vector.Node{
			ID:        key,
			Embedding: doc.Embedding,
			Metadata:  doc.Metadata,
		}
		if err := collection.VectorIndex.Upsert(ctx, node); err != nil {
			return fmt.Errorf("add to vector index: %w", err)
		}
	}

	return nil
}

// Get retrieves a document from a collection.
func (m *Manager) Get(ctx context.Context, collectionName, key string) (*Document, error) {
	collection, err := m.GetCollection(collectionName)
	if err != nil {
		return nil, err
	}

	collection.mu.RLock()
	defer collection.mu.RUnlock()

	doc, ok := collection.documents[key]
	if !ok {
		return nil, fmt.Errorf("document %q not found in collection %q", key, collectionName)
	}
	return doc, nil
}

// Delete removes a document from a collection.
func (m *Manager) Delete(ctx context.Context, collectionName, key string) error {
	collection, err := m.GetCollection(collectionName)
	if err != nil {
		return err
	}

	collection.mu.Lock()
	delete(collection.documents, key)
	collection.mu.Unlock()

	return collection.VectorIndex.Delete(ctx, key)
}

// Search performs a similarity search in a collection.
func (m *Manager) Search(ctx context.Context, collectionName, query string, opts SearchOptions) ([]SearchResult, error) {
	collection, err := m.GetCollection(collectionName)
	if err != nil {
		return nil, err
	}

	// Compute query embedding
	if m.embedder == nil {
		return nil, fmt.Errorf("embedder not configured")
	}

	embedding, err := m.embedder.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("compute query embedding: %w", err)
	}

	// Set defaults
	topK := opts.TopK
	if topK <= 0 {
		topK = 10
	}

	// Search vector index
	searchResults, err := collection.VectorIndex.Search(ctx, embedding, topK, opts.Filters)
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}

	// Build results
	results := make([]SearchResult, 0, len(searchResults))
	collection.mu.RLock()
	defer collection.mu.RUnlock()

	for _, sr := range searchResults {
		// Apply min score filter
		if sr.Score < opts.MinScore {
			continue
		}

		doc, ok := collection.documents[sr.Node.ID]
		if !ok {
			continue
		}

		result := SearchResult{
			Score: sr.Score,
			Document: Document{
				ID:        doc.ID,
				Content:   doc.Content,
				CreatedAt: doc.CreatedAt,
				UpdatedAt: doc.UpdatedAt,
			},
		}

		if opts.IncludeMetadata {
			result.Document.Metadata = doc.Metadata
		}

		results = append(results, result)
	}

	return results, nil
}

// Count returns the number of documents in a collection.
func (m *Manager) Count(ctx context.Context, collectionName string) (int, error) {
	collection, err := m.GetCollection(collectionName)
	if err != nil {
		return 0, err
	}

	collection.mu.RLock()
	defer collection.mu.RUnlock()

	return len(collection.documents), nil
}

// List returns all documents in a collection.
func (m *Manager) List(ctx context.Context, collectionName string, limit, offset int) ([]Document, error) {
	collection, err := m.GetCollection(collectionName)
	if err != nil {
		return nil, err
	}

	collection.mu.RLock()
	defer collection.mu.RUnlock()

	docs := make([]Document, 0, len(collection.documents))
	for _, doc := range collection.documents {
		docs = append(docs, *doc)
	}

	// Apply offset
	if offset > 0 && offset < len(docs) {
		docs = docs[offset:]
	} else if offset >= len(docs) {
		return nil, nil
	}

	// Apply limit
	if limit > 0 && limit < len(docs) {
		docs = docs[:limit]
	}

	return docs, nil
}
