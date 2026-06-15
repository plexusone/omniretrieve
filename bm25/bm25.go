// Package bm25 provides BM25 (Best Matching 25) text search implementation.
package bm25

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"
)

// Document represents a document in the index.
type Document struct {
	// ID is the unique document identifier.
	ID string
	// Content is the document text.
	Content string
	// Metadata contains additional document metadata.
	Metadata map[string]string
}

// ScoredDocument represents a document with its BM25 score.
type ScoredDocument struct {
	Document Document
	Score    float64
}

// Index implements a BM25 index for text search.
type Index struct {
	mu sync.RWMutex

	// Documents keyed by ID.
	docs map[string]*Document

	// Term frequency per document: docID -> term -> count.
	termFreqs map[string]map[string]int

	// Document frequency: term -> number of documents containing term.
	docFreqs map[string]int

	// Document lengths: docID -> number of terms.
	docLengths map[string]int

	// Average document length.
	avgDL float64

	// Total number of documents.
	totalDocs int

	// BM25 parameters.
	k1 float64 // Term frequency saturation (default: 1.5)
	b  float64 // Document length normalization (default: 0.75)
}

// Config configures the BM25 index.
type Config struct {
	// K1 controls term frequency saturation (default: 1.5).
	// Higher values give more weight to term frequency.
	K1 float64

	// B controls document length normalization (default: 0.75).
	// B=0 disables normalization, B=1 fully normalizes.
	B float64
}

// DefaultConfig returns the default BM25 configuration.
func DefaultConfig() Config {
	return Config{
		K1: 1.5,
		B:  0.75,
	}
}

// New creates a new BM25 index with the given configuration.
func New(cfg Config) *Index {
	if cfg.K1 == 0 {
		cfg.K1 = 1.5
	}
	if cfg.B == 0 {
		cfg.B = 0.75
	}
	return &Index{
		docs:       make(map[string]*Document),
		termFreqs:  make(map[string]map[string]int),
		docFreqs:   make(map[string]int),
		docLengths: make(map[string]int),
		k1:         cfg.K1,
		b:          cfg.B,
	}
}

// Insert adds a document to the index.
func (idx *Index) Insert(ctx context.Context, doc Document) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove existing document if present
	idx.removeDocInternal(doc.ID)

	// Tokenize content
	terms := tokenize(doc.Content)
	if len(terms) == 0 {
		return nil
	}

	// Store document
	idx.docs[doc.ID] = &doc
	idx.docLengths[doc.ID] = len(terms)
	idx.totalDocs++

	// Calculate term frequencies for this document
	termFreq := make(map[string]int)
	seenTerms := make(map[string]bool)
	for _, term := range terms {
		termFreq[term]++
		if !seenTerms[term] {
			idx.docFreqs[term]++
			seenTerms[term] = true
		}
	}
	idx.termFreqs[doc.ID] = termFreq

	// Update average document length
	idx.updateAvgDL()

	return nil
}

// InsertBatch adds multiple documents to the index.
func (idx *Index) InsertBatch(ctx context.Context, docs []Document) error {
	for _, doc := range docs {
		if err := idx.Insert(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes a document from the index.
func (idx *Index) Delete(ctx context.Context, id string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.removeDocInternal(id)
	return nil
}

// removeDocInternal removes a document without locking.
func (idx *Index) removeDocInternal(id string) {
	if _, exists := idx.docs[id]; !exists {
		return
	}

	// Update document frequencies
	if termFreq, ok := idx.termFreqs[id]; ok {
		for term := range termFreq {
			idx.docFreqs[term]--
			if idx.docFreqs[term] <= 0 {
				delete(idx.docFreqs, term)
			}
		}
	}

	// Remove document data
	delete(idx.docs, id)
	delete(idx.termFreqs, id)
	delete(idx.docLengths, id)
	idx.totalDocs--

	// Update average document length
	idx.updateAvgDL()
}

// updateAvgDL recalculates the average document length.
func (idx *Index) updateAvgDL() {
	if idx.totalDocs == 0 {
		idx.avgDL = 0
		return
	}

	var totalLen int
	for _, length := range idx.docLengths {
		totalLen += length
	}
	idx.avgDL = float64(totalLen) / float64(idx.totalDocs)
}

// Search performs a BM25 search and returns the top k results.
func (idx *Index) Search(ctx context.Context, query string, k int) ([]ScoredDocument, error) {
	return idx.SearchWithFilters(ctx, query, k, nil)
}

// SearchWithFilters performs a BM25 search with optional metadata filters.
func (idx *Index) SearchWithFilters(ctx context.Context, query string, k int, filters map[string]string) ([]ScoredDocument, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.totalDocs == 0 {
		return nil, nil
	}

	// Tokenize query
	queryTerms := tokenize(query)
	if len(queryTerms) == 0 {
		return nil, nil
	}

	// Calculate scores for all documents
	scores := make(map[string]float64)
	for docID := range idx.docs {
		// Apply metadata filters
		if !idx.matchesFilters(docID, filters) {
			continue
		}

		score := idx.calculateScore(docID, queryTerms)
		if score > 0 {
			scores[docID] = score
		}
	}

	// Convert to slice and sort
	results := make([]ScoredDocument, 0, len(scores))
	for docID, score := range scores {
		results = append(results, ScoredDocument{
			Document: *idx.docs[docID],
			Score:    score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Return top k
	if k > 0 && len(results) > k {
		results = results[:k]
	}

	return results, nil
}

// calculateScore calculates the BM25 score for a document given query terms.
func (idx *Index) calculateScore(docID string, queryTerms []string) float64 {
	termFreq := idx.termFreqs[docID]
	docLen := float64(idx.docLengths[docID])

	var score float64
	for _, term := range queryTerms {
		tf := float64(termFreq[term])
		if tf == 0 {
			continue
		}

		df := float64(idx.docFreqs[term])
		n := float64(idx.totalDocs)

		// IDF component: log((N - df + 0.5) / (df + 0.5) + 1)
		idf := math.Log((n-df+0.5)/(df+0.5) + 1)

		// TF component: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
		tfComponent := (tf * (idx.k1 + 1)) / (tf + idx.k1*(1-idx.b+idx.b*docLen/idx.avgDL))

		score += idf * tfComponent
	}

	return score
}

// matchesFilters checks if a document matches the given filters.
func (idx *Index) matchesFilters(docID string, filters map[string]string) bool {
	if len(filters) == 0 {
		return true
	}

	doc := idx.docs[docID]
	if doc == nil || doc.Metadata == nil {
		return len(filters) == 0
	}

	for key, value := range filters {
		if doc.Metadata[key] != value {
			return false
		}
	}
	return true
}

// Count returns the number of documents in the index.
func (idx *Index) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.totalDocs
}

// Get retrieves a document by ID.
func (idx *Index) Get(ctx context.Context, id string) (*Document, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	doc, ok := idx.docs[id]
	if !ok {
		return nil, false
	}
	return doc, true
}

// tokenize splits text into lowercase terms.
func tokenize(text string) []string {
	text = strings.ToLower(text)

	// Simple tokenization: split on non-alphanumeric characters
	var terms []string
	var current strings.Builder

	for _, r := range text {
		if isAlphanumeric(r) {
			current.WriteRune(r)
		} else if current.Len() > 0 {
			term := current.String()
			if len(term) > 1 { // Skip single-character terms
				terms = append(terms, term)
			}
			current.Reset()
		}
	}

	// Don't forget the last term
	if current.Len() > 0 {
		term := current.String()
		if len(term) > 1 {
			terms = append(terms, term)
		}
	}

	return terms
}

// isAlphanumeric checks if a rune is alphanumeric.
func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9')
}
