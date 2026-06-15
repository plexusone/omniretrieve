package bm25

import (
	"context"
	"testing"
)

func TestIndex_SearchBasic(t *testing.T) {
	idx := New(DefaultConfig())
	ctx := context.Background()

	// Insert test documents
	docs := []Document{
		{ID: "1", Content: "The quick brown fox jumps over the lazy dog"},
		{ID: "2", Content: "A quick brown dog runs in the park"},
		{ID: "3", Content: "The lazy cat sleeps all day"},
		{ID: "4", Content: "Brown foxes are quick and agile animals"},
	}

	for _, doc := range docs {
		if err := idx.Insert(ctx, doc); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	if idx.Count() != 4 {
		t.Fatalf("Expected 4 documents, got %d", idx.Count())
	}

	// Search for "quick brown fox"
	results, err := idx.Search(ctx, "quick brown fox", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected results, got none")
	}

	// Document 1 should be the top result (exact match)
	if results[0].Document.ID != "1" {
		t.Errorf("Expected document 1 to be top result, got %s", results[0].Document.ID)
	}

	// Document 4 should also be highly ranked (contains fox, brown, quick)
	found4 := false
	for _, r := range results {
		if r.Document.ID == "4" {
			found4 = true
			break
		}
	}
	if !found4 {
		t.Error("Expected document 4 in results")
	}
}

func TestIndex_SearchWithFilters(t *testing.T) {
	idx := New(DefaultConfig())
	ctx := context.Background()

	docs := []Document{
		{ID: "1", Content: "Programming in Go", Metadata: map[string]string{"category": "tech"}},
		{ID: "2", Content: "Cooking with Go(o)d ingredients", Metadata: map[string]string{"category": "food"}},
		{ID: "3", Content: "Advanced Go programming", Metadata: map[string]string{"category": "tech"}},
	}

	for _, doc := range docs {
		if err := idx.Insert(ctx, doc); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Search for "programming" with tech filter
	results, err := idx.SearchWithFilters(ctx, "programming", 10, map[string]string{"category": "tech"})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// All results should have category=tech
	for _, r := range results {
		if r.Document.Metadata["category"] != "tech" {
			t.Errorf("Expected category=tech, got %s", r.Document.Metadata["category"])
		}
	}
}

func TestIndex_Delete(t *testing.T) {
	idx := New(DefaultConfig())
	ctx := context.Background()

	if err := idx.Insert(ctx, Document{ID: "1", Content: "hello world"}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Insert(ctx, Document{ID: "2", Content: "goodbye world"}); err != nil {
		t.Fatal(err)
	}

	if idx.Count() != 2 {
		t.Fatalf("Expected 2 documents, got %d", idx.Count())
	}

	if err := idx.Delete(ctx, "1"); err != nil {
		t.Fatal(err)
	}

	if idx.Count() != 1 {
		t.Fatalf("Expected 1 document, got %d", idx.Count())
	}

	// Search should only return doc 2
	results, err := idx.Search(ctx, "world", 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 || results[0].Document.ID != "2" {
		t.Error("Expected only document 2 in results")
	}
}

func TestIndex_EmptyQuery(t *testing.T) {
	idx := New(DefaultConfig())
	ctx := context.Background()

	if err := idx.Insert(ctx, Document{ID: "1", Content: "hello world"}); err != nil {
		t.Fatal(err)
	}

	results, err := idx.Search(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Error("Expected no results for empty query")
	}
}

func TestIndex_NoDocuments(t *testing.T) {
	idx := New(DefaultConfig())
	ctx := context.Background()

	results, err := idx.Search(ctx, "hello", 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Error("Expected no results for empty index")
	}
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"Hello World", []string{"hello", "world"}},
		{"the quick brown fox", []string{"the", "quick", "brown", "fox"}},
		{"hello-world_test", []string{"hello", "world", "test"}},
		{"123 abc 456", []string{"123", "abc", "456"}},
		{"a b c", []string{}}, // Single chars are filtered
		{"", []string{}},
	}

	for _, tt := range tests {
		result := tokenize(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("tokenize(%q): expected %v, got %v", tt.input, tt.expected, result)
			continue
		}
		for i, term := range result {
			if term != tt.expected[i] {
				t.Errorf("tokenize(%q)[%d]: expected %q, got %q", tt.input, i, tt.expected[i], term)
			}
		}
	}
}
