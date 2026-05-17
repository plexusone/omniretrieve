package memory

import (
	"context"
	"hash/fnv"

	"github.com/plexusone/omniretrieve/vector"
)

// HashEmbedder creates deterministic embeddings using hashing.
// This is for testing only - not suitable for production.
type HashEmbedder struct {
	dimensions int
}

// NewHashEmbedder creates a new hash-based embedder.
func NewHashEmbedder(dimensions int) *HashEmbedder {
	if dimensions <= 0 {
		dimensions = 384
	}
	return &HashEmbedder{dimensions: dimensions}
}

// Embed implements vector.Embedder.
func (e *HashEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	embedding := make([]float32, e.dimensions)

	// Create a deterministic embedding from text
	h := fnv.New64a()
	h.Write([]byte(text))
	seed := h.Sum64()

	// Generate embedding values using the hash as a seed
	for i := 0; i < e.dimensions; i++ {
		// Simple deterministic value generation
		shift := uint(i % 64) //nolint:gosec // i%64 is always in [0,63], safe for uint
		val := float64((seed>>shift)&0xFF) / 255.0
		embedding[i] = float32(val*2 - 1) // Normalize to [-1, 1]
	}

	// Normalize the vector
	var norm float64
	for _, v := range embedding {
		norm += float64(v * v)
	}
	if norm > 0 {
		norm = 1.0 / norm
		for i := range embedding {
			embedding[i] *= float32(norm)
		}
	}

	return embedding, nil
}

// EmbedBatch implements vector.Embedder.
func (e *HashEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	embeddings := make([][]float32, len(texts))
	for i, text := range texts {
		emb, err := e.Embed(ctx, text)
		if err != nil {
			return nil, err
		}
		embeddings[i] = emb
	}
	return embeddings, nil
}

// Model implements vector.Embedder.
func (e *HashEmbedder) Model() string {
	return "hash-embedder"
}

// Verify interface compliance
var _ vector.Embedder = (*HashEmbedder)(nil)
