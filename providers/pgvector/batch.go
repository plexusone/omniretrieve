package pgvector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/plexusone/omniretrieve/vector"
	"github.com/lib/pq"
)

// InsertBatch implements vector.BatchIndex.
func (idx *Index) InsertBatch(ctx context.Context, nodes []vector.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// Use a transaction for atomicity
	tx, err := idx.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Prepare statement for batch insert
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(
		idx.tableName,
		"id", "content", "embedding", "source", "metadata",
	))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, node := range nodes {
		metadataJSON, err := json.Marshal(node.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata for node %s: %w", node.ID, err)
		}

		_, err = stmt.ExecContext(ctx,
			node.ID,
			node.Content,
			vectorToString(node.Embedding),
			node.Source,
			string(metadataJSON),
		)
		if err != nil {
			return fmt.Errorf("failed to exec for node %s: %w", node.ID, err)
		}
	}

	// Flush the COPY buffer
	_, err = stmt.ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush COPY: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// UpsertBatch implements vector.BatchIndex.
func (idx *Index) UpsertBatch(ctx context.Context, nodes []vector.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// Build a multi-row upsert query
	// PostgreSQL supports ON CONFLICT for bulk upserts
	valueStrings := make([]string, 0, len(nodes))
	valueArgs := make([]any, 0, len(nodes)*5)

	for i, node := range nodes {
		metadataJSON, err := json.Marshal(node.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata for node %s: %w", node.ID, err)
		}

		base := i * 5
		valueStrings = append(valueStrings,
			fmt.Sprintf("($%d, $%d, $%d::vector, $%d, $%d::jsonb)",
				base+1, base+2, base+3, base+4, base+5))

		valueArgs = append(valueArgs,
			node.ID,
			node.Content,
			vectorToString(node.Embedding),
			node.Source,
			string(metadataJSON),
		)
	}

	//nolint:gosec // Table name escaped via pq.QuoteIdentifier, values are parameterized
	query := fmt.Sprintf(`
		INSERT INTO %s (id, content, embedding, source, metadata)
		VALUES %s
		ON CONFLICT (id) DO UPDATE SET
			content = EXCLUDED.content,
			embedding = EXCLUDED.embedding,
			source = EXCLUDED.source,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`, pq.QuoteIdentifier(idx.tableName), strings.Join(valueStrings, ","))

	_, err := idx.db.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return fmt.Errorf("upsert batch failed: %w", err)
	}

	return nil
}

// DeleteBatch implements vector.BatchIndex.
func (idx *Index) DeleteBatch(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	// Build parameterized IN clause
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	//nolint:gosec // Table name escaped via pq.QuoteIdentifier, IDs are parameterized
	query := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)",
		pq.QuoteIdentifier(idx.tableName),
		strings.Join(placeholders, ","))

	_, err := idx.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete batch failed: %w", err)
	}

	return nil
}

// Verify interface compliance
var _ vector.BatchIndex = (*Index)(nil)
