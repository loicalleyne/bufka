package tui

import (
	"context"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/couac"
)

// schemaTestDB opens an in-memory DuckDB holding the given topic schemas.
func schemaTestDB(t *testing.T, rows ...database.TopicSchemaRow) *couac.DB {
	t.Helper()
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := database.EnsureTopicSchemaTable(ctx, db); err != nil {
		t.Fatal(err)
	}
	if len(rows) > 0 {
		if err := database.UpsertTopicSchemas(ctx, db, rows); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
