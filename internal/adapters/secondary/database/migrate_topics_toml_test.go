package database_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/topicsfile"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestMigrateTopicsToml(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "migrate.duckdb")
	db, err := database.InitDB(&config.Config{DuckDBPath: dbPath})
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	defer db.Close()

	// 1. Missing file returns 0, nil
	missingPath := filepath.Join(dbDir, "missing.toml")
	count, err := database.MigrateTopicsToml(ctx, db, missingPath)
	if err != nil || count != 0 {
		t.Fatalf("expected (0, nil), got (%d, %v)", count, err)
	}

	// 2. Create sample topics.toml
	tomlPath := filepath.Join(dbDir, "topics.toml")
	f := &topicsfile.File{
		Topics: map[string]topicsfile.Topic{
			"events.user": {
				Format:          "json",
				Table:           "kafka_events_user_sample",
				SampleCount:     25,
				MatchedCount:    25,
				MessageOverride: false,
				SampledAt:       time.Now().UTC().Format(time.RFC3339),
			},
			"events.order": {
				Format:              "protobuf",
				MessageName:         "OrderPlaced",
				ProtoFile:           "order.proto",
				ImportRoot:          "./proto",
				CutConfluencePrefix: true,
				MessageOverride:     true,
				SampleCount:         50,
				MatchedCount:        50,
				SampledAt:           time.Now().UTC().Format(time.RFC3339),
			},
		},
	}
	if err := f.Save(tomlPath); err != nil {
		t.Fatalf("Save toml: %v", err)
	}

	// 3. Migrate
	count, err = database.MigrateTopicsToml(ctx, db, tomlPath)
	if err != nil {
		t.Fatalf("MigrateTopicsToml error: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 imported, got %d", count)
	}

	// 4. Check file renamed to .bak
	if _, err := os.Stat(tomlPath); !os.IsNotExist(err) {
		t.Errorf("expected original toml to be moved/renamed")
	}
	if _, err := os.Stat(tomlPath + ".bak"); err != nil {
		t.Errorf("expected .bak to exist: %v", err)
	}

	// 5. Verify data in DuckDB
	schemas, err := database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("ReadTopicSchemas: %v", err)
	}
	if len(schemas) != 2 {
		t.Fatalf("expected 2 rows in DuckDB, got %d", len(schemas))
	}
	orderSchema, ok := schemas["events.order"]
	if !ok {
		t.Fatalf("events.order not found in schemas")
	}
	if orderSchema.MessageName != "OrderPlaced" || !orderSchema.MessageOverride || !orderSchema.CutConfluencePrefix {
		t.Errorf("unexpected schema values: %+v", orderSchema)
	}
}
