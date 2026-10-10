package database_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestCacheConfigPersistenceAndDrop(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test_cache.duckdb")
	db, err := database.InitDB(&config.Config{DuckDBPath: dbPath})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// 1. Initial read should be empty
	configs, err := database.ReadCacheConfigs(ctx, db)
	if err != nil {
		t.Fatalf("ReadCacheConfigs failed: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs, got %d", len(configs))
	}

	// 2. Upsert config
	row1 := database.CacheConfigRow{
		Topic:         "test-topic-1",
		Format:        "json",
		MessageName:   "",
		StartFrom:     "beginning",
		LimitType:     "indefinitely",
		LimitValue:    "",
		Active:        true,
		EstimatedSize: 1048576,
		CreatedAt:     time.Now().Truncate(time.Microsecond),
	}
	if err := database.UpsertCacheConfig(ctx, db, row1); err != nil {
		t.Fatalf("UpsertCacheConfig failed: %v", err)
	}

	// 3. Read back
	configs, err = database.ReadCacheConfigs(ctx, db)
	if err != nil {
		t.Fatalf("ReadCacheConfigs after insert failed: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(configs))
	}
	if configs[0].Topic != row1.Topic || configs[0].Format != row1.Format || !configs[0].Active {
		t.Fatalf("unexpected config row: %+v", configs[0])
	}

	// 4. Update config active = false
	row1.Active = false
	if err := database.UpsertCacheConfig(ctx, db, row1); err != nil {
		t.Fatalf("UpsertCacheConfig update failed: %v", err)
	}
	configs, err = database.ReadCacheConfigs(ctx, db)
	if err != nil {
		t.Fatalf("ReadCacheConfigs after update failed: %v", err)
	}
	if len(configs) != 1 || configs[0].Active {
		t.Fatalf("expected 1 inactive config, got %+v", configs[0])
	}

	// 5. Delete config
	if err := database.DeleteCacheConfig(ctx, db, row1.Topic); err != nil {
		t.Fatalf("DeleteCacheConfig failed: %v", err)
	}
	configs, err = database.ReadCacheConfigs(ctx, db)
	if err != nil {
		t.Fatalf("ReadCacheConfigs after delete failed: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs after delete, got %d", len(configs))
	}

	// 6. Test DropTable and CompactDB
	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("db.Connect failed: %v", err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE dummy_to_drop (id INT);"); err != nil {
		_ = conn.Close()
		t.Fatalf("create dummy table failed: %v", err)
	}
	_ = conn.Close()

	if err := database.DropTable(ctx, db, "dummy_to_drop"); err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	if err := database.CompactDB(ctx, db); err != nil {
		t.Fatalf("CompactDB failed: %v", err)
	}
}
