package database_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestGetResumeOffsetsCombinesLegacyAndPrefixedColumns(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "t.duckdb")})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// A JSON cache written before the rename, then extended with the new columns:
	// old rows only have kafka_*, new rows only have _kafka_*.
	for _, q := range []string{
		`CREATE TABLE kafka_t (kafka_partition INTEGER, kafka_offset BIGINT, _kafka_partition INTEGER, _kafka_offset BIGINT)`,
		`INSERT INTO kafka_t VALUES (0, 5, NULL, NULL), (1, 9, NULL, NULL), (0, NULL, NULL, NULL)`,
		`INSERT INTO kafka_t VALUES (NULL, NULL, 0, 7), (NULL, NULL, 2, 3)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	got, err := database.GetResumeOffsets(ctx, db, "kafka_t")
	if err != nil {
		t.Fatal(err)
	}
	want := map[int32]int64{0: 7, 1: 9, 2: 3}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for p, o := range want {
		if got[p] != o {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
