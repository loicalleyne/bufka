package database_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestCacheOffsetsSaveReadDelete(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "o.duckdb")})
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if got, err := database.ReadCacheOffsets(ctx, db, "t.a"); err != nil || len(got) != 0 {
		t.Fatalf("empty read = %v, %v", got, err)
	}
	if err := database.SaveCacheOffsets(ctx, db, "t.a", map[int32]int64{0: 99, 1: 5}); err != nil {
		t.Fatal(err)
	}
	// Lower offsets never move a partition backwards.
	if err := database.SaveCacheOffsets(ctx, db, "t.a", map[int32]int64{0: 10, 2: 7}); err != nil {
		t.Fatal(err)
	}
	if err := database.SaveCacheOffsets(ctx, db, "t.b", map[int32]int64{0: 1}); err != nil {
		t.Fatal(err)
	}
	got, err := database.ReadCacheOffsets(ctx, db, "t.a")
	if err != nil {
		t.Fatal(err)
	}
	want := map[int32]int64{0: 99, 1: 5, 2: 7}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for p, o := range want {
		if got[p] != o {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	if err := database.DeleteCacheOffsets(ctx, db, "t.a"); err != nil {
		t.Fatal(err)
	}
	if got, _ := database.ReadCacheOffsets(ctx, db, "t.a"); len(got) != 0 {
		t.Fatalf("after delete: %v", got)
	}
	if got, _ := database.ReadCacheOffsets(ctx, db, "t.b"); got[0] != 1 {
		t.Fatalf("t.b offsets: %v", got)
	}
}
