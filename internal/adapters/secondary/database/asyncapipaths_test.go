package database_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestAsyncAPIPathsRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "p.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Nothing saved yet: not found, so callers fall back to the env var.
	paths, found, err := database.ReadAsyncAPIPaths(ctx, db)
	if err != nil || found || paths != nil {
		t.Fatalf("expected not found, got paths=%v found=%v err=%v", paths, found, err)
	}

	want := []string{"/z/last", "/a/first", "/m/mid"}
	if err := database.WriteAsyncAPIPaths(ctx, db, want); err != nil {
		t.Fatal(err)
	}
	got, found, err := database.ReadAsyncAPIPaths(ctx, db)
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatalf("order not preserved: got=%v found=%v err=%v", got, found, err)
	}

	// Replace semantics.
	if err := database.WriteAsyncAPIPaths(ctx, db, []string{"/only"}); err != nil {
		t.Fatal(err)
	}
	got, _, _ = database.ReadAsyncAPIPaths(ctx, db)
	if !reflect.DeepEqual(got, []string{"/only"}) {
		t.Fatalf("expected replace, got %v", got)
	}

	// Saving an empty list leaves a table with no rows.
	if err := database.WriteAsyncAPIPaths(ctx, db, nil); err != nil {
		t.Fatal(err)
	}
	got, found, err = database.ReadAsyncAPIPaths(ctx, db)
	if err != nil || !found || len(got) != 0 {
		t.Fatalf("expected found+empty, got=%v found=%v err=%v", got, found, err)
	}
}

func TestAsyncAPIPathsIndependentOfProtoPaths(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "p.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := database.WriteProtoImportPaths(ctx, db, []string{"/proto"}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := database.ReadAsyncAPIPaths(ctx, db); err != nil || found {
		t.Fatalf("asyncapi paths should be unset, found=%v err=%v", found, err)
	}
	if err := database.WriteAsyncAPIPaths(ctx, db, []string{"/specs"}); err != nil {
		t.Fatal(err)
	}
	proto, _, _ := database.ReadProtoImportPaths(ctx, db)
	async, _, _ := database.ReadAsyncAPIPaths(ctx, db)
	if !reflect.DeepEqual(proto, []string{"/proto"}) || !reflect.DeepEqual(async, []string{"/specs"}) {
		t.Fatalf("tables not independent: proto=%v async=%v", proto, async)
	}
}
