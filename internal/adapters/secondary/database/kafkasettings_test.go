package database_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestKafkaSettingsRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "k.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	got, err := database.ReadKafkaSettings(ctx, db)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("expected empty non-nil map, got %v %v", got, err)
	}

	want := map[string]string{"seeds": "a:1;b:2", "tls_enabled": "T", "sasl_password_env": "MY_PASS"}
	// Empty values must not be stored.
	in := map[string]string{"seeds": "a:1;b:2", "tls_enabled": "T", "sasl_password_env": "MY_PASS", "instance_prefix": "  "}
	if err := database.WriteKafkaSettings(ctx, db, in); err != nil {
		t.Fatal(err)
	}
	if got, _ = database.ReadKafkaSettings(ctx, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}

	// Replace semantics; writing nothing clears everything.
	if err := database.WriteKafkaSettings(ctx, db, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ = database.ReadKafkaSettings(ctx, db); len(got) != 0 {
		t.Fatalf("expected cleared, got %v", got)
	}
}
