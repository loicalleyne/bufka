package cmd

import (
	"context"
	"fmt"
	"net"
	"slices"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func freeLocalPort(t *testing.T) int {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func TestStartDuckDBUI(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = database.StopDuckDBUI(ctx, db)
		_ = db.Close()
	}()
	envPort, savedPort := freeLocalPort(t), freeLocalPort(t)
	addr := fmt.Sprintf("http://localhost:%d", envPort)

	// Default: off, nothing started.
	if got := startDuckDBUI(ctx, &config.Config{DuckDBUIAddr: addr}, db, true); got.Running || got.Err != nil || got.Settings.Enabled {
		t.Fatalf("default should be off: %+v", got)
	}

	// Env override starts it on the DUCKDB_UI_ADDR port.
	got := startDuckDBUI(ctx, &config.Config{DuckDBUIEnabled: true, DuckDBUIAddr: addr}, db, false)
	if !got.Running || got.Err != nil || got.Settings.Port != envPort {
		t.Fatalf("env on: %+v", got)
	}
	_ = database.StopDuckDBUI(ctx, db)

	// A saved setting wins over the env only when useSaved (TUI, not run).
	if err := database.WriteDuckDBUISettings(ctx, db, database.DuckDBUISettings{Enabled: true, Port: savedPort}); err != nil {
		t.Fatal(err)
	}
	if got := startDuckDBUI(ctx, &config.Config{DuckDBUIAddr: addr}, db, false); got.Running {
		t.Fatalf("run must ignore saved settings: %+v", got)
	}
	got = startDuckDBUI(ctx, &config.Config{DuckDBUIAddr: addr}, db, true)
	if !got.Running || got.Settings.Port != savedPort {
		t.Fatalf("tui should use saved settings: %+v", got)
	}
	_ = database.StopDuckDBUI(ctx, db)

	// Busy port: reported, not running.
	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", envPort))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	if got := startDuckDBUI(ctx, &config.Config{DuckDBUIEnabled: true, DuckDBUIAddr: addr}, db, false); got.Running || got.Err == nil {
		t.Fatalf("busy port should fail: %+v", got)
	}
}

func TestStartDuckDBUIWithoutDB(t *testing.T) {
	got := startDuckDBUI(context.Background(), &config.Config{DuckDBUIEnabled: true}, nil, true)
	if got.Running || got.Err != nil {
		t.Fatalf("no db: %+v", got)
	}
}

func TestWithIncludePathsAppendsAfterTopicPaths(t *testing.T) {
	topics := []config.KafkaTopicConfig{{Topic: "a", ImportPaths: []string{"/own"}}, {Topic: "b"}}
	got := withIncludePaths(topics, []string{"/inc"})
	if !slices.Equal(got[0].ImportPaths, []string{"/own", "/inc"}) || !slices.Equal(got[1].ImportPaths, []string{"/inc"}) {
		t.Fatalf("%+v", got)
	}
	if len(topics[0].ImportPaths) != 1 {
		t.Fatal("input must not be mutated")
	}
}
