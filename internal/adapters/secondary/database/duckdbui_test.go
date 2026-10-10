package database_test

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestDuckDBUISettingsRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "u.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, found, err := database.ReadDuckDBUISettings(ctx, db); err != nil || found {
		t.Fatalf("expected nothing saved, found=%v err=%v", found, err)
	}
	want := database.DuckDBUISettings{Enabled: true, Port: 4299}
	if err := database.WriteDuckDBUISettings(ctx, db, want); err != nil {
		t.Fatal(err)
	}
	got, found, err := database.ReadDuckDBUISettings(ctx, db)
	if err != nil || !found || got != want {
		t.Fatalf("got %+v found=%v err=%v, want %+v", got, found, err, want)
	}
	// Saving "disabled" is remembered (it must not fall back to the env).
	if err := database.WriteDuckDBUISettings(ctx, db, database.DuckDBUISettings{Port: 4299}); err != nil {
		t.Fatal(err)
	}
	if got, found, _ = database.ReadDuckDBUISettings(ctx, db); !found || got.Enabled {
		t.Fatalf("disabled not persisted: %+v found=%v", got, found)
	}
}

func TestValidateDuckDBUIPort(t *testing.T) {
	for in, ok := range map[string]bool{"4213": true, " 8080 ": true, "80": false, "70000": false, "abc": false, "": false} {
		if _, err := database.ValidateDuckDBUIPort(in); (err == nil) != ok {
			t.Errorf("ValidateDuckDBUIPort(%q) err=%v, want ok=%v", in, err, ok)
		}
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func serving(port int) bool {
	c := http.Client{Timeout: 2 * time.Second}
	r, err := c.Get("http://localhost:" + strconv.Itoa(port) + "/")
	if err != nil {
		return false
	}
	_ = r.Body.Close()
	return true
}

func TestStartStopDuckDBUI(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	p1, p2 := freePort(t), freePort(t)
	if err := database.StartDuckDBUI(ctx, db, p1); err != nil {
		t.Fatal(err)
	}
	if !serving(p1) {
		t.Fatalf("ui not serving on %d", p1)
	}
	// Restarting on another port moves the server.
	if err := database.StartDuckDBUI(ctx, db, p2); err != nil {
		t.Fatal(err)
	}
	if !serving(p2) || serving(p1) {
		t.Fatalf("ui should have moved from %d to %d", p1, p2)
	}
	if err := database.StopDuckDBUI(ctx, db); err != nil {
		t.Fatal(err)
	}
	if serving(p2) {
		t.Fatal("ui still serving after stop")
	}
}

func TestStartDuckDBUIPortInUse(t *testing.T) {
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	port := l.Addr().(*net.TCPAddr).Port

	done := make(chan error, 1)
	go func() { done <- database.StartDuckDBUI(context.Background(), db, port) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected port-in-use error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StartDuckDBUI hung on a busy port")
	}
}

func TestDuckDBUIFromConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want database.DuckDBUISettings
	}{
		{"off by default", config.Config{DuckDBUIAddr: "http://localhost:4213"}, database.DuckDBUISettings{Port: 4213}},
		{"on, port from addr", config.Config{DuckDBUIEnabled: true, DuckDBUIAddr: "http://localhost:5000"}, database.DuckDBUISettings{Enabled: true, Port: 5000}},
		{"no port in addr", config.Config{DuckDBUIAddr: "http://localhost"}, database.DuckDBUISettings{Port: 4213}},
		{"bad port in addr", config.Config{DuckDBUIAddr: "http://localhost:80"}, database.DuckDBUISettings{Port: 4213}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := database.DuckDBUIFromConfig(&tt.cfg); got != tt.want {
				t.Fatalf("got %+v want %+v", got, tt.want)
			}
		})
	}
}
