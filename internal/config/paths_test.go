package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveDBPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", "")

	p, def, err := resolveDBPath("")
	if err != nil || !def || p != filepath.Join(home, ".local/share/bufka/bufka.duckdb") {
		t.Fatalf("default = %q %v %v", p, def, err)
	}

	xdg := t.TempDir()
	t.Setenv("XDG_DATA_HOME", xdg)
	if p, _, _ := resolveDBPath(""); p != filepath.Join(xdg, "bufka", "bufka.duckdb") {
		t.Fatalf("XDG_DATA_HOME ignored: %q", p)
	}
	t.Setenv("XDG_DATA_HOME", "relative/dir")
	if p, _, _ := resolveDBPath(""); !strings.HasPrefix(p, home) {
		t.Fatalf("a relative XDG_DATA_HOME must be ignored: %q", p)
	}

	if p, def, _ := resolveDBPath(":memory:"); p != "" || def {
		t.Fatalf(":memory: = %q %v", p, def)
	}
	if p, _, _ := resolveDBPath("~/x.duckdb"); p != filepath.Join(home, "x.duckdb") {
		t.Fatalf("~ not expanded: %q", p)
	}
	if p, def, _ := resolveDBPath("/data/b.duckdb"); p != "/data/b.duckdb" || def {
		t.Fatalf("explicit = %q %v", p, def)
	}
}

func TestStateDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	if d, _ := StateDir(); d != filepath.Join(home, ".local/state/bufka") {
		t.Fatalf("state dir = %q", d)
	}
}

func TestLegacyDBNotice(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(wd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{DuckDBPath: filepath.Join(dir, "data", "bufka.duckdb"), DuckDBPathDefaulted: true}
	if n := LegacyDBNotice(cfg); n != "" {
		t.Fatalf("no legacy file, got %q", n)
	}
	if err := os.WriteFile(LegacyDBName, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if n := LegacyDBNotice(cfg); !strings.Contains(n, "mv ") {
		t.Fatalf("notice = %q", n)
	}
	cfg.DuckDBPathDefaulted = false
	if n := LegacyDBNotice(cfg); n != "" {
		t.Fatal("an explicit DUCKDB_PATH needs no notice")
	}
	cfg.DuckDBPathDefaulted = true
	_ = EnsureDBDir(cfg.DuckDBPath)
	_ = os.WriteFile(cfg.DuckDBPath, nil, 0o600)
	if n := LegacyDBNotice(cfg); n != "" {
		t.Fatal("once the new database exists, no notice")
	}
}
