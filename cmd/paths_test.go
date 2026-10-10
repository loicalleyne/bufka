package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/internal/config"
)

func TestRunPaths(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	db := filepath.Join(t.TempDir(), "bufka.duckdb")
	var out bytes.Buffer
	if err := runPaths(&out, &config.Config{DuckDBPath: db, DuckDBPathDefaulted: true}); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	for _, want := range []string{"database:", db, "(default; set DUCKDB_PATH to change)", "not created yet", "sql history:", "sql_history", "duckdb driver: ok"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}
	out.Reset()
	_ = runPaths(&out, &config.Config{})
	if !strings.Contains(out.String(), "in-memory") {
		t.Fatalf("memory:\n%s", out.String())
	}
}
