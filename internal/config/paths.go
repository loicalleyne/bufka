package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// MemoryDB is the DUCKDB_PATH value that selects a throwaway in-memory
// database instead of the default file.
const MemoryDB = ":memory:"

// DefaultDBName is the database file name under DataDir.
const DefaultDBName = "bufka.duckdb"

// LegacyDBName is the per-directory database older versions created when
// DUCKDB_PATH pointed at it; only used to tell users how to move it.
const LegacyDBName = "bufka.db"

// DataDir is where bufka keeps its database: $XDG_DATA_HOME/bufka, else
// ~/.local/share/bufka (on macOS too, like most developer CLIs).
func DataDir() (string, error) {
	return xdgDir("XDG_DATA_HOME", ".local/share")
}

// StateDir is where bufka keeps state such as the SQL REPL history:
// $XDG_STATE_HOME/bufka, else ~/.local/state/bufka.
func StateDir() (string, error) {
	return xdgDir("XDG_STATE_HOME", ".local/state")
}

func xdgDir(env, fallback string) (string, error) {
	// The spec requires absolute paths; relative ones are ignored.
	if base := strings.TrimSpace(os.Getenv(env)); filepath.IsAbs(base) {
		return filepath.Join(base, "bufka"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", errors.New("cannot determine the home directory: set " + env + " or DUCKDB_PATH")
	}
	return filepath.Join(home, fallback, "bufka"), nil
}

// DefaultDBPath is the database used when DUCKDB_PATH is not set.
func DefaultDBPath() (string, error) {
	dir, err := DataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, DefaultDBName), nil
}

// resolveDBPath turns the DUCKDB_PATH setting into the path to open: the
// default file when unset, "" (in-memory) for ":memory:", else the value
// with a leading ~/ expanded. defaulted reports whether the default was
// used.
func resolveDBPath(env string) (path string, defaulted bool, err error) {
	env = strings.TrimSpace(env)
	switch {
	case env == MemoryDB:
		return "", false, nil
	case env == "":
		p, err := DefaultDBPath()
		return p, true, err
	case strings.HasPrefix(env, "~/"):
		home, err := os.UserHomeDir()
		if err != nil {
			return "", false, err
		}
		return filepath.Join(home, env[2:]), false, nil
	}
	return env, false, nil
}

// EnsureDBDir creates the directory holding path (mode 0700) when path is
// a file database.
func EnsureDBDir(path string) error {
	if path == "" {
		return nil
	}
	return os.MkdirAll(filepath.Dir(path), 0o700)
}

// LegacyDBNotice returns a message when the default database is in use, it
// does not exist yet, and a bufka.db from an older version is in the
// current directory; "" otherwise.
func LegacyDBNotice(cfg *Config) string {
	if !cfg.DuckDBPathDefaulted || cfg.DuckDBPath == "" {
		return ""
	}
	if _, err := os.Stat(cfg.DuckDBPath); err == nil {
		return ""
	}
	if st, err := os.Stat(LegacyDBName); err != nil || st.IsDir() {
		return ""
	}
	abs, _ := filepath.Abs(LegacyDBName)
	return "found " + abs + " from an older bufka; the database now lives at " + cfg.DuckDBPath +
		". To keep using it: mv " + abs + " " + cfg.DuckDBPath +
		" (and its .wal file if present), or set DUCKDB_PATH=" + abs
}
