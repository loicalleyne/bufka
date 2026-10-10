package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/spf13/cobra"
)

// pathsCmd prints where bufka keeps its files and checks the DuckDB driver.
var pathsCmd = &cobra.Command{
	Use:   "paths",
	Short: "Show where bufka keeps its database and history, and check the DuckDB driver",
	Long: `Print the files bufka uses and whether the DuckDB ADBC driver can be loaded.

bufka keeps every setting in its DuckDB database or in environment variables:
  database   DUCKDB_PATH, else $XDG_DATA_HOME/bufka/bufka.duckdb
             (~/.local/share/bufka/bufka.duckdb); DUCKDB_PATH=:memory: for a
             throwaway in-memory database
  history    $XDG_STATE_HOME/bufka/sql_history (~/.local/state/bufka)`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPaths(cmd.OutOrStdout(), GetConfig())
	},
}

func runPaths(w io.Writer, cfg *config.Config) error {
	db := cfg.DuckDBPath
	switch {
	case db == "":
		db = "in-memory (DUCKDB_PATH=:memory:)"
	case cfg.DuckDBPathDefaulted:
		db += " (default; set DUCKDB_PATH to change)"
	default:
		db += " (DUCKDB_PATH)"
	}
	if cfg.DuckDBPath != "" {
		if st, err := os.Stat(cfg.DuckDBPath); err == nil {
			db += fmt.Sprintf(", %s", humanBytes(st.Size()))
		} else {
			db += ", not created yet"
		}
	}
	history := "unavailable"
	if dir, err := config.StateDir(); err == nil {
		history = filepath.Join(dir, "sql_history")
	}

	driver := "ok"
	if err := driverOK(); errors.Is(err, database.ErrDriverMissing) {
		driver = "NOT INSTALLED: run \"bufka setup\""
	} else if err != nil {
		driver = "ERROR: " + err.Error()
	}

	rows := [][2]string{
		{"database", db},
		{"sql history", history},
		{"duckdb driver", driver},
	}
	for _, r := range rows {
		if _, err := fmt.Fprintf(w, "%-14s %s\n", r[0]+":", r[1]); err != nil {
			return err
		}
	}
	if notice := config.LegacyDBNotice(cfg); notice != "" {
		_, _ = fmt.Fprintf(w, "\nnote: %s\n", notice)
	}
	return nil
}

func init() {
	rootCmd.AddCommand(pathsCmd)
}
