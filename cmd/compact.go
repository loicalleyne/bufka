/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/spf13/cobra"
)

// compactCmd compacts the DuckDB database to reclaim disk space.
var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "Compact the DuckDB database to reclaim disk space",
	Long: `Reclaim disk space from the DuckDB file (see "bufka paths").

Compaction force-checkpoints the write-ahead log and rewrites the file-backed
database into a compacted copy, discarding the dead blocks left behind by
repeated table replacements (e.g. sampling and size estimates). The original
file is atomically replaced with the compacted copy.

Run this against a database that is not being written to concurrently.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()
		return runCompact(ctx)
	},
}

func runCompact(ctx context.Context) error {
	cfg := GetConfig()
	if cfg.DuckDBPath == "" {
		return fmt.Errorf("DUCKDB_PATH is :memory:; nothing to compact")
	}

	db, err := database.InitDB(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	log.Printf("compacting DuckDB at %s ...", cfg.DuckDBPath)
	if err := db.Compact(ctx); err != nil {
		return fmt.Errorf("compact duckdb: %w", err)
	}
	log.Printf("compaction complete")
	return nil
}

func init() {
	rootCmd.AddCommand(compactCmd)
}
