package cmd

import (
	"context"
	"log"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/couac"
)

// duckDBUIStartup is the DuckDB UI state after startup.
type duckDBUIStartup struct {
	Settings database.DuckDBUISettings
	Running  bool
	// Err is why the server failed to start, when Settings.Enabled.
	Err error
}

// startDuckDBUI is the startup step shared by run and tui. The settings
// come from the environment (DUCKDB_UI_ENABLED, default off, and the port in
// DUCKDB_UI_ADDR); when useSaved, a setting saved in the TUI's
// settings > duckdb ui takes precedence. The server is started only when
// enabled. A failure is returned in Err for the caller to decide on: run
// treats it as fatal, the TUI shows it and carries on.
func startDuckDBUI(ctx context.Context, cfg *config.Config, db *couac.DB, useSaved bool) duckDBUIStartup {
	out := duckDBUIStartup{Settings: database.DuckDBUIFromConfig(cfg)}
	if db == nil {
		return out
	}
	if useSaved {
		saved, found, err := database.ReadDuckDBUISettings(ctx, db)
		switch {
		case err != nil:
			log.Printf("could not read saved DuckDB UI settings, using environment: %v", err)
		case found:
			out.Settings = saved
		}
	}
	if !out.Settings.Enabled {
		return out
	}
	if out.Err = database.StartDuckDBUI(ctx, db, out.Settings.Port); out.Err != nil {
		log.Printf("duckdb ui not started: %v", out.Err)
		return out
	}
	out.Running = true
	log.Printf("duckdb ui running at %s", out.Settings.URL())
	return out
}
