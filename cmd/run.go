/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/primary/http"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/core/services"
	httpsrv "github.com/loicalleyne/bufka/pkg/httpserver"
	"github.com/loicalleyne/bufka/pkg/logger"
	"github.com/spf13/cobra"
)

// runTimeoutSeconds is the --timeout flag value: number of seconds to run
// before automatically starting a graceful shutdown. 0 (default) disables
// the timeout and the process runs until a signal is received.
var runTimeoutSeconds int

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start the bufka ingest pipeline and HTTP server",
	Long: `Start the bufka Kafka -> Arrow -> DuckDB ingest pipeline and its
HTTP server (health check + DuckDB UI proxy), with graceful shutdown support.

The process listens for SIGINT (Ctrl+C) and SIGTERM signals and performs a
graceful shutdown with a configurable timeout: the Kafka consumer stops
accepting new records, in-flight Arrow batches are flushed and written to
DuckDB, then the HTTP server and DuckDB are closed.

Use --timeout to automatically start the same graceful shutdown after a
fixed number of seconds, useful for scripted runs and smoke tests.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := GetConfig()

		// Initialize logger from config
		log := logger.New(&logger.Config{
			Level:  cfg.LogLevel,
			Format: cfg.LogFormat,
		})

		log.Info("Starting bufka...")

		// ── Database ──────────────────────────────────────────────────────
		duckDB, err := database.InitDB(cfg)
		if err != nil {
			return err
		}
		defer func() { _ = duckDB.Close() }()

		// DuckDB UI: environment only (DUCKDB_UI_ENABLED, default off),
		// proxied at /duckdb-ui/. When enabled, failing to start is fatal.
		if ui := startDuckDBUI(context.Background(), cfg, duckDB, false); ui.Err != nil {
			return fmt.Errorf("start duckdb ui: %w", ui.Err)
		}

		// ── Kafka ingest pipeline ────────────────────────────────────────
		ingestCtx, ingestCancel := context.WithCancel(context.Background())
		defer ingestCancel()
		waitIngest, err := startKafkaIngest(ingestCtx, cfg, duckDB)
		if err != nil {
			return err
		}
		if waitIngest == nil {
			log.Info("No Kafka topics configured; ingest pipeline is disabled")
		}

		// ── Services (core) ───────────────────────────────────────────────
		svcs := services.New(&services.Config{})

		// Create and configure the HTTP server.
		// Framework-specific server mechanics live in pkg/httpserver.
		// Route handler wiring (ping, duckdb-ui proxy, ...) lives in
		// internal/adapters/primary/http/http.go.
		srv := http.New(&httpsrv.ServerConfig{
			Config: cfg,
			Logger: log,
		}, svcs)

		// Channel to capture OS signals
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

		// Channel for server errors
		errChan := make(chan error, 1)

		// Optional run timeout: automatically starts a graceful shutdown
		// after the configured number of seconds have elapsed.
		var timeoutChan <-chan time.Time
		if runTimeoutSeconds > 0 {
			timeout := time.Duration(runTimeoutSeconds) * time.Second
			log.Info("Run timeout enabled: shutting down after %s", timeout)
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			timeoutChan = timer.C
		}

		// Start server (non-blocking)
		srv.Run(errChan)

		gracefulShutdown := func(reason string) error {
			log.Info("%s, initiating graceful shutdown...", reason)

			// Stop accepting new Kafka records and wait for in-flight
			// batches to flush to DuckDB before shutting down the rest.
			ingestCancel()
			if waitIngest != nil {
				waitIngest()
			}

			// Reclaim disk space from the DuckDB file now that all writes
			// have flushed. Compact checkpoints and rewrites the file-backed
			// database into a compacted copy. Open connections remain valid.
			log.Info("Compacting DuckDB to reclaim disk space...")
			if err := duckDB.Compact(context.Background()); err != nil {
				log.Error("DuckDB compaction failed: %v", err)
			} else {
				log.Info("DuckDB compaction complete")
			}

			// Create shutdown context with timeout
			shutdownCtx, shutdownCancel := context.WithTimeout(
				context.Background(),
				cfg.Server.ShutdownTimeout,
			)
			defer shutdownCancel()

			// Attempt graceful shutdown
			if err := srv.Stop(shutdownCtx); err != nil {
				log.Error("Server shutdown error: %v", err)
				return err
			}

			log.Info("Server stopped gracefully")
			return nil
		}

		// Wait for signal, run timeout, or fatal server error
		select {
		case sig := <-sigChan:
			return gracefulShutdown(fmt.Sprintf("Received signal: %v", sig))

		case <-timeoutChan:
			return gracefulShutdown(fmt.Sprintf("Run timeout of %ds reached", runTimeoutSeconds))

		case err := <-errChan:
			log.Error("Server error: %v", err)
			ingestCancel()
			if waitIngest != nil {
				waitIngest()
			}
			return err
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().IntVar(&runTimeoutSeconds, "timeout", 0, "number of seconds to run before automatically starting a graceful shutdown (0 disables the timeout)")
}
