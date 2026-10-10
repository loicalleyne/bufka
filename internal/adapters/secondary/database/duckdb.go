// Package database is a SECONDARY adapter (outbound) that opens and manages
// the DuckDB database via couac (ADBC).
package database

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/couac"
)

const startUIServerQry = `CALL start_ui_server();`

// ErrDriverMissing is returned when the DuckDB ADBC driver is not installed.
// It wraps couac.ErrDriverNotFound.
var ErrDriverMissing = fmt.Errorf("the DuckDB driver is not installed: run \"bufka setup\" (%w)", couac.ErrDriverNotFound)

// InitDB opens a DuckDB database via couac: file-backed when cfg.DuckDBPath
// is set, in-memory otherwise. It never starts the UI server: callers that
// want it use DuckDBUIFromConfig and StartDuckDBUI.
func InitDB(cfg *config.Config) (*couac.DB, error) {
	opts := []couac.Option{couac.WithDriverLookup()}
	if cfg.DuckDBPath != "" {
		opts = append(opts, couac.WithPath(cfg.DuckDBPath))
	}

	if err := config.EnsureDBDir(cfg.DuckDBPath); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	db, err := couac.NewDuck(opts...)
	if errors.Is(err, couac.ErrDriverNotFound) {
		return nil, ErrDriverMissing
	}
	if err != nil {
		if cfg.DuckDBPath != "" {
			return nil, fmt.Errorf("open duckdb file %q: %w", cfg.DuckDBPath, err)
		}
		return nil, fmt.Errorf("open in-memory duckdb: %w", err)
	}

	if cfg.DuckDBPath != "" {
		log.Printf("duckdb opened at %s", cfg.DuckDBPath)
	} else {
		log.Printf("duckdb opened in-memory")
	}

	return db, nil
}

// DropTable drops table if it exists in the database.
func DropTable(ctx context.Context, db *couac.DB, table string) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %q", table)); err != nil {
		return fmt.Errorf("drop table %s: %w", table, err)
	}
	return nil
}

// CompactDB checkpoints and compacts the DuckDB database to reclaim freed disk space.
func CompactDB(ctx context.Context, db *couac.DB) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	return db.Compact(ctx)
}
