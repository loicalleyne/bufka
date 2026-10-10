package database

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/loicalleyne/couac"
)

// ArrowTableWriter writes Arrow record batches into DuckDB tables over one dedicated connection.
// It is not safe for concurrent use.
type ArrowTableWriter struct {
	conn    *couac.Conn
	schemas map[string]*arrow.Schema
}

// NewArrowTableWriter opens a dedicated connection on db for batch ingestion.
func NewArrowTableWriter(db *couac.DB) (*ArrowTableWriter, error) {
	if db == nil {
		return nil, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	return &ArrowTableWriter{conn: conn, schemas: make(map[string]*arrow.Schema)}, nil
}

// WriteBatch appends rec to table, creating or schema-merging the table when rec's schema
// differs from the last batch written to it.
func (w *ArrowTableWriter) WriteBatch(ctx context.Context, table string, rec arrow.RecordBatch) error {
	// Plain append avoids IngestMerge's CREATE OR REPLACE when couac's schema string comparison drifts.
	if last, ok := w.schemas[table]; ok && last.Equal(rec.Schema()) {
		if _, err := w.conn.Ingest(ctx, table, rec); err != nil {
			return fmt.Errorf("append to %s: %w", table, err)
		}
		return nil
	}
	if _, err := w.conn.IngestMerge(ctx, table, rec); err != nil {
		return fmt.Errorf("merge into %s: %w", table, err)
	}
	w.schemas[table] = rec.Schema()
	return nil
}

// Close closes the writer's connection.
func (w *ArrowTableWriter) Close() error {
	return w.conn.Close()
}
