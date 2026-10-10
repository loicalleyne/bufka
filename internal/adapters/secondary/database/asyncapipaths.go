package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/couac"
)

// AsyncAPIPathsTableName is the DuckDB table holding the user-edited
// AsyncAPI spec paths, one row per path in priority order.
const AsyncAPIPathsTableName = "asyncapi_paths"

var asyncAPIPathsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "position", Type: arrow.PrimitiveTypes.Int32},
	{Name: "path", Type: arrow.BinaryTypes.String},
}, nil)

func buildAsyncAPIPathsRecord(paths []string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, asyncAPIPathsSchema)
	defer b.Release()
	posB := b.Field(0).(*array.Int32Builder)
	pathB := b.Field(1).(*array.StringBuilder)
	for i, p := range paths {
		posB.Append(int32(i))
		pathB.Append(p)
	}
	return b.NewRecordBatch()
}

// WriteAsyncAPIPaths replaces the saved AsyncAPI paths with paths. Saving an
// empty list leaves the table with no rows; callers treat that the same as
// nothing saved and fall back to the environment variable.
func WriteAsyncAPIPaths(ctx context.Context, db *couac.DB, paths []string) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	rec := buildAsyncAPIPathsRecord(paths)
	defer rec.Release()
	if _, err := conn.IngestReplace(ctx, AsyncAPIPathsTableName, rec); err != nil {
		return fmt.Errorf("replace %s: %w", AsyncAPIPathsTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// ReadAsyncAPIPaths returns the saved AsyncAPI paths in order. found is
// false when nothing has ever been saved (the table does not exist), in which
// case the caller should fall back to its default source.
func ReadAsyncAPIPaths(ctx context.Context, db *couac.DB) (paths []string, found bool, err error) {
	if db == nil {
		return nil, false, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, false, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, AsyncAPIPathsTableName)
	if schema == nil {
		return nil, false, nil
	}

	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT path FROM %q ORDER BY position`, AsyncAPIPathsTableName))
	if err != nil {
		return nil, false, fmt.Errorf("query %s: %w", AsyncAPIPathsTableName, err)
	}
	defer res.Close()

	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		col := rec.Column(0).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			paths = append(paths, strings.Clone(col.Value(i)))
		}
	}
	if err := res.Reader.Err(); err != nil {
		return nil, false, fmt.Errorf("read %s: %w", AsyncAPIPathsTableName, err)
	}
	return paths, true, nil
}
