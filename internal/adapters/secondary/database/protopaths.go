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

// ProtoImportPathsTableName is the DuckDB table holding the user-edited
// protobuf import paths, one row per path in priority order.
const ProtoImportPathsTableName = "proto_import_paths"

var protoImportPathsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "position", Type: arrow.PrimitiveTypes.Int32},
	{Name: "path", Type: arrow.BinaryTypes.String},
}, nil)

func buildProtoImportPathsRecord(paths []string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, protoImportPathsSchema)
	defer b.Release()
	posB := b.Field(0).(*array.Int32Builder)
	pathB := b.Field(1).(*array.StringBuilder)
	for i, p := range paths {
		posB.Append(int32(i))
		pathB.Append(p)
	}
	return b.NewRecordBatch()
}

// WriteProtoImportPaths replaces the saved import paths with paths. Saving an
// empty list leaves the table with no rows; callers treat that the same as
// nothing saved and fall back to the environment variable.
func WriteProtoImportPaths(ctx context.Context, db *couac.DB, paths []string) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	rec := buildProtoImportPathsRecord(paths)
	defer rec.Release()
	if _, err := conn.IngestReplace(ctx, ProtoImportPathsTableName, rec); err != nil {
		return fmt.Errorf("replace %s: %w", ProtoImportPathsTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// ReadProtoImportPaths returns the saved import paths in order. found is
// false when nothing has ever been saved (the table does not exist), in which
// case the caller should fall back to its default source.
func ReadProtoImportPaths(ctx context.Context, db *couac.DB) (paths []string, found bool, err error) {
	if db == nil {
		return nil, false, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, false, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, ProtoImportPathsTableName)
	if schema == nil {
		return nil, false, nil
	}

	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT path FROM %q ORDER BY position`, ProtoImportPathsTableName))
	if err != nil {
		return nil, false, fmt.Errorf("query %s: %w", ProtoImportPathsTableName, err)
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
		return nil, false, fmt.Errorf("read %s: %w", ProtoImportPathsTableName, err)
	}
	return paths, true, nil
}
