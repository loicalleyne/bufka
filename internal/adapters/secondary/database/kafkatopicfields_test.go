package database_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/pkg/schemafield"
)

func TestTopicFields(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "test.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	got, err := database.ReadTopicFields(ctx, db, "nope")
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("unknown topic before table: %v %v", got, err)
	}
	if err := database.EnsureTopicFieldsTable(ctx, db); err != nil {
		t.Fatal(err)
	}
	got, err = database.ReadTopicFields(ctx, db, "nope")
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("unknown topic: %v %v", got, err)
	}

	// Paths deliberately not alphabetical to verify position ordering.
	fields := []schemafield.FieldDef{
		{Path: "z", Type: "string", Required: true, Description: "last alpha", Enum: []string{"a", "b"}},
		{Path: "a.b", Type: "int64", Format: "int64", Repeated: true, Nullable: true, Default: "1", Example: "2", Number: 7, Oneof: "o"},
		{Path: "m", Type: "object"},
	}
	if err := database.ReplaceTopicFields(ctx, db, "t1", "asyncapi", "Msg", fields); err != nil {
		t.Fatal(err)
	}
	if err := database.ReplaceTopicFields(ctx, db, "t2", "proto", "P", fields[:1]); err != nil {
		t.Fatal(err)
	}
	got, err = database.ReadTopicFields(ctx, db, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].FieldPath != "z" || got[1].FieldPath != "a.b" || got[2].FieldPath != "m" {
		t.Fatalf("order: %+v", got)
	}
	if !reflect.DeepEqual(got[0].Enum, []string{"a", "b"}) || !got[0].Required || got[0].Source != "asyncapi" || got[0].Message != "Msg" {
		t.Fatalf("row0: %+v", got[0])
	}
	r := got[1]
	if !r.Repeated || !r.Nullable || r.Default != "1" || r.Example != "2" || r.FieldNumber != 7 || r.Oneof != "o" || r.Format != "int64" || r.Position != 1 || r.Enum != nil {
		t.Fatalf("row1: %+v", r)
	}

	// Replace semantics: old rows gone, other topics untouched.
	if err := database.ReplaceTopicFields(ctx, db, "t1", "proto", "Other", fields[2:]); err != nil {
		t.Fatal(err)
	}
	got, _ = database.ReadTopicFields(ctx, db, "t1")
	if len(got) != 1 || got[0].FieldPath != "m" || got[0].Source != "proto" || got[0].Message != "Other" || got[0].Position != 0 {
		t.Fatalf("after replace: %+v", got)
	}
	if got, _ = database.ReadTopicFields(ctx, db, "t2"); len(got) != 1 {
		t.Fatalf("t2 disturbed: %+v", got)
	}
	// Empty replace clears.
	if err := database.ReplaceTopicFields(ctx, db, "t1", "proto", "", nil); err != nil {
		t.Fatal(err)
	}
	if got, _ = database.ReadTopicFields(ctx, db, "t1"); len(got) != 0 {
		t.Fatalf("not cleared: %+v", got)
	}
}
