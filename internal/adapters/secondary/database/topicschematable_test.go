package database_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestEnsureTopicSchemaTable(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "test.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Calling ReadTopicSchemas on uninitialized table returns empty map without error.
	got, err := database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("unexpected error reading uninitialized table: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("expected empty non-nil map, got %v", got)
	}

	// EnsureTopicSchemaTable creates table if not existing.
	if err := database.EnsureTopicSchemaTable(ctx, db); err != nil {
		t.Fatalf("EnsureTopicSchemaTable failed: %v", err)
	}

	// Calling it a second time should be idempotent.
	if err := database.EnsureTopicSchemaTable(ctx, db); err != nil {
		t.Fatalf("second EnsureTopicSchemaTable failed: %v", err)
	}

	got, err = database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("read after EnsureTopicSchemaTable failed: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("expected empty map after ensure, got %v", got)
	}
}

func TestTopicSchemaRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "test.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	now := time.Now().UTC().Truncate(time.Microsecond)
	rows := []database.TopicSchemaRow{
		{
			Topic:               "orders-topic",
			Table:               "orders_sample",
			Format:              "protobuf",
			MessageName:         "acme.OrderPlaced",
			ProtoFile:           "orders/v1/order.proto",
			ImportRoot:          "/protos",
			CutConfluencePrefix: true,
			MessageOverride:     true,
			SampleCount:         50,
			MatchedCount:        48,
			UpdatedAt:           now,
		},
		{
			Topic:               "events-topic",
			Table:               "", // null in table
			Format:              "json",
			MessageName:         "", // null
			ProtoFile:           "", // null
			ImportRoot:          "", // null
			CutConfluencePrefix: false,
			MessageOverride:     false,
			SampleCount:         10,
			MatchedCount:        10,
			UpdatedAt:           now.Add(-time.Hour),
		},
	}

	if err := database.WriteTopicSchemas(ctx, db, rows); err != nil {
		t.Fatalf("WriteTopicSchemas failed: %v", err)
	}

	schemas, err := database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("ReadTopicSchemas failed: %v", err)
	}

	if len(schemas) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(schemas))
	}

	r1, ok := schemas["orders-topic"]
	if !ok {
		t.Fatalf("orders-topic missing from result")
	}
	if r1.Topic != rows[0].Topic ||
		r1.Table != rows[0].Table ||
		r1.Format != rows[0].Format ||
		r1.MessageName != rows[0].MessageName ||
		r1.ProtoFile != rows[0].ProtoFile ||
		r1.ImportRoot != rows[0].ImportRoot ||
		r1.CutConfluencePrefix != rows[0].CutConfluencePrefix ||
		r1.MessageOverride != rows[0].MessageOverride ||
		r1.SampleCount != rows[0].SampleCount ||
		r1.MatchedCount != rows[0].MatchedCount ||
		!r1.UpdatedAt.Equal(rows[0].UpdatedAt) {
		t.Fatalf("row 1 mismatch: got %+v, want %+v", r1, rows[0])
	}

	r2, ok := schemas["events-topic"]
	if !ok {
		t.Fatalf("events-topic missing from result")
	}
	if r2.Topic != rows[1].Topic ||
		r2.Table != rows[1].Table ||
		r2.Format != rows[1].Format ||
		r2.MessageName != rows[1].MessageName ||
		r2.ProtoFile != rows[1].ProtoFile ||
		r2.ImportRoot != rows[1].ImportRoot ||
		r2.CutConfluencePrefix != rows[1].CutConfluencePrefix ||
		r2.MessageOverride != rows[1].MessageOverride ||
		r2.SampleCount != rows[1].SampleCount ||
		r2.MatchedCount != rows[1].MatchedCount ||
		!r2.UpdatedAt.Equal(rows[1].UpdatedAt) {
		t.Fatalf("row 2 mismatch: got %+v, want %+v", r2, rows[1])
	}

	// Writing empty rows clears the table.
	if err := database.WriteTopicSchemas(ctx, db, nil); err != nil {
		t.Fatalf("WriteTopicSchemas nil failed: %v", err)
	}
	cleared, err := database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("ReadTopicSchemas after clear failed: %v", err)
	}
	if len(cleared) != 0 {
		t.Fatalf("expected 0 rows after clearing, got %d", len(cleared))
	}
}

func TestUpsertTopicSchemas(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "test.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	t1Time := time.Now().UTC().Truncate(time.Microsecond)
	// 1. Initial upsert with topic A and topic B
	initial := []database.TopicSchemaRow{
		{
			Topic:               "topic-a",
			Table:               "t_a",
			Format:              "protobuf",
			MessageName:         "pkg.MsgA",
			ProtoFile:           "a.proto",
			ImportRoot:          "/protos",
			CutConfluencePrefix: false,
			MessageOverride:     false,
			SampleCount:         20,
			MatchedCount:        20,
			UpdatedAt:           t1Time,
		},
		{
			Topic:        "topic-b",
			Table:        "t_b",
			Format:       "json",
			SampleCount:  5,
			MatchedCount: 5,
			UpdatedAt:    t1Time,
		},
	}

	if err := database.UpsertTopicSchemas(ctx, db, initial); err != nil {
		t.Fatalf("initial upsert failed: %v", err)
	}

	got, err := database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(got))
	}

	// 2. Set an override on topic-a
	override := database.TopicSchemaRow{
		Topic:               "topic-a",
		Table:               "t_a",
		Format:              "protobuf",
		MessageName:         "pkg.OverriddenMsg",
		ProtoFile:           "override.proto",
		ImportRoot:          "/protos",
		CutConfluencePrefix: true,
		MessageOverride:     true,
		SampleCount:         20,
		MatchedCount:        20,
		UpdatedAt:           t1Time.Add(time.Minute),
	}
	if err := database.UpsertTopicSchemas(ctx, db, []database.TopicSchemaRow{override}); err != nil {
		t.Fatalf("override upsert failed: %v", err)
	}

	// 3. New detection sample arrives for topic-a with MessageOverride: false and different detection
	sampleUpdate := database.TopicSchemaRow{
		Topic:               "topic-a",
		Table:               "t_a_new",
		Format:              "protobuf",
		MessageName:         "pkg.DetectedMsg",
		ProtoFile:           "detected.proto",
		ImportRoot:          "/other",
		CutConfluencePrefix: false,
		MessageOverride:     false, // Not explicitly changing override
		SampleCount:         100,
		MatchedCount:        95,
		UpdatedAt:           t1Time.Add(2 * time.Minute),
	}
	if err := database.UpsertTopicSchemas(ctx, db, []database.TopicSchemaRow{sampleUpdate}); err != nil {
		t.Fatalf("sample update upsert failed: %v", err)
	}

	got, err = database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	rowA := got["topic-a"]
	// Verify that MessageOverride flag and user-selected schema fields are kept!
	if !rowA.MessageOverride {
		t.Fatalf("expected MessageOverride to remain true")
	}
	if rowA.MessageName != "pkg.OverriddenMsg" {
		t.Fatalf("expected MessageName 'pkg.OverriddenMsg', got %q", rowA.MessageName)
	}
	if rowA.ProtoFile != "override.proto" {
		t.Fatalf("expected ProtoFile 'override.proto', got %q", rowA.ProtoFile)
	}
	if rowA.ImportRoot != "/protos" {
		t.Fatalf("expected ImportRoot '/protos', got %q", rowA.ImportRoot)
	}
	if !rowA.CutConfluencePrefix {
		t.Fatalf("expected CutConfluencePrefix true")
	}
	// Sample counts and updated table/timestamp should be updated from the new sample
	if rowA.SampleCount != 100 || rowA.MatchedCount != 95 {
		t.Fatalf("expected sample count 100 and matched 95, got %d, %d", rowA.SampleCount, rowA.MatchedCount)
	}
	if rowA.Table != "t_a_new" {
		t.Fatalf("expected table 't_a_new', got %q", rowA.Table)
	}

	// Topic B should be unchanged
	rowB := got["topic-b"]
	if rowB.Topic != "topic-b" || rowB.Format != "json" || rowB.SampleCount != 5 {
		t.Fatalf("topic-b was unexpectedly altered: %+v", rowB)
	}

	// 4. User explicitly changes override with MessageOverride: true
	explicitChange := database.TopicSchemaRow{
		Topic:               "topic-a",
		Table:               "t_a_new",
		Format:              "protobuf",
		MessageName:         "pkg.BrandNewManualPick",
		ProtoFile:           "manual.proto",
		ImportRoot:          "/manual",
		CutConfluencePrefix: false,
		MessageOverride:     true,
		SampleCount:         100,
		MatchedCount:        95,
		UpdatedAt:           t1Time.Add(3 * time.Minute),
	}
	if err := database.UpsertTopicSchemas(ctx, db, []database.TopicSchemaRow{explicitChange}); err != nil {
		t.Fatalf("explicit change upsert failed: %v", err)
	}

	got, err = database.ReadTopicSchemas(ctx, db)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	rowA = got["topic-a"]
	if rowA.MessageName != "pkg.BrandNewManualPick" || rowA.ProtoFile != "manual.proto" {
		t.Fatalf("explicit override change was not applied: %+v", rowA)
	}
}

func TestNilDBHandling(t *testing.T) {
	ctx := context.Background()
	if err := database.EnsureTopicSchemaTable(ctx, nil); err == nil {
		t.Fatal("expected error on EnsureTopicSchemaTable with nil db")
	}
	if err := database.WriteTopicSchemas(ctx, nil, nil); err == nil {
		t.Fatal("expected error on WriteTopicSchemas with nil db")
	}
	if _, err := database.ReadTopicSchemas(ctx, nil); err == nil {
		t.Fatal("expected error on ReadTopicSchemas with nil db")
	}
	if err := database.UpsertTopicSchemas(ctx, nil, nil); err == nil {
		t.Fatal("expected error on UpsertTopicSchemas with nil db")
	}
}
